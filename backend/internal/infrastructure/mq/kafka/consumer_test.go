package kafka

import (
	eventbus "IM_backend/internal/application/ports/eventbus"
	inboxport "IM_backend/internal/application/ports/inbox"
	"IM_backend/internal/shared/protocol"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

type consumerTestHandler func(context.Context, eventbus.IncomingEvent) error

func (f consumerTestHandler) Handle(ctx context.Context, event eventbus.IncomingEvent) error {
	return f(ctx, event)
}

type consumerTestPublisher func(context.Context, eventbus.IntegrationEvent) error

func (f consumerTestPublisher) Publish(ctx context.Context, event eventbus.IntegrationEvent) error {
	return f(ctx, event)
}

type consumerTestTx struct{}

func (consumerTestTx) WithinTransaction(ctx context.Context, fn func(any) error) error {
	return fn(nil)
}

type consumerTestInbox struct {
	inboxport.InboxRepository
	mu        sync.Mutex
	attempts  map[string]int
	terminal  map[string]bool
	completed chan string
	complete  func(string) error
	renew     func() error
}

func (r *consumerTestInbox) WithTx(any) inboxport.InboxRepository { return r }
func (r *consumerTestInbox) TryClaim(_ context.Context, id, _ string, _, _ time.Time) (bool, string, int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.terminal[id] {
		return false, "", r.attempts[id], nil
	}
	r.attempts[id]++
	return true, id, r.attempts[id], nil
}
func (r *consumerTestInbox) MarkCompleted(_ context.Context, id, _ string, _ time.Time) error {
	if r.complete != nil {
		if err := r.complete(id); err != nil {
			return err
		}
	}
	r.mu.Lock()
	r.terminal[id] = true
	r.mu.Unlock()
	r.completed <- id
	return nil
}
func (r *consumerTestInbox) MarkRetry(context.Context, string, string, string) error {
	return nil
}
func (r *consumerTestInbox) MarkDead(_ context.Context, id, _, _ string, _ time.Time) error {
	r.mu.Lock()
	r.terminal[id] = true
	r.mu.Unlock()
	return nil
}
func (r *consumerTestInbox) RenewBatch(context.Context, []string, string, time.Time) error {
	if r.renew != nil {
		return r.renew()
	}
	return nil
}

type consumerTestSession struct {
	sarama.ConsumerGroupSession
	ctx    context.Context
	marked chan int64
}

func (s consumerTestSession) Context() context.Context { return s.ctx }
func (s consumerTestSession) MarkMessage(message *sarama.ConsumerMessage, _ string) {
	s.marked <- message.Offset
}

type consumerTestClaim struct {
	sarama.ConsumerGroupClaim
	messages chan *sarama.ConsumerMessage
}

func (c consumerTestClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }
func (consumerTestClaim) HighWaterMarkOffset() int64                 { return 1000 }

func consumerTestMessage(id string, offset int64) *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{Topic: "messages", Key: []byte("same-room"), Offset: offset,
		Headers: []*sarama.RecordHeader{{Key: []byte("event_id"), Value: []byte(id)}}}
}

func newConsumerTest(t *testing.T, handler consumerTestHandler) (*Consumer, *consumerTestInbox, saramaAdapter) {
	t.Helper()
	name := protocol.EventTypeSendMessage
	router, err := NewTopicRouter(map[string]string{name: "messages"})
	if err != nil {
		t.Fatal(err)
	}
	inbox := &consumerTestInbox{attempts: map[string]int{}, terminal: map[string]bool{}, completed: make(chan string, 20)}
	c := &Consumer{handlers: map[string]eventbus.Handler{name: handler},
		inbox: inbox, txManager: consumerTestTx{}, leaseStaleAfter: time.Minute, maxRetries: 3, deadLetterSuffix: ".dlq"}
	return c, inbox, saramaAdapter{consumer: c, topicRouter: router}
}

func consumerTestReceive[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for consumer")
		var zero T
		return zero
	}
}

func TestConsumerProcessesPartitionMessagesSequentially(t *testing.T) {
	firstID, secondID := "a", "b"
	release := make(chan struct{})
	started := make(chan string, 2)
	_, inbox, adapter := newConsumerTest(t, func(ctx context.Context, event eventbus.IncomingEvent) error {
		started <- event.EventID
		if event.EventID == firstID {
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := consumerTestSession{ctx: ctx, marked: make(chan int64, 4)}
	claim := consumerTestClaim{messages: make(chan *sarama.ConsumerMessage, 2)}
	claim.messages <- consumerTestMessage(firstID, 42)
	claim.messages <- consumerTestMessage(secondID, 47) // offset can have gaps.
	close(claim.messages)
	done := make(chan error, 1)
	go func() { done <- adapter.ConsumeClaim(session, claim) }()
	if id := consumerTestReceive(t, started); id != firstID {
		t.Fatalf("first started event = %s, want %s", id, firstID)
	}
	select {
	case id := <-started:
		t.Fatalf("later event %s started before the earlier event completed", id)
	case <-time.After(30 * time.Millisecond):
	}
	if len(session.marked) != 0 {
		t.Fatal("offset must not be marked before the current event completes")
	}
	close(release)
	if id := consumerTestReceive(t, inbox.completed); id != firstID {
		t.Fatalf("first completed event = %s, want %s", id, firstID)
	}
	if offset := consumerTestReceive(t, session.marked); offset != 42 {
		t.Fatalf("first marked offset = %d, want 42", offset)
	}
	if id := consumerTestReceive(t, started); id != secondID {
		t.Fatalf("second started event = %s, want %s", id, secondID)
	}
	if id := consumerTestReceive(t, inbox.completed); id != secondID {
		t.Fatalf("second completed event = %s, want %s", id, secondID)
	}
	if offset := consumerTestReceive(t, session.marked); offset != 47 {
		t.Fatalf("second marked offset = %d, want 47", offset)
	}
	if err := consumerTestReceive(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestConsumerSessionCancellationStopsCurrentAndLaterMessages(t *testing.T) {
	started := make(chan string, 4)
	_, _, adapter := newConsumerTest(t, func(ctx context.Context, event eventbus.IncomingEvent) error {
		started <- event.EventID
		<-ctx.Done()
		return ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := consumerTestSession{ctx: ctx, marked: make(chan int64, 4)}
	claim := consumerTestClaim{messages: make(chan *sarama.ConsumerMessage, 2)}
	claim.messages <- consumerTestMessage("first", 1)
	claim.messages <- consumerTestMessage("queued", 2)
	done := make(chan error, 1)
	go func() { done <- adapter.ConsumeClaim(session, claim) }()
	consumerTestReceive(t, started)
	cancel()
	if err := consumerTestReceive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("claim returned %v", err)
	}
	select {
	case id := <-started:
		t.Fatalf("revoked task executed: %s", id)
	case <-time.After(30 * time.Millisecond):
	}
	if len(session.marked) != 0 {
		t.Fatal("canceled tasks must not mark offsets")
	}
}

func TestConsumerRetriesFailedCompletionBeforeMarkingOffset(t *testing.T) {
	attempts := make(chan struct{}, 4)
	_, inbox, adapter := newConsumerTest(t, func(context.Context, eventbus.IncomingEvent) error {
		attempts <- struct{}{}
		return nil
	})
	completeCalls := 0
	inbox.complete = func(string) error {
		completeCalls++
		if completeCalls == 1 {
			return errors.New("database response lost")
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := consumerTestSession{ctx: ctx, marked: make(chan int64, 4)}
	claim := consumerTestClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
	claim.messages <- consumerTestMessage("retry", 9)
	close(claim.messages)
	done := make(chan error, 1)
	go func() { done <- adapter.ConsumeClaim(session, claim) }()
	consumerTestReceive(t, attempts)
	select {
	case offset := <-session.marked:
		t.Fatalf("marked %d before Inbox completion persisted", offset)
	case <-time.After(30 * time.Millisecond):
	}
	if err := consumerTestReceive(t, done); err != nil {
		t.Fatal(err)
	}
	if offset := consumerTestReceive(t, session.marked); offset != 9 {
		t.Fatalf("marked %d, want 9", offset)
	}
	if completeCalls != 2 {
		t.Fatalf("completion calls = %d, want 2", completeCalls)
	}
}

func TestConsumerLeaseLossCancelsHandler(t *testing.T) {
	c, inbox, _ := newConsumerTest(t, func(ctx context.Context, _ eventbus.IncomingEvent) error {
		<-ctx.Done()
		return ctx.Err()
	})
	c.leaseStaleAfter = 30 * time.Millisecond
	inbox.renew = func() error { return inboxport.ErrLeaseLost }
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := c.handle(ctx, eventbus.IncomingEvent{Name: protocol.EventTypeSendMessage, EventID: "lease"})
	if !errors.Is(err, inboxport.ErrLeaseLost) {
		t.Fatalf("handler result = %v, want lease lost", err)
	}
	if len(inbox.completed) != 0 {
		t.Fatal("must not complete a lost lease")
	}
}

func TestConsumerDeadLetterMustPublishBeforeTerminalState(t *testing.T) {
	c, inbox, _ := newConsumerTest(t, func(context.Context, eventbus.IncomingEvent) error {
		return eventbus.NonRetryable(errors.New("invalid payload"))
	})
	publishCalls := 0
	c.deadLetterPublisher = consumerTestPublisher(func(context.Context, eventbus.IntegrationEvent) error {
		publishCalls++
		if publishCalls == 1 {
			return errors.New("Kafka unavailable")
		}
		return nil
	})
	event := eventbus.IncomingEvent{Name: protocol.EventTypeSendMessage, EventID: "dead"}
	if err := c.handle(context.Background(), event); err == nil {
		t.Fatal("failed DLQ publication must not complete the task")
	}
	if inbox.terminal[event.EventID] {
		t.Fatal("failed DLQ publication must not mark Inbox terminal")
	}
	if err := c.handle(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if !inbox.terminal[event.EventID] {
		t.Fatal("successful DLQ publication should mark Inbox dead")
	}
}
