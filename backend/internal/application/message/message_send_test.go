package message

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"IM_backend/configs"
	outboxport "IM_backend/internal/application/ports/outbox"
	roomcache "IM_backend/internal/application/ports/persistence/cache/room"
	conversationrepo "IM_backend/internal/application/ports/persistence/repository/conversation"
	filerepo "IM_backend/internal/application/ports/persistence/repository/file"
	messagerepo "IM_backend/internal/application/ports/persistence/repository/message"
	roomrepo "IM_backend/internal/application/ports/persistence/repository/room"
	roomapp "IM_backend/internal/application/room"
	conversationentity "IM_backend/internal/domain/conversation/entity"
	fileentity "IM_backend/internal/domain/file/entity"
	messageentity "IM_backend/internal/domain/message/entity"
	messagevo "IM_backend/internal/domain/message/value_object"
	roomentity "IM_backend/internal/domain/room/entity"
	roomvo "IM_backend/internal/domain/room/value_object"
	"IM_backend/internal/shared/protocol"
)

type sendTestTransaction struct {
	beforeCommit func()
	commitErr    error
	committed    atomic.Bool
	calls        int
}

type sendTestRoomRepository struct {
	roomrepo.RoomRepository
	members int
}

func (r sendTestRoomRepository) FindActiveRoom(string, int) (*roomentity.Room, error) {
	return &roomentity.Room{MemberCount: r.members}, nil
}

type sendTestRoomCache struct {
	roomcache.RoomCache
	tx          *sendTestTransaction
	level       int
	activityErr error
	warmErr     error
	records     int
	warmed      []protocol.MessageEvent
}

func (c *sendTestRoomCache) RecordActivityAndGetLevel(context.Context, string, int) (int, error) {
	if !c.tx.committed.Load() {
		panic("activity recorded before transaction commit")
	}
	c.records++
	return c.level, c.activityErr
}

func (c *sendTestRoomCache) AppendRecentMessageSeq(_ context.Context, _ string, event protocol.MessageEvent) error {
	if !c.tx.committed.Load() {
		panic("cache warmed before transaction commit")
	}
	c.warmed = append(c.warmed, event)
	return c.warmErr
}

func TestSendMessageWarmsCommittedRoomMessages(t *testing.T) {
	for _, test := range []struct {
		name                 string
		members, level       int
		activityErr, warmErr error
		wantWarm             bool
	}{
		{name: "normal", members: 10},
		{name: "warn", members: 10, level: roomcache.RoomActivityWarn, wantWarm: true},
		{name: "active", members: 10, level: roomcache.RoomActivityActive, wantWarm: true},
		{name: "large", members: 501, wantWarm: true},
		{name: "cache_failure", members: 501, warmErr: errors.New("Redis unavailable"), wantWarm: true},
		{name: "activity_failure_large", members: 501, activityErr: errors.New("Redis unavailable"), wantWarm: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			app, tx, _, _, _ := newSendTestApplication()
			cache := &sendTestRoomCache{tx: tx, level: test.level, activityErr: test.activityErr, warmErr: test.warmErr}
			app.roomCache = cache
			app.roomRepository = sendTestRoomRepository{members: test.members}
			app.config.Message.RoomRealtimeFanoutLimit = 500
			ack, err := app.HandleSendMessage(context.Background(), sendTestDTO())
			if err != nil || ack.Status != string(protocol.AckStatusSent) {
				t.Fatalf("cache must not change ACK: %+v %v", ack, err)
			}
			if cache.records != 1 || (len(cache.warmed) == 1) != test.wantWarm {
				t.Fatalf("records=%d warmed=%d", cache.records, len(cache.warmed))
			}
			if test.wantWarm && (cache.warmed[0].Seq != ack.Seq || cache.warmed[0].MessageId != ack.MessageID) {
				t.Fatal("cache must describe committed message")
			}
		})
	}
}

func TestSendMessageRollbackAndDuplicateDoNotRecordActivity(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		app, tx, messages, _, _ := newSendTestApplication()
		cache := &sendTestRoomCache{tx: tx}
		app.roomCache = cache
		app.roomRepository = sendTestRoomRepository{members: 501}
		dto := sendTestDTO()
		if duplicate {
			dto.ConversationID = dto.ReceiverID
			hash, err := buildMessageRequestHash(dto)
			if err != nil {
				t.Fatal(err)
			}
			messages.err = messageentity.ErrDuplicateClientMessage
			messages.existing = &messageentity.Message{MessageId: "original", ConversationId: "room", Seq: 3, RequestHash: hash}
		} else {
			tx.commitErr = errors.New("commit failed")
		}
		_, err := app.HandleSendMessage(context.Background(), dto)
		if duplicate && err != nil {
			t.Fatal(err)
		}
		if !duplicate && err == nil {
			t.Fatal("expected commit failure")
		}
		if cache.records != 0 || len(cache.warmed) != 0 {
			t.Fatal("rollback and duplicate must not update activity or warm cache")
		}
	}
}

func TestRoomActiveHoldContinuesWarmupAndDoesNotRenewOnNormalOrError(t *testing.T) {
	for _, activityErr := range []error{nil, errors.New("Redis unavailable")} {
		app, tx, _, _, _ := newSendTestApplication()
		tx.committed.Store(true)
		app.roomRepository = sendTestRoomRepository{members: 10}
		app.config.Message.RoomRealtimeFanoutLimit = 500
		cache := &sendTestRoomCache{tx: tx, level: roomcache.RoomActivityActive}
		app.roomCache = cache
		event := protocol.MessageEvent{ConvType: protocol.RoomChat, ConversationId: "room", Seq: 1}
		app.warmRoomMessageCache(context.Background(), event)
		if !app.roomActivityState.IsActive("room", time.Now()) {
			t.Fatal("ACTIVE must immediately mark shared state")
		}
		// 用公开接口设置剩余一分钟的标识，验证 NORMAL 和统计失败不会将其续期。
		now := time.Now()
		app.roomActivityState = roomapp.NewRoomActivityState(2 * time.Minute)
		app.roomActivityState.MarkActive("room", now.Add(-time.Minute))
		cache.level, cache.activityErr = roomcache.RoomActivityNormal, activityErr
		event.Seq = 2
		app.warmRoomMessageCache(context.Background(), event)
		if cache.records != 2 || len(cache.warmed) != 2 {
			t.Fatal("hold must preserve per-message activity checks and warmup")
		}
		if app.roomActivityState.IsActive("room", now.Add(90*time.Second)) {
			t.Fatal("NORMAL or activity error must not renew ACTIVE")
		}
		event.Seq = 3
		app.warmRoomMessageCache(context.Background(), event)
		if cache.records != 3 || len(cache.warmed) != 2 {
			t.Fatal("expired small room must stop warmup when not WARN/ACTIVE")
		}
	}
}

func (tx *sendTestTransaction) WithinTransaction(ctx context.Context, fn func(any) error) error {
	tx.calls++
	if err := fn(tx); err != nil {
		return err
	}
	if tx.beforeCommit != nil {
		tx.beforeCommit()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if tx.commitErr != nil {
		return tx.commitErr
	}
	tx.committed.Store(true)
	return nil
}

type sendTestIDs struct{ next atomic.Int32 }

func (g *sendTestIDs) Generate() (string, error) {
	return fmt.Sprintf("message-%d", g.next.Add(1)), nil
}

type sendTestMembers struct{ roomcache.RoomMemberCache }

func (sendTestMembers) GetMember(context.Context, string, string) (*roomcache.MemberState, bool, error) {
	return &roomcache.MemberState{Status: roomvo.Activate}, true, nil
}

type sendTestMessages struct {
	messagerepo.MessageRepository
	tx       *sendTestTransaction
	message  *messageentity.Message
	existing *messageentity.Message
	err      error
}

func (r *sendTestMessages) WithTx(tx any) messagerepo.MessageRepository {
	if tx != r.tx {
		panic("message write outside send transaction")
	}
	return r
}

func (r *sendTestMessages) CreateNewMessage(_ context.Context, message *messageentity.Message) error {
	r.message = message
	return r.err
}

func (r *sendTestMessages) FindByClientMsgID(context.Context, string, string) (*messageentity.Message, error) {
	return r.existing, nil
}

type sendTestConversations struct {
	conversationrepo.ConversationRepository
	tx    *sendTestTransaction
	calls int
}

func (r *sendTestConversations) WithTx(tx any) conversationrepo.ConversationRepository {
	if tx != r.tx {
		panic("sequence update outside send transaction")
	}
	return r
}

func (r *sendTestConversations) UpdateLatestSequence(context.Context, string, string) (int64, error) {
	r.calls++
	return 7, nil
}

type sendTestUserConversations struct {
	conversationrepo.UserConversationRepository
	tx *sendTestTransaction
}

func (r *sendTestUserConversations) WithTx(tx any) conversationrepo.UserConversationRepository {
	if tx != r.tx {
		panic("read update outside send transaction")
	}
	return r
}

func (*sendTestUserConversations) UpdateReadSeq(context.Context, *conversationentity.UserConversation) error {
	return nil
}

type sendTestOutbox struct {
	outboxport.OutboxRepository
	tx      *sendTestTransaction
	entries []*outboxport.Entry
	err     error
}

func (r *sendTestOutbox) WithTx(tx any) outboxport.OutboxRepository {
	if tx != r.tx {
		panic("outbox write outside send transaction")
	}
	return r
}

func (r *sendTestOutbox) Create(_ context.Context, entry *outboxport.Entry) error {
	r.entries = append(r.entries, entry)
	return r.err
}

type sendTestNotifier struct {
	tx           *sendTestTransaction
	calls        atomic.Int32
	beforeCommit atomic.Bool
}

func (n *sendTestNotifier) Notify() {
	if !n.tx.committed.Load() {
		n.beforeCommit.Store(true)
	}
	n.calls.Add(1)
}

func newSendTestApplication() (*MessageApplication, *sendTestTransaction, *sendTestMessages, *sendTestOutbox, *sendTestNotifier) {
	tx := &sendTestTransaction{}
	messages := &sendTestMessages{tx: tx}
	outbox := &sendTestOutbox{tx: tx}
	notifier := &sendTestNotifier{tx: tx}
	return &MessageApplication{
		roomActivityState: roomapp.NewRoomActivityState(2 * time.Minute),
		txManager:         tx, idGenerator: &sendTestIDs{}, roomMemberCache: sendTestMembers{},
		messageRepository: messages, conversationRepository: &sendTestConversations{tx: tx},
		userConversationRepository: &sendTestUserConversations{tx: tx},
		messageOutboxRepository:    outbox, producerNotifier: notifier,
		config: configs.Config{Message: configs.MessageConfig{MaxTextRunes: 4096, MaxIdentifierLength: 128, AttachmentTTLSeconds: 60}},
	}, tx, messages, outbox, notifier
}

func sendTestDTO() SendMessageDTO {
	return SendMessageDTO{SenderID: "sender", ReceiverID: "room", ConversationType: int(protocol.RoomChat),
		ClientMessageID: "client-1", Type: int(messagevo.Text), Content: "hello"}
}

func TestSendMessageWaitsForCommitBeforeAckAndNotify(t *testing.T) {
	app, tx, messages, outbox, notifier := newSendTestApplication()
	ready, release := make(chan struct{}), make(chan struct{})
	tx.beforeCommit = func() { close(ready); <-release }
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	result := make(chan *MessageAckDTO, 1)
	errCh := make(chan error, 1)
	go func() {
		ack, err := app.HandleSendMessage(context.Background(), sendTestDTO())
		errCh <- err
		result <- ack
	}()
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not reach commit")
	}
	select {
	case <-result:
		t.Fatal("ACK returned before commit")
	default:
	}
	if notifier.calls.Load() != 0 {
		t.Fatal("producer notified before commit")
	}
	close(release)
	var ack *MessageAckDTO
	select {
	case ack = <-result:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not complete")
	}
	if err := <-errCh; err != nil || ack.Status != string(protocol.AckStatusSent) || ack.Seq != 7 {
		t.Fatalf("unexpected ACK: %+v err=%v", ack, err)
	}
	if notifier.calls.Load() != 1 || notifier.beforeCommit.Load() || len(outbox.entries) != 1 {
		t.Fatal("successful commit must notify once and contain a message outbox")
	}
	var envelope protocol.Envelope
	var event protocol.MessageEvent
	if err := json.Unmarshal(outbox.entries[0].Payload, &envelope); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(envelope.Payload, &event); err != nil {
		t.Fatal(err)
	}
	if event.Seq != ack.Seq || event.MessageId != messages.message.MessageId || event.ClientMsgId != ack.ClientMessageID {
		t.Fatalf("outbox and ACK must describe the committed message: %+v %+v", event, ack)
	}
}

func TestSendMessageFailureDoesNotAckSuccessOrNotify(t *testing.T) {
	for _, stage := range []string{"message", "outbox", "commit", "cancel"} {
		t.Run(stage, func(t *testing.T) {
			app, tx, messages, outbox, notifier := newSendTestApplication()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			failure := errors.New("write failed")
			switch stage {
			case "message":
				messages.err = failure
			case "outbox":
				outbox.err = failure
			case "commit":
				tx.commitErr = failure
			case "cancel":
				tx.beforeCommit = cancel
			}
			ack, err := app.HandleSendMessage(ctx, sendTestDTO())
			if err == nil || ack.Status != string(protocol.AckStatusFailed) || tx.committed.Load() || notifier.calls.Load() != 0 {
				t.Fatalf("failure must not return success or notify: ack=%+v err=%v", ack, err)
			}
		})
	}
}

func TestSendMessageDuplicateReturnsOriginalAck(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprintf("conflict=%t", conflict), func(t *testing.T) {
			app, tx, messages, outbox, notifier := newSendTestApplication()
			dto := sendTestDTO()
			dto.ConversationID = dto.ReceiverID
			hash, err := buildMessageRequestHash(dto)
			if err != nil {
				t.Fatal(err)
			}
			if conflict {
				hash = "different-request"
			}
			messages.err = messageentity.ErrDuplicateClientMessage
			messages.existing = &messageentity.Message{MessageId: "original", ConversationId: "room", Seq: 3, RequestHash: hash}
			ack, err := app.HandleSendMessage(context.Background(), dto)
			if conflict {
				if !errors.Is(err, messageentity.ErrClientMessageConflict) || ack.Status != string(protocol.AckStatusFailed) {
					t.Fatalf("conflicting retry must fail: %+v %v", ack, err)
				}
			} else if err != nil || ack.MessageID != "original" || ack.Seq != 3 || ack.Status != string(protocol.AckStatusSent) {
				t.Fatalf("retry must return original committed message: %+v %v", ack, err)
			}
			if tx.committed.Load() || len(outbox.entries) != 0 || notifier.calls.Load() != 0 {
				t.Fatal("duplicate must not commit a second message or notify")
			}
		})
	}
}

type sendTestFiles struct {
	filerepo.FileRepository
	tx     *sendTestTransaction
	locked *fileentity.File
}

func (r *sendTestFiles) WithTx(tx any) filerepo.FileRepository {
	if tx != r.tx {
		panic("file check outside send transaction")
	}
	return r
}

func (*sendTestFiles) FindUploadedFileByIDAndUploader(context.Context, string, string) (*fileentity.File, error) {
	return &fileentity.File{FileId: "file", FileName: "cached.txt", ContentType: "text/plain", Size: 1}, nil
}

func (r *sendTestFiles) FindUploadedFileByIDAndUploaderForUpdate(context.Context, string, string) (*fileentity.File, error) {
	return r.locked, nil
}

type sendTestAttachments struct {
	messagerepo.MessageAttachmentRepository
	tx      *sendTestTransaction
	created *messageentity.MessageAttachment
}

func (r *sendTestAttachments) WithTx(tx any) messagerepo.MessageAttachmentRepository {
	if tx != r.tx {
		panic("attachment write outside send transaction")
	}
	return r
}

func (r *sendTestAttachments) Create(_ context.Context, attachment *messageentity.MessageAttachment) error {
	r.created = attachment
	return nil
}

func TestSendMediaUsesLockedFileAndRejectsDeletedFile(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		t.Run(fmt.Sprintf("deleted=%t", deleted), func(t *testing.T) {
			app, tx, messages, outbox, notifier := newSendTestApplication()
			files := &sendTestFiles{tx: tx}
			if !deleted {
				files.locked = &fileentity.File{FileId: "file", FileName: "actual.txt", ContentType: "text/plain", Size: 99}
			}
			attachments := &sendTestAttachments{tx: tx}
			app.fileRepository, app.messageAttachmentsRepository = files, attachments
			dto := sendTestDTO()
			dto.Type, dto.Content, dto.FileID = int(messagevo.File), "", "file"
			ack, err := app.HandleSendMessage(context.Background(), dto)
			if deleted {
				if err == nil || ack.Status != string(protocol.AckStatusFailed) || messages.message != nil || len(outbox.entries) != 0 || notifier.calls.Load() != 0 {
					t.Fatalf("deleted file must not be sent: %+v %v", ack, err)
				}
				if app.conversationRepository.(*sendTestConversations).calls != 0 {
					t.Fatal("deleted file must not reserve seq")
				}
				return
			}
			if err != nil || ack.AttachmentID == "" || attachments.created == nil || len(outbox.entries) != 2 || messages.message.Content != "actual.txt" {
				t.Fatalf("media send must atomically save attachment and both events: %+v %v", ack, err)
			}
			var envelope protocol.Envelope
			var event protocol.FileCardWarmupEvent
			if err := json.Unmarshal(outbox.entries[1].Payload, &envelope); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(envelope.Payload, &event); err != nil {
				t.Fatal(err)
			}
			if event.FileName != "actual.txt" || event.Size != 99 || event.AttachmentID != ack.AttachmentID {
				t.Fatalf("warmup must use locked metadata: %+v", event)
			}
		})
	}
}
