package message

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	messagecache "IM_backend/internal/application/ports/persistence/cache/message"
	messageentity "IM_backend/internal/domain/message/entity"
	"IM_backend/internal/shared/protocol"
)

type batchDedupTestCache struct {
	entries    map[messagecache.DedupKey]messagecache.DedupEntry
	tx         *sendTestTransaction
	err        error
	batchReads int
	keys       []messagecache.DedupKey
	writes     []messagecache.DedupKey
}

func (*batchDedupTestCache) GetDedupEntry(context.Context, string, string) (messagecache.DedupEntry, error) {
	panic("batch send must not perform per-message Redis reads")
}

func (c *batchDedupTestCache) GetDedupEntries(_ context.Context, keys []messagecache.DedupKey) (map[messagecache.DedupKey]messagecache.DedupEntry, error) {
	c.batchReads++
	c.keys = append([]messagecache.DedupKey(nil), keys...)
	if c.err != nil {
		return nil, c.err
	}
	entries := make(map[messagecache.DedupKey]messagecache.DedupEntry)
	for _, key := range keys {
		if entry, exists := c.entries[key]; exists {
			entries[key] = entry
		}
	}
	return entries, nil
}

func (c *batchDedupTestCache) SetDedupEntry(_ context.Context, senderID, clientMsgID string, entry messagecache.DedupEntry, _ time.Duration) (bool, error) {
	if !c.tx.committed.Load() {
		panic("dedup result cached before commit")
	}
	key := messagecache.DedupKey{SenderID: senderID, ClientMsgID: clientMsgID}
	c.writes = append(c.writes, key)
	if _, exists := c.entries[key]; exists {
		return false, nil
	}
	if c.entries == nil {
		c.entries = make(map[messagecache.DedupKey]messagecache.DedupEntry)
	}
	c.entries[key] = entry
	return true, nil
}

func (c *batchDedupTestCache) SetDedupEntries(_ context.Context, entries []messagecache.DedupEntryWrite, _ time.Duration) error {
	if !c.tx.committed.Load() {
		panic("dedup result cached before commit")
	}
	if c.entries == nil {
		c.entries = make(map[messagecache.DedupKey]messagecache.DedupEntry)
	}
	for _, item := range entries {
		c.writes = append(c.writes, item.Key)
		if _, exists := c.entries[item.Key]; !exists {
			c.entries[item.Key] = item.Entry
		}
	}
	return nil
}

func TestSendMessagesBatchDedupKeepsAckOrderAndPersistsOnlyMisses(t *testing.T) {
	app, tx, _, outbox, notifier := newSendTestApplication()
	dtos := []SendMessageDTO{sendTestDTO(), sendTestDTO(), sendTestDTO(), sendTestDTO()}
	dtos[0].ClientMessageID = "new-1"
	dtos[1].ClientMessageID = "cached"
	dtos[2].ClientMessageID = "new-2"
	dtos[3].ClientMessageID = "cached"
	dtos[3].SenderID = "other-sender"
	cache := &batchDedupTestCache{tx: tx, entries: make(map[messagecache.DedupKey]messagecache.DedupEntry)}
	for _, i := range []int{1, 3} {
		hash, err := buildMessageRequestHash(dtos[i])
		if err != nil {
			t.Fatal(err)
		}
		key := messagecache.DedupKey{SenderID: dtos[i].SenderID, ClientMsgID: dtos[i].ClientMessageID}
		cache.entries[key] = messagecache.DedupEntry{
			MessageID: "original-" + dtos[i].SenderID, ConversationID: dtos[i].ConversationID,
			Seq: int64(i), RequestHash: hash, AttachmentID: "attachment-" + dtos[i].SenderID, SendTime: 123,
		}
	}
	app.messageCache = cache

	acks, err := app.HandleSendMessages(context.Background(), "conversation", dtos)
	if err != nil {
		t.Fatal(err)
	}
	if cache.batchReads != 1 || len(cache.keys) != 4 || len(acks) != 4 {
		t.Fatalf("batch reads=%d keys=%+v acks=%+v", cache.batchReads, cache.keys, acks)
	}
	for i, dto := range dtos {
		if cache.keys[i].SenderID != dto.SenderID || cache.keys[i].ClientMsgID != dto.ClientMessageID || acks[i].ClientMessageID != dto.ClientMessageID || acks[i].Status != string(protocol.AckStatusSent) {
			t.Fatalf("input %d lost its identity: key=%+v ack=%+v", i, cache.keys[i], acks[i])
		}
	}
	for _, i := range []int{1, 3} {
		if acks[i].MessageID != "original-"+dtos[i].SenderID || acks[i].Seq != int64(i) || acks[i].AttachmentID != "attachment-"+dtos[i].SenderID || acks[i].SendTime != 123 {
			t.Fatalf("cached ACK was not reused at input %d: %+v", i, acks[i])
		}
	}
	if acks[0].Seq != 6 || acks[2].Seq != 7 || tx.calls != 1 || len(outbox.entries) != 2 || notifier.calls.Load() != 1 || len(cache.writes) != 2 {
		t.Fatalf("only misses should be persisted: acks=%+v tx=%d outbox=%d notify=%d writes=%+v", acks, tx.calls, len(outbox.entries), notifier.calls.Load(), cache.writes)
	}
	for i, clientMsgID := range []string{"new-1", "new-2"} {
		var event protocol.MessageEvent
		if err := json.Unmarshal(outbox.entries[i].Payload, &event); err != nil {
			t.Fatal(err)
		}
		if event.ClientMsgId != clientMsgID {
			t.Fatalf("cached message reached the outbox: %+v", event)
		}
	}
}

func TestSendMessagesAllCacheHitsSkipTransaction(t *testing.T) {
	app, tx, _, outbox, notifier := newSendTestApplication()
	dto := sendTestDTO()
	hash, err := buildMessageRequestHash(dto)
	if err != nil {
		t.Fatal(err)
	}
	key := messagecache.DedupKey{SenderID: dto.SenderID, ClientMsgID: dto.ClientMessageID}
	cache := &batchDedupTestCache{tx: tx, entries: map[messagecache.DedupKey]messagecache.DedupEntry{
		key: {MessageID: "original", ConversationID: dto.ConversationID, Seq: 3, RequestHash: hash},
	}}
	app.messageCache = cache
	acks, err := app.HandleSendMessages(context.Background(), "conversation", []SendMessageDTO{dto, dto})
	if err != nil || len(acks) != 2 {
		t.Fatalf("cached batch failed: acks=%+v err=%v", acks, err)
	}
	for _, ack := range acks {
		if ack.MessageID != "original" || ack.Seq != 3 {
			t.Fatalf("unexpected cached result: %+v", ack)
		}
	}
	if cache.batchReads != 1 || tx.calls != 0 || len(outbox.entries) != 0 || notifier.calls.Load() != 0 || len(cache.writes) != 0 {
		t.Fatal("all cache hits must skip persistence, cache writes, and producer notification")
	}
}

func TestSendMessagesBatchDedupRejectsHashConflict(t *testing.T) {
	app, tx, _, outbox, notifier := newSendTestApplication()
	dto := sendTestDTO()
	app.messageCache = &batchDedupTestCache{tx: tx, entries: map[messagecache.DedupKey]messagecache.DedupEntry{
		{SenderID: dto.SenderID, ClientMsgID: dto.ClientMessageID}: {
			MessageID: "original", ConversationID: dto.ConversationID, Seq: 3, RequestHash: "different-request",
		},
	}}
	acks, err := app.HandleSendMessages(context.Background(), "conversation", []SendMessageDTO{dto})
	if err != nil || len(acks) != 1 || acks[0].Status != string(protocol.AckStatusFailed) || acks[0].Error != messageentity.ErrClientMessageConflict.Error() || tx.calls != 0 || len(outbox.entries) != 0 || notifier.calls.Load() != 0 {
		t.Fatalf("hash conflict must not write: acks=%+v err=%v", acks, err)
	}
}

func TestSendMessagesBatchCacheFailureFallsBackToDatabase(t *testing.T) {
	app, tx, _, outbox, _ := newSendTestApplication()
	cache := &batchDedupTestCache{tx: tx, err: errors.New("Redis unavailable")}
	app.messageCache = cache
	first, second := sendTestDTO(), sendTestDTO()
	second.ClientMessageID = "client-2"
	acks, err := app.HandleSendMessages(context.Background(), "conversation", []SendMessageDTO{first, second})
	if err != nil || len(acks) != 2 || cache.batchReads != 1 || tx.calls != 1 || len(outbox.entries) != 2 {
		t.Fatalf("cache failure must keep database fallback: acks=%+v err=%v reads=%d tx=%d outbox=%d", acks, err, cache.batchReads, tx.calls, len(outbox.entries))
	}
}

func TestSendMessagesWithoutClientIDsSkipsDedupQueries(t *testing.T) {
	app, tx, _, outbox, _ := newSendTestApplication()
	cache := &batchDedupTestCache{tx: tx}
	app.messageCache = cache
	dto := sendTestDTO()
	dto.ClientMessageID = ""
	acks, err := app.HandleSendMessages(context.Background(), "conversation", []SendMessageDTO{dto})
	if err != nil || len(acks) != 1 || tx.calls != 1 || len(outbox.entries) != 1 || cache.batchReads != 0 || len(cache.writes) != 0 {
		t.Fatalf("messages without client IDs must skip dedup cache: acks=%+v err=%v reads=%d", acks, err, cache.batchReads)
	}
}
