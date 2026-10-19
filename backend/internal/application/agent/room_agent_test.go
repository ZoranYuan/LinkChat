package agent

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	roomcache "IM_backend/internal/application/ports/persistence/cache/room"
	summarycache "IM_backend/internal/application/ports/persistence/cache/summary"
	conversationrepo "IM_backend/internal/application/ports/persistence/repository/conversation"
	messagerepo "IM_backend/internal/application/ports/persistence/repository/message"
	roomrepo "IM_backend/internal/application/ports/persistence/repository/room"
	conversationentity "IM_backend/internal/domain/conversation/entity"
	conversationvo "IM_backend/internal/domain/conversation/value_object"
	messageentity "IM_backend/internal/domain/message/entity"
	messagevo "IM_backend/internal/domain/message/value_object"
	roomentity "IM_backend/internal/domain/room/entity"
	roomvo "IM_backend/internal/domain/room/value_object"
)

type roomAgentTestRepository struct {
	roomrepo.RoomUserRepository
	member  *roomentity.RoomUser
	err     error
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (r *roomAgentTestRepository) GetRelationByIDs(_, _ string) (*roomentity.RoomUser, error) {
	r.calls.Add(1)
	if r.entered != nil {
		r.entered <- struct{}{}
	}
	if r.release != nil {
		<-r.release
	}
	return r.member, r.err
}

type roomAgentTestMemberCache struct {
	roomcache.RoomMemberCache
	mu    sync.Mutex
	state *roomcache.MemberState
	hit   bool
}

type roomAgentTestConversationRepository struct {
	conversationrepo.ConversationRepository
	conversation *conversationentity.Conversation
	err          error
	calls        atomic.Int32
}

func (r *roomAgentTestConversationRepository) FindRoomConversation(context.Context, string) (*conversationentity.Conversation, error) {
	r.calls.Add(1)
	return r.conversation, r.err
}

type roomAgentTestUserConversationRepository struct {
	conversationrepo.UserConversationRepository
	conversation *conversationentity.UserConversation
	err          error
	calls        atomic.Int32
}

func (r *roomAgentTestUserConversationRepository) GetUserConversation(context.Context, string, string) (*conversationentity.UserConversation, error) {
	r.calls.Add(1)
	return r.conversation, r.err
}

type roomAgentTestMessageRepository struct {
	messagerepo.MessageRepository
	messages []*messageentity.Message
	err      error
	calls    atomic.Int32
	convID   string
	beginSeq int64
	endSeq   int64
}

func (r *roomAgentTestMessageRepository) ListBySeqRange(_ context.Context, conversationID string, beginSeq, endSeq int64) ([]*messageentity.Message, error) {
	r.calls.Add(1)
	r.convID, r.beginSeq, r.endSeq = conversationID, beginSeq, endSeq
	return r.messages, r.err
}

type roomAgentTestUnreadSnapshotCache struct {
	summarycache.RoomUnreadSnapshotCache
	mu       sync.Mutex
	snapshot *summarycache.RoomUnreadSnapshot
	getErr   error
	setErr   error
	sets     atomic.Int32
}

func (c *roomAgentTestUnreadSnapshotCache) GetActive(context.Context, string, string) (*summarycache.RoomUnreadSnapshot, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.getErr != nil {
		return nil, false, c.getErr
	}
	if c.snapshot == nil {
		return nil, false, nil
	}
	snapshot := *c.snapshot
	return &snapshot, true, nil
}

func (c *roomAgentTestUnreadSnapshotCache) SetActive(_ context.Context, _, _ string, snapshot summarycache.RoomUnreadSnapshot, _ time.Duration) error {
	c.sets.Add(1)
	if c.setErr != nil {
		return c.setErr
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	value := snapshot
	c.snapshot = &value
	return nil
}

func (c *roomAgentTestMemberCache) GetMember(context.Context, string, string) (*roomcache.MemberState, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.hit || c.state == nil {
		return nil, c.hit, nil
	}
	state := *c.state
	return &state, true, nil
}

func (c *roomAgentTestMemberCache) SetMemberIfVersionGreater(_ context.Context, _, _ string, state *roomcache.MemberState) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hit && c.state != nil && c.state.Version >= state.Version {
		return false, nil
	}
	value := *state
	c.state, c.hit = &value, true
	return true, nil
}

func (c *roomAgentTestMemberCache) SetMemberNotFound(context.Context, string, string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hit && c.state != nil {
		return nil
	}
	c.state, c.hit = nil, true
	return nil
}

func newMembershipOnlyRoomAgentApplication(repository roomrepo.RoomUserRepository, cache roomcache.RoomMemberCache) *RoomAgentApplication {
	return NewRoomAgentApplication(repository, cache, nil, nil, nil, nil, 0)
}

func TestValidateRoomMembershipUsesNegativeCache(t *testing.T) {
	repository := &roomAgentTestRepository{}
	cache := &roomAgentTestMemberCache{hit: true}
	app := newMembershipOnlyRoomAgentApplication(repository, cache)

	err := app.validateRoomMembership(context.Background(), "room-1", "user-1")
	if !errors.Is(err, ErrRoomAgentAccessDenied) {
		t.Fatalf("负缓存命中应拒绝访问：err=%v", err)
	}
	if got := repository.calls.Load(); got != 0 {
		t.Fatalf("负缓存命中不应查询 MySQL：calls=%d", got)
	}
}

func TestValidateRoomMembershipCachesMissingMember(t *testing.T) {
	repository := &roomAgentTestRepository{err: roomentity.ErrMemberNotFound}
	cache := &roomAgentTestMemberCache{}
	app := newMembershipOnlyRoomAgentApplication(repository, cache)

	for i := 0; i < 2; i++ {
		if err := app.validateRoomMembership(context.Background(), "room-1", "user-1"); !errors.Is(err, ErrRoomAgentAccessDenied) {
			t.Fatalf("不存在的成员应拒绝访问：err=%v", err)
		}
	}
	if got := repository.calls.Load(); got != 1 {
		t.Fatalf("写入负缓存后第二次不应回源 MySQL：calls=%d", got)
	}
}

func TestValidateRoomMembershipWarmsMemberCache(t *testing.T) {
	repository := &roomAgentTestRepository{member: &roomentity.RoomUser{
		UserId: "user-1", RoomId: "room-1", Status: roomvo.Activate,
		Role: roomvo.RegularUser, Version: 4,
	}}
	cache := &roomAgentTestMemberCache{}
	app := newMembershipOnlyRoomAgentApplication(repository, cache)

	if err := app.validateRoomMembership(context.Background(), "room-1", "user-1"); err != nil {
		t.Fatalf("有效成员应通过校验：err=%v", err)
	}
	state, hit, err := cache.GetMember(context.Background(), "room-1", "user-1")
	if err != nil || !hit || state == nil || state.Version != 4 || state.Status != roomvo.Activate {
		t.Fatalf("MySQL 结果应回填成员缓存：state=%+v hit=%v err=%v", state, hit, err)
	}
}

func TestValidateRoomMembershipSingleflightCoalescesMisses(t *testing.T) {
	const callers = 12
	repository := &roomAgentTestRepository{
		member: &roomentity.RoomUser{
			UserId: "user-1", RoomId: "room-1", Status: roomvo.Activate,
			Role: roomvo.RegularUser, Version: 1,
		},
		entered: make(chan struct{}, callers),
		release: make(chan struct{}),
	}
	app := newMembershipOnlyRoomAgentApplication(repository, &roomAgentTestMemberCache{})
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wait.Done()
			<-start
			if err := app.validateRoomMembership(context.Background(), "room-1", "user-1"); err != nil {
				t.Errorf("有效成员校验失败：%v", err)
			}
		}()
	}
	close(start)
	select {
	case <-repository.entered:
	case <-time.After(time.Second):
		t.Fatal("singleflight 未启动 MySQL 查询")
	}
	time.Sleep(20 * time.Millisecond)
	close(repository.release)
	wait.Wait()
	if got := repository.calls.Load(); got != 1 {
		t.Fatalf("并发缓存未命中应合并为一次 MySQL 查询：calls=%d", got)
	}
}

func TestGetRoomAgentContextFallsBackToMySQLAndBuildsTextSnapshot(t *testing.T) {
	memberRepository := &roomAgentTestRepository{member: &roomentity.RoomUser{
		UserId: "user-1", RoomId: "room-1", Status: roomvo.Activate,
	}}
	memberCache := &roomAgentTestMemberCache{hit: true, state: &roomcache.MemberState{Status: roomvo.Activate}}
	conversationRepository := &roomAgentTestConversationRepository{conversation: &conversationentity.Conversation{
		ConversationId: "conversation-1", Convtype: conversationvo.RoomChat, RoomId: "room-1",
	}}
	userConversationRepository := &roomAgentTestUserConversationRepository{conversation: &conversationentity.UserConversation{
		UserId: "user-1", ConversationId: "conversation-1", SummaryUnreadBeginSeq: 10, SummaryUnreadEndSeq: 13,
	}}
	clientMsgID := "client-10"
	messageRepository := &roomAgentTestMessageRepository{messages: []*messageentity.Message{
		{MessageId: "message-10", ConversationId: "conversation-1", SenderId: "user-2", ClientMsgId: &clientMsgID, Seq: 10, Type: messagevo.Text, Content: "会议在三点开始", Status: messagevo.Normal, SendTime: 1000},
		{MessageId: "message-11", ConversationId: "conversation-1", SenderId: "user-2", Seq: 11, Type: messagevo.Text, Content: "这条已撤回", Status: messagevo.WithDraw, SendTime: 1001},
		{MessageId: "message-12", ConversationId: "conversation-1", SenderId: "user-2", Seq: 12, Type: messagevo.Image, Content: "image-file-id", Status: messagevo.Normal, SendTime: 1002},
		{MessageId: "message-13", ConversationId: "conversation-1", SenderId: "user-3", Seq: 13, Type: messagevo.Text, Content: "地点在三楼会议室", Status: messagevo.Normal, SendTime: 1003},
	}}
	snapshotCache := &roomAgentTestUnreadSnapshotCache{getErr: errors.New("redis unavailable")}
	app := NewRoomAgentApplication(
		memberRepository,
		memberCache,
		conversationRepository,
		userConversationRepository,
		messageRepository,
		snapshotCache,
		2*time.Minute,
	)

	result, err := app.GetRoomAgentContext(context.Background(), "room-1", "user-1")
	if err != nil {
		t.Fatalf("MySQL 回源组装摘要上下文失败：%v", err)
	}
	if result.UserID != "user-1" || result.UnreadBeginSeq != 10 || result.UnreadEndSeq != 13 {
		t.Fatalf("用户或固定未读边界不正确：result=%+v", result)
	}
	if result.UnreadMessageCount != 2 || len(result.Messages) != 2 {
		t.Fatalf("应只返回未撤回的文本消息：count=%d messages=%+v", result.UnreadMessageCount, result.Messages)
	}
	if result.Messages[0].MessageID != "message-10" || result.Messages[0].ClientMsgID == nil || *result.Messages[0].ClientMsgID != clientMsgID || result.Messages[1].MessageID != "message-13" {
		t.Fatalf("消息字段或 seq 顺序不正确：messages=%+v", result.Messages)
	}
	if messageRepository.convID != "conversation-1" || messageRepository.beginSeq != 10 || messageRepository.endSeq != 13 {
		t.Fatalf("查询未使用固定会话和 seq 范围：conv=%s range=[%d,%d]", messageRepository.convID, messageRepository.beginSeq, messageRepository.endSeq)
	}
	if userConversationRepository.calls.Load() != 1 || snapshotCache.sets.Load() != 1 {
		t.Fatalf("Redis 失败后应回源一次 MySQL 并尝试回填：mysql=%d cache_set=%d", userConversationRepository.calls.Load(), snapshotCache.sets.Load())
	}
	if result.ThreadID != roomAgentThreadID("user-1", "room-1") || result.ThreadID == roomAgentThreadID("user-2", "room-1") {
		t.Fatalf("thread_id 应稳定且按用户和房间隔离：%s", result.ThreadID)
	}
}

func TestGetRoomAgentContextUsesCachedSnapshotAndAllowsEmptyRange(t *testing.T) {
	memberRepository := &roomAgentTestRepository{member: &roomentity.RoomUser{
		UserId: "user-1", RoomId: "room-1", Status: roomvo.Activate,
	}}
	memberCache := &roomAgentTestMemberCache{hit: true, state: &roomcache.MemberState{Status: roomvo.Activate}}
	conversationRepository := &roomAgentTestConversationRepository{conversation: &conversationentity.Conversation{
		ConversationId: "conversation-1", Convtype: conversationvo.RoomChat,
	}}
	userConversationRepository := &roomAgentTestUserConversationRepository{err: errors.New("should not query MySQL snapshot")}
	messageRepository := &roomAgentTestMessageRepository{}
	snapshotCache := &roomAgentTestUnreadSnapshotCache{snapshot: &summarycache.RoomUnreadSnapshot{RoomID: "room-1"}}
	app := NewRoomAgentApplication(
		memberRepository,
		memberCache,
		conversationRepository,
		userConversationRepository,
		messageRepository,
		snapshotCache,
		2*time.Minute,
	)

	result, err := app.GetRoomAgentContext(context.Background(), "room-1", "user-1")
	if err != nil {
		t.Fatalf("空未读范围应返回空上下文：%v", err)
	}
	if result.UnreadBeginSeq != 0 || result.UnreadEndSeq != 0 || result.UnreadMessageCount != 0 || len(result.Messages) != 0 {
		t.Fatalf("空快照结果不正确：%+v", result)
	}
	if userConversationRepository.calls.Load() != 0 || messageRepository.calls.Load() != 0 {
		t.Fatalf("Redis 空快照命中后不应回源 MySQL：snapshot=%d messages=%d", userConversationRepository.calls.Load(), messageRepository.calls.Load())
	}
}
