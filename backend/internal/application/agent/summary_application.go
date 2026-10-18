package agent

import (
	agentport "IM_backend/internal/application/ports/agent"
	idport "IM_backend/internal/application/ports/id"
	summarycache "IM_backend/internal/application/ports/persistence/cache/summary"
	conversationrepo "IM_backend/internal/application/ports/persistence/repository/conversation"
	messagerepo "IM_backend/internal/application/ports/persistence/repository/message"
	roomrepo "IM_backend/internal/application/ports/persistence/repository/room"
	summaryrepo "IM_backend/internal/application/ports/persistence/repository/summary"
	txmanager "IM_backend/internal/application/ports/persistence/tx_manager"
	conversationentity "IM_backend/internal/domain/conversation/entity"
	conversationvo "IM_backend/internal/domain/conversation/value_object"
	messagevo "IM_backend/internal/domain/message/value_object"
	roomvo "IM_backend/internal/domain/room/value_object"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

var (
	ErrUnknownConversation    = errors.New("未知会话")
	ErrSummaryRunNotFound     = errors.New("摘要任务不存在")
	ErrSummaryRunInitializing = errors.New("摘要任务正在初始化，请稍后重试")
	ErrSummaryForbidden       = errors.New("无权访问摘要任务")
	ErrSummaryInvalidState    = errors.New("摘要任务当前状态不允许此操作")
	ErrSummaryInvalidResponse = errors.New("摘要 Agent 返回了无效响应")
)

type SummaryEvent struct {
	Name string
	Data agentport.SummaryResponse
}

type SummarySubscription struct {
	Events <-chan SummaryEvent
	Close  func()
}

type SummaryRunInfo struct {
	SummaryRunID string `json:"summaryRunId"`
	RoomID       string `json:"roomId"`
	Status       string `json:"status"`
}

const summaryScopeRunTTL = 2 * time.Minute

type SummaryService interface {
	InitSummaryRun(ctx context.Context, roomID, userID string) (*SummaryRunInfo, error)
	OpenSummarySSE(ctx context.Context, roomID, userID, summaryRunID string) (*SummarySubscription, error)
	RetrySummary(ctx context.Context, roomID, userID, summaryRunID string) error
	CancelSummary(ctx context.Context, roomID, userID, summaryRunID string) error
}

type subscription struct {
	ch   chan SummaryEvent
	done chan struct{}
}

type SummaryApplication struct {
	rootCtx           context.Context
	client            agentport.SummaryClient
	ids               idport.Generator
	messages          messagerepo.MessageRepository
	conversations     conversationrepo.ConversationRepository
	userConversations conversationrepo.UserConversationRepository
	roomUsers         roomrepo.RoomUserRepository
	runs              summaryrepo.SummaryRunRepository
	txManager         txmanager.TxManager
	snapshots         summarycache.RoomUnreadSnapshotCache
	scopeRuns         summarycache.SummaryScopeRunStore

	mu                 sync.RWMutex
	subscribers        map[string]map[uint64]*subscription
	nextSubscriptionID uint64
}

func NewSummaryApplication(
	rootCtx context.Context,
	client agentport.SummaryClient,
	ids idport.Generator,
	messages messagerepo.MessageRepository,
	conversations conversationrepo.ConversationRepository,
	userConversations conversationrepo.UserConversationRepository,
	roomUsers roomrepo.RoomUserRepository,
	runs summaryrepo.SummaryRunRepository,
	txManager txmanager.TxManager,
	snapshots summarycache.RoomUnreadSnapshotCache,
	scopeRuns summarycache.SummaryScopeRunStore,
) *SummaryApplication {
	if rootCtx == nil {
		rootCtx = context.Background()
	}
	return &SummaryApplication{
		rootCtx: rootCtx, client: client, ids: ids, messages: messages,
		conversations: conversations, userConversations: userConversations,
		roomUsers: roomUsers, runs: runs, txManager: txManager,
		snapshots: snapshots, scopeRuns: scopeRuns,
		subscribers: make(map[string]map[uint64]*subscription),
	}
}

func (sa *SummaryApplication) InitSummaryRun(ctx context.Context, roomID, userID string) (*SummaryRunInfo, error) {
	// 校验当前用户是否有权限，防止出现非法的越界访问
	if err := sa.checkMember(roomID, userID); err != nil {
		return nil, err
	}
	if run, found, err := sa.findScopeRun(ctx, userID, roomID); err != nil {
		return nil, err
	} else if found {
		return toSummaryRunInfo(run), nil
	}

	conversation, err := sa.conversations.FindRoomConversation(ctx, roomID)
	if err != nil {
		return nil, err
	}
	if conversation == nil || conversation.Convtype != conversationvo.RoomChat {
		return nil, ErrUnknownConversation
	}
	fromSeq, toSeq, err := sa.resolveRange(ctx, roomID, userID, conversation)
	if err != nil {
		return nil, err
	}
	messages, err := sa.messages.ListBySeqRange(ctx, conversation.ConversationId, fromSeq, toSeq)
	if err != nil {
		return nil, err
	}
	roomMessages := make([]agentport.RoomMessage, 0, len(messages))
	for _, message := range messages {
		if message == nil {
			continue
		}
		status := "normal"
		if message.Status != messagevo.WithDraw {
			// 已经撤回的消息是不可以继续查看的
			roomMessages = append(roomMessages, agentport.RoomMessage{
				RoomID: roomID, MessageID: message.MessageId, SenderID: message.SenderId,
				Seq: message.Seq, Type: int32(message.Type), Content: message.Content,
				Status: status, SendTime: time.UnixMilli(message.SendTime),
			})
		}
	}

	summaryRunID, err := sa.ids.Generate()
	if err != nil {
		return nil, err
	}

	var scopeLockToken string
	if sa.scopeRuns != nil {
		existingSummaryRunID, lockToken, acquired, err := sa.scopeRuns.ClaimSummaryRunIDByScope(
			ctx, userID, roomID, summaryRunID, summaryScopeRunTTL,
		)
		if err != nil {
			return nil, err
		}

		// 没抢占到锁
		if !acquired {
			if existingSummaryRunID == "" {
				return nil, ErrSummaryRunInitializing
			}

			// double check
			existingRun, err := sa.runs.FindRun(ctx, existingSummaryRunID)
			if err != nil {
				return nil, err
			}

			if existingRun == nil {
				return nil, ErrSummaryRunInitializing
			}

			return toSummaryRunInfo(existingRun), nil
		}

		// 只有抢占成功时，才继续使用新生成的 summaryRunID。
		summaryRunID = existingSummaryRunID
		scopeLockToken = lockToken
	}

	requestID, err := sa.ids.Generate()
	if err != nil {
		if sa.scopeRuns != nil {
			_ = sa.scopeRuns.ReleaseSummaryRunIDByScope(ctx, userID, roomID, summaryRunID, scopeLockToken)
		}
		return nil, err
	}

	now := time.Now().UnixMilli()
	if err := sa.runs.CreateRun(ctx, summaryrepo.RunRecord{
		SummaryRunID: summaryRunID, RoomID: roomID, UserID: userID,
		Status: string(agentport.SummaryStatusRunning), FromSeq: fromSeq, ToSeq: toSeq,
		ResponsePayload: "{}", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		if sa.scopeRuns != nil {
			_ = sa.scopeRuns.ReleaseSummaryRunIDByScope(ctx, userID, roomID, summaryRunID, scopeLockToken)
		}
		return nil, err
	}

	// 启动摘要预热，减少用户等待时间，后续通过 summary_run_id 可以获取到历史的结果
	sa.startAgentRun(summaryRunID, scopeLockToken, agentport.SummaryRequest{
		RequestID: requestID, SummaryRunID: summaryRunID,
		Scope:  agentport.Scope{RoomID: roomID, UserID: userID},
		Action: agentport.SummaryActionStart, RoomMessages: roomMessages,
	})
	return &SummaryRunInfo{SummaryRunID: summaryRunID, RoomID: roomID, Status: string(agentport.SummaryStatusRunning)}, nil
}

func (sa *SummaryApplication) findScopeRun(ctx context.Context, userID, roomID string) (*summaryrepo.RunRecord, bool, error) {
	if sa.scopeRuns != nil {
		runID, found, err := sa.scopeRuns.GetSummaryRunIDByScope(ctx, userID, roomID)
		if err != nil {
			return nil, false, err
		}
		if found {
			run, err := sa.runs.FindRun(ctx, runID)
			if err != nil {
				return nil, false, err
			}
			if run == nil {
				return nil, false, ErrSummaryRunInitializing
			}
			return run, true, nil
		}
	}

	// Redis 租约丢失或实例重启后，以数据库中的活动任务作为兜底，防止同一个 scope 再次发起 START。
	run, err := sa.runs.FindActiveRunByScope(ctx, userID, roomID)
	if err != nil {
		return nil, false, err
	}
	return run, run != nil, nil
}

func toSummaryRunInfo(run *summaryrepo.RunRecord) *SummaryRunInfo {
	return &SummaryRunInfo{
		SummaryRunID: run.SummaryRunID,
		RoomID:       run.RoomID,
		Status:       run.Status,
	}
}

func (sa *SummaryApplication) OpenSummarySSE(ctx context.Context, roomID, userID, summaryRunID string) (*SummarySubscription, error) {
	latestRun, err := sa.runs.FindRun(ctx, summaryRunID)
	if err != nil {
		return nil, err
	}
	if latestRun == nil {
		return nil, ErrSummaryRunNotFound
	}
	if latestRun.RoomID != roomID || latestRun.UserID != userID {
		return nil, ErrSummaryForbidden
	}

	sa.mu.Lock()
	sa.nextSubscriptionID++
	id := sa.nextSubscriptionID
	sub := &subscription{ch: make(chan SummaryEvent, 2), done: make(chan struct{})}
	if sa.subscribers[summaryRunID] == nil {
		sa.subscribers[summaryRunID] = make(map[uint64]*subscription)
	}
	sa.subscribers[summaryRunID][id] = sub
	sa.mu.Unlock()

	// 重新查询最新结果，避免出现加入之前已经完成摘要推送的窗口
	latestRun, err = sa.runs.FindRun(ctx, summaryRunID)
	if err != nil {
		sa.removeSubscription(summaryRunID, id)
		return nil, err
	}
	if latestRun == nil {
		sa.removeSubscription(summaryRunID, id)
		return nil, ErrSummaryRunNotFound
	}
	if latestRun.RoomID != roomID || latestRun.UserID != userID {
		sa.removeSubscription(summaryRunID, id)
		return nil, ErrSummaryForbidden
	}

	go func() {
		if latestRun.ResponsePayload != "" &&
			latestRun.ResponsePayload != "{}" {
			var response agentport.SummaryResponse
			if json.Unmarshal([]byte(latestRun.ResponsePayload), &response) == nil {
				sa.sendToSubscription(
					sub,
					SummaryEvent{
						Name: "summary",
						Data: response,
					},
				)

				if response.Status != agentport.SummaryStatusRunning {
					sa.finishRun(summaryRunID)
				}
			}
		}
	}()

	return &SummarySubscription{Events: sub.ch, Close: func() { sa.removeSubscription(summaryRunID, id) }}, nil
}

func (sa *SummaryApplication) RetrySummary(ctx context.Context, roomID, userID, summaryRunID string) error {
	return sa.runAction(ctx, roomID, userID, summaryRunID, agentport.SummaryActionRetry)
}

func (sa *SummaryApplication) CancelSummary(ctx context.Context, roomID, userID, summaryRunID string) error {
	return sa.runAction(ctx, roomID, userID, summaryRunID, agentport.SummaryActionCancel)
}

func (sa *SummaryApplication) runAction(ctx context.Context, roomID, userID, summaryRunID string, action agentport.SummaryAction) error {
	if sa.txManager == nil {
		return errors.New("摘要任务事务管理器未配置")
	}
	requestID, err := sa.ids.Generate()
	if err != nil {
		return err
	}
	if err := sa.txManager.WithinTransaction(ctx, func(tx any) error {
		runRepo := sa.runs.WithTx(tx)
		run, err := runRepo.FindRunForUpdate(ctx, summaryRunID)
		if err != nil {
			return err
		}
		if run == nil {
			return ErrSummaryRunNotFound
		}
		if run.RoomID != roomID || run.UserID != userID {
			return ErrSummaryForbidden
		}
		if run.Status != string(agentport.SummaryStatusWaitingUserDecision) {
			return ErrSummaryInvalidState
		}
		return runRepo.UpdateStatus(ctx, summaryRunID, string(agentport.SummaryStatusRunning))
	}); err != nil {
		return err
	}
	sa.startAgentRun(summaryRunID, "", agentport.SummaryRequest{
		RequestID: requestID, SummaryRunID: summaryRunID,
		Scope: agentport.Scope{RoomID: roomID, UserID: userID}, Action: action,
	})
	return nil
}

func (sa *SummaryApplication) startAgentRun(summaryRunID, lockToken string, request agentport.SummaryRequest) {
	go func() {
		refreshDone, stopLeaseRefresh := sa.startSummaryScopeLeaseRefresh(request.Scope, summaryRunID, lockToken)
		sa.executeRequest(summaryRunID, request.RequestID, func(ctx context.Context) (*agentport.SummaryResponse, error) {
			return sa.client.RunSummary(ctx, request)
		})
		if stopLeaseRefresh != nil {
			stopLeaseRefresh()
			<-refreshDone
		}
	}()
}

// startSummaryScopeLeaseRefresh 只续期 InitSummaryRun 创建的同一条 Redis 租约，不会创建新的锁，也不会生成新的 lockToken。
func (sa *SummaryApplication) startSummaryScopeLeaseRefresh(scope agentport.Scope, summaryRunID, lockToken string) (<-chan struct{}, func()) {
	done := make(chan struct{})
	if sa.scopeRuns == nil || scope.UserID == "" || scope.RoomID == "" || lockToken == "" {
		close(done)
		return done, nil
	}
	cancel := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(summaryScopeRunTTL / 2)
		defer ticker.Stop()
		for {
			select {
			case <-cancel:
				return
			case <-ticker.C:
				_, _ = sa.scopeRuns.RefreshSummaryRunIDByScope(
					sa.rootCtx, scope.UserID, scope.RoomID, summaryRunID, lockToken, summaryScopeRunTTL,
				)
			}
		}
	}()
	return done, func() { close(cancel) }
}

func (sa *SummaryApplication) executeRequest(summaryRunID, requestID string, call func(context.Context) (*agentport.SummaryResponse, error)) {
	response, err := call(sa.rootCtx)
	if err != nil {
		sa.persistFailure(summaryRunID, requestID, err)
		return
	}
	if response == nil || response.Status == "UNSPECIFIED" {
		sa.persistFailure(summaryRunID, requestID, ErrSummaryInvalidResponse)
		return
	}
	if response.SummaryRunID == "" {
		response.SummaryRunID = summaryRunID
	}
	if response.RequestID == "" {
		response.RequestID = requestID
	}
	sa.persistResponse(summaryRunID, response)
}

func (sa *SummaryApplication) persistResponse(summaryRunID string, response *agentport.SummaryResponse) {
	payload, err := json.Marshal(response)
	if err != nil {
		return
	}
	var finishedAt *int64
	if response.Status != agentport.SummaryStatusRunning {
		now := time.Now().UnixMilli()
		finishedAt = &now
	}
	if err := sa.runs.SaveResponse(sa.rootCtx, summaryRunID, response.RequestID, string(response.Status), string(payload), finishedAt); err != nil {
		return
	}
	sa.publish(summaryRunID, SummaryEvent{Name: "summary", Data: *response})
	if response.Status != agentport.SummaryStatusRunning {
		sa.finishRun(summaryRunID)
	}
}

func (sa *SummaryApplication) persistFailure(summaryRunID, requestID string, err error) {
	sa.persistResponse(summaryRunID, &agentport.SummaryResponse{
		RequestID: requestID, SummaryRunID: summaryRunID, Status: agentport.SummaryStatusFailed,
		Error: &agentport.SummaryError{Code: "AGENT_CALL_FAILED", Message: err.Error()},
	})
}

func (sa *SummaryApplication) publish(summaryRunID string, event SummaryEvent) {
	sa.mu.RLock()
	defer sa.mu.RUnlock()
	for _, sub := range sa.subscribers[summaryRunID] {
		select {
		case sub.ch <- event:
		case <-sub.done:
		default:
		}
	}
}

func (sa *SummaryApplication) sendToSubscription(sub *subscription, event SummaryEvent) {
	sa.mu.RLock()
	defer sa.mu.RUnlock()
	select {
	case sub.ch <- event:
	case <-sub.done:
	default:
	}
}

func (sa *SummaryApplication) finishRun(summaryRunID string) {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	for id, sub := range sa.subscribers[summaryRunID] {
		close(sub.done)
		close(sub.ch)
		delete(sa.subscribers[summaryRunID], id)
	}
	delete(sa.subscribers, summaryRunID)
}

func (sa *SummaryApplication) removeSubscription(summaryRunID string, id uint64) {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	if sub := sa.subscribers[summaryRunID][id]; sub != nil {
		close(sub.done)
		close(sub.ch)
		delete(sa.subscribers[summaryRunID], id)
	}
	if len(sa.subscribers[summaryRunID]) == 0 {
		delete(sa.subscribers, summaryRunID)
	}
}

func (sa *SummaryApplication) checkMember(roomID, userID string) error {
	member, err := sa.roomUsers.GetRelationByIDs(userID, roomID)
	if err != nil {
		return err
	}
	if member == nil || member.Status != roomvo.Activate {
		return ErrSummaryForbidden
	}
	return nil
}

func (sa *SummaryApplication) resolveRange(ctx context.Context, roomID, userID string, conversation *conversationentity.Conversation) (int64, int64, error) {
	if snapshot, found, err := sa.snapshots.GetActive(ctx, userID, roomID); err != nil {
		return 0, 0, err
	} else if found && snapshot.RoomID == roomID {
		return snapshot.FromSeq, snapshot.ToSeq, nil
	}
	userConversation, err := sa.userConversations.GetUserConversation(ctx, userID, conversation.ConversationId)
	if err != nil {
		return 0, 0, err
	}
	from := int64(1)
	if userConversation != nil {
		from = userConversation.LastReadSeq + 1
	}

	to := conversation.LatestSeq

	//  重新回填缓存，避免出现摘要不稳定的情况
	_ = sa.snapshots.SetActive(ctx, userID, roomID, summarycache.RoomUnreadSnapshot{
		FromSeq: from,
		ToSeq:   to,
		RoomID:  roomID,
	}, summaryScopeRunTTL)

	return from, conversation.LatestSeq, nil
}
