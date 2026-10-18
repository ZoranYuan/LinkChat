package friend

import (
	conversationapp "IM_backend/internal/application/conversation"
	eventbus "IM_backend/internal/application/ports/eventbus"
	idport "IM_backend/internal/application/ports/id"
	outboxport "IM_backend/internal/application/ports/outbox"
	friendcache "IM_backend/internal/application/ports/persistence/cache/friend"
	conversationrepo "IM_backend/internal/application/ports/persistence/repository/conversation"
	friendrepo "IM_backend/internal/application/ports/persistence/repository/friend"
	messagerepo "IM_backend/internal/application/ports/persistence/repository/message"
	userrepo "IM_backend/internal/application/ports/persistence/repository/user"
	txmanager "IM_backend/internal/application/ports/persistence/tx_manager"
	conversationentity "IM_backend/internal/domain/conversation/entity"
	conversationvo "IM_backend/internal/domain/conversation/value_object"
	friendentity "IM_backend/internal/domain/friend/entity"
	friendvo "IM_backend/internal/domain/friend/value_object"
	messageentity "IM_backend/internal/domain/message/entity"
	messagevo "IM_backend/internal/domain/message/value_object"
	"IM_backend/internal/shared/protocol"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"
	"unicode/utf8"
)

type FriendRequestApplication struct {
	friendRequestRepository friendrepo.FriendRequestRepository
	userRepository          userrepo.UserRepository
	messageRepository       messagerepo.MessageRepository
	friendRepository        friendrepo.FriendRepository
	conversationRepository  conversationrepo.ConversationRepository
	conversationApplication *conversationapp.ConversationApplication
	userConvRepository      conversationrepo.UserConversationRepository
	friendCache             friendcache.FriendCache
	txManager               txmanager.TxManager
	idGenerator             idport.Generator
	outboxRepository        outboxport.OutboxRepository
	producerNotifier        eventbus.ProducerNotifier
}

func NewFriendRequestApplication(
	friendRequestRepository friendrepo.FriendRequestRepository,
	userRepository userrepo.UserRepository,
	messageRepository messagerepo.MessageRepository,
	conversationRepository conversationrepo.ConversationRepository,
	userConvRepository conversationrepo.UserConversationRepository,
	friendRepository friendrepo.FriendRepository,
	friendCache friendcache.FriendCache,
	txManager txmanager.TxManager,
	idGenerator idport.Generator,
	outboxRepository outboxport.OutboxRepository,
) *FriendRequestApplication {
	return &FriendRequestApplication{
		friendRequestRepository: friendRequestRepository,
		userRepository:          userRepository,
		messageRepository:       messageRepository,
		userConvRepository:      userConvRepository,
		conversationRepository:  conversationRepository,
		friendRepository:        friendRepository,
		friendCache:             friendCache,
		txManager:               txManager,
		idGenerator:             idGenerator,
		outboxRepository:        outboxRepository,
	}
}

// SetProducerNotifier 在事务提交后唤醒 Kafka Producer。
func (fa *FriendRequestApplication) SetProducerNotifier(notifier eventbus.ProducerNotifier) {
	if fa == nil {
		return
	}
	fa.producerNotifier = notifier
}

func (fa *FriendRequestApplication) SetConversationApplication(application *conversationapp.ConversationApplication) {
	fa.conversationApplication = application
}

func (fa *FriendRequestApplication) notifyProducer() {
	if fa != nil && fa.producerNotifier != nil {
		fa.producerNotifier.Notify()
	}
}

func (fa *FriendRequestApplication) createNewFriendRequest(
	ctx context.Context,
	userID string,
	peerUserID string,
	message string,
	repository friendrepo.FriendRequestRepository,
	outboxRepository outboxport.OutboxRepository,
) (*FriendRequestDTO, error) {
	requestID, err := fa.idGenerator.Generate()
	if err != nil {
		return nil, fmt.Errorf("生成好友申请 ID 失败：%w", err)
	}

	request, err := friendentity.NewFriendRequest(
		requestID,
		userID,
		peerUserID,
		message,
	)
	if err != nil {
		return nil, err
	}

	record, err := repository.Create(request)
	if err != nil {
		return nil, fmt.Errorf("创建好友申请失败：%w", err)
	}

	if err := fa.createFriendRequestOutbox(ctx, record, outboxRepository); err != nil {
		return nil, fmt.Errorf("创建好友申请提醒失败：%w", err)
	}

	dto := toFriendRequestDTO(record)
	fa.fillApplicantNickName(&dto)
	return &dto, nil
}

func (fa *FriendRequestApplication) createFriendRequestOutbox(
	ctx context.Context,
	record *friendentity.FriendRequest,
	repository outboxport.OutboxRepository,
) error {
	if repository == nil || record == nil {
		return nil
	}

	payload, err := json.Marshal(protocol.FriendRequestCreatedEvent{
		RequestId:       record.RequestId,
		ApplicantUserId: record.ApplicantUserId,
		PeerUserId:      record.PeerUserId,
		Message:         record.Message,
		ApplyTime:       record.ApplyTime,
	})
	if err != nil {
		return err
	}

	return repository.Create(ctx, &outboxport.Entry{
		EventType:    protocol.EventFriendRequestCreated,
		PartitionKey: record.PeerUserId,
		Payload:      payload,
	})
}

func (fa *FriendRequestApplication) reRequest(
	ctx context.Context,
	record *friendentity.FriendRequest,
	message string,
	repository friendrepo.FriendRequestRepository,
	outboxRepository outboxport.OutboxRepository,
) (*FriendRequestDTO, error) {
	requestId, err := fa.idGenerator.Generate()
	if err != nil {
		return nil, fmt.Errorf("生成好友申请 ID 失败：%w", err)
	}

	if err := record.ReRequest(requestId, message); err != nil {
		return nil, err
	}

	if err := repository.ReRequest(record); err != nil {
		return nil, fmt.Errorf("更新好友申请失败：%w", err)
	}

	if err := fa.createFriendRequestOutbox(ctx, record, outboxRepository); err != nil {
		return nil, fmt.Errorf("创建好友申请提醒失败：%w", err)
	}

	dto := toFriendRequestDTO(record)
	fa.fillApplicantNickName(&dto)
	return &dto, nil
}

func validateFriendRequest(
	applicantUserID string,
	peerUserID string,
	message string,
) error {
	if applicantUserID == "" || peerUserID == "" {
		return ErrEmptyUserId
	}
	if applicantUserID == peerUserID {
		return ErrSelfRequest
	}
	if utf8.RuneCountInString(message) > 200 {
		return ErrTooManyMessage
	}
	return nil
}

func (fa *FriendRequestApplication) CreateFriendRequest(
	userID string,
	peerUserID string,
	message string,
) (*FriendRequestDTO, error) {
	if err := validateFriendRequest(userID, peerUserID, message); err != nil {
		return nil, err
	}

	relation, err := fa.friendRepository.FindRelation(userID, peerUserID)
	if err != nil {
		return nil, fmt.Errorf("查询好友关系失败：%w", err)
	}
	if relation != nil {
		return nil, ErrAlreadyFriends
	}

	user, err := fa.userRepository.FindByUserID(peerUserID)
	if err != nil {
		return nil, fmt.Errorf("查询目标用户失败：%w", err)
	}
	if user == nil {
		return nil, ErrUserNotFound
	}

	reverse, err := fa.friendRequestRepository.FindLatestRequest(peerUserID, userID)
	if err != nil {
		return nil, fmt.Errorf("查询反向好友申请失败：%w", err)
	}
	if reverse != nil && reverse.Status == friendvo.Pending {
		if err := reverse.Accept(userID); err != nil {
			if errors.Is(err, friendentity.ErrDuplicateRequestOperation) {
				return nil, ErrDuplicateOperation
			}
			return nil, err
		}
		if err := fa.acceptFriendRequest(reverse, userID); err != nil {
			return nil, err
		}
		dto := toFriendRequestDTO(reverse)
		fa.fillApplicantNickName(&dto)
		return &dto, nil
	}

	record, err := fa.friendRequestRepository.FindLatestRequest(
		userID,
		peerUserID,
	)
	if err != nil {
		return nil, fmt.Errorf("查询最新好友申请失败：%w", err)
	}

	if record == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var result *FriendRequestDTO
		if err := fa.txManager.WithinTransaction(ctx, func(tx any) error {
			dto, err := fa.createNewFriendRequest(
				ctx,
				userID,
				peerUserID,
				message,
				fa.friendRequestRepository.WithTx(tx),
				fa.outboxRepository.WithTx(tx),
			)
			if err != nil {
				return err
			}
			result = dto
			return nil
		}); err != nil {
			return nil, err
		}
		fa.notifyProducer()
		return result, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var result *FriendRequestDTO
	if err := fa.txManager.WithinTransaction(ctx, func(tx any) error {
		dto, err := fa.reRequest(
			ctx,
			record,
			message,
			fa.friendRequestRepository.WithTx(tx),
			fa.outboxRepository.WithTx(tx),
		)
		if err != nil {
			return err
		}
		result = dto
		return nil
	}); err != nil {
		return nil, err
	}
	fa.notifyProducer()
	return result, nil
}

func (fa *FriendRequestApplication) Refuse(requestId string, userId string) error {
	record, err := fa.friendRequestRepository.FindByRequestID(requestId)

	if err != nil {
		return err
	}

	if err := record.Refuse(userId); err != nil {
		return err
	}

	if err := fa.friendRequestRepository.OperateRequest(record.RequestId, int(friendvo.Pending), int(record.Status)); err != nil {
		return err
	}

	return nil
}

func (fa *FriendRequestApplication) Accept(requestId string, userId string) error {
	record, err := fa.friendRequestRepository.FindByRequestID(requestId)

	if err != nil {
		return err
	}

	if err := record.Accept(userId); err != nil {
		if errors.Is(err, friendentity.ErrDuplicateRequestOperation) {
			return ErrDuplicateOperation
		} else {
			return ErrUnknown
		}
	}

	return fa.acceptFriendRequest(record, userId)
}

func (fa *FriendRequestApplication) acceptFriendRequest(
	record *friendentity.FriendRequest,
	userID string,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 初始化元信息
	convId, err := fa.idGenerator.Generate()
	if err != nil {
		return err
	}

	// 创建会话
	conv := conversationentity.NewConversation(
		convId,
		record.ApplicantUserId,
		record.PeerUserId,
		int(conversationvo.PrivateChat),
		0,
		"",
	)

	toUserConv := conversationentity.BuildUserConversation(
		userID,
		convId,
		0,
	)

	fromUserConv := conversationentity.BuildUserConversation(
		record.ApplicantUserId,
		convId,
		0,
	)

	messages := []*messageentity.Message{}

	// 初始化数据
	if record.Message != "" {
		greetMessageId, err := fa.idGenerator.Generate()
		if err != nil {
			return err
		}
		greetReplyMessageId, err := fa.idGenerator.Generate()
		if err != nil {
			return err
		}

		messages = append(messages, []*messageentity.Message{{
			MessageId:      greetMessageId,
			ConversationId: convId,
			SenderId:       record.ApplicantUserId,
			Seq:            1,
			Type:           messagevo.Text,
			Content:        record.Message,
			SendTime:       record.ApplyTime,
			Status:         messagevo.Normal,
		}, {
			MessageId:      greetReplyMessageId,
			ConversationId: convId,
			SenderId:       record.PeerUserId,
			Seq:            2,
			Type:           messagevo.Text,
			Content:        "我们已经是好友了，开始聊天吧~",
			SendTime:       time.Now().UnixMilli(),
			Status:         messagevo.Normal,
		}}...)
	} else {
		// 请求方需要主动插入请求时附带的消息
		greetMessageId, err := fa.idGenerator.Generate()
		if err != nil {
			return err
		}

		messages = append(messages, &messageentity.Message{
			MessageId:      greetMessageId,
			ConversationId: convId,
			SenderId:       record.PeerUserId,
			Seq:            1,
			Type:           messagevo.Text,
			Content:        "我们已经是好友了，开始聊天吧~",
			SendTime:       time.Now().UnixMilli(),
			Status:         messagevo.Normal,
		})
	}

	conv.LatestMessageId = messages[len(messages)-1].MessageId
	conv.LatestSeq = int64(len(messages))
	toUserConv.LastReadSeq = int64(len(messages))

	fromUserConv.LastReadSeq = int64(len(messages))

	if err := fa.txManager.WithinTransaction(ctx, func(tx any) error {
		conversationRepo := fa.conversationRepository.WithTx(tx)
		if err := conversationRepo.LockPrivateParticipants(ctx, record.ApplicantUserId, record.PeerUserId); err != nil {
			return err
		}
		existing, err := conversationRepo.FindPrivateConversation(ctx, record.ApplicantUserId, record.PeerUserId)
		if err != nil {
			return err
		}
		if err := fa.friendRequestRepository.WithTx(tx).OperateRequest(record.RequestId, int(friendvo.Pending), int(record.Status)); err != nil {
			fmt.Println("处理好友申请失败：", err)
			return ErrOperationFailed
		}

		if err := fa.friendRepository.WithTx(tx).Create([]friendentity.Friend{
			{
				UserId:       record.ApplicantUserId,
				FriendUserId: record.PeerUserId,
				Status:       friendvo.Status(friendvo.Friend),
			},
			{
				UserId:       record.PeerUserId,
				FriendUserId: record.ApplicantUserId,
				Status:       friendvo.Status(friendvo.Friend),
			},
		}); err != nil {
			log.Println("创建好友关系失败：", err)
			return ErrOperationFailed
		}

		if existing == nil {
			if err := conversationRepo.CreateConversation(ctx, conv); err != nil {
				return ErrCreateConvFailed
			}
		} else {
			firstSeq, err := conversationRepo.ReserveSequenceRange(ctx, existing.ConversationId, len(messages), messages[len(messages)-1].MessageId)
			if err != nil {
				return err
			}
			conv = existing
			conv.LatestSeq = firstSeq + int64(len(messages)) - 1
			conv.LatestMessageId = messages[len(messages)-1].MessageId
			for i, message := range messages {
				message.ConversationId = conv.ConversationId
				message.Seq = firstSeq + int64(i)
			}
			toUserConv.ConversationId, fromUserConv.ConversationId = conv.ConversationId, conv.ConversationId
			toUserConv.LastReadSeq, fromUserConv.LastReadSeq = conv.LatestSeq, conv.LatestSeq
		}

		if err := fa.userConvRepository.WithTx(tx).CreateUserConversation(ctx, toUserConv); err != nil && !errors.Is(err, conversationentity.ErrDuplicateCreation) {
			return ErrCreateUserConvFailed
		}

		if err := fa.userConvRepository.WithTx(tx).CreateUserConversation(ctx, fromUserConv); err != nil && !errors.Is(err, conversationentity.ErrDuplicateCreation) {
			return ErrCreateUserConvFailed
		}

		// 初始消息必须和好友关系、会话处于同一事务，并进入 Outbox。
		messageRepo := fa.messageRepository.WithTx(tx)
		for _, message := range messages {
			if err := messageRepo.CreateNewMessage(ctx, message); err != nil {
				return err
			}
		}
		if err := fa.createInitialMessageOutboxes(ctx, tx, record, conv, messages); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return ErrOperationFailed
	}
	fa.notifyProducer()

	state := &friendcache.RelationState{Status: friendvo.Friend}
	fa.conversationApplication.CacheConversationInfo(ctx, conv)
	if err := fa.friendCache.SetRelation(ctx, record.ApplicantUserId, record.PeerUserId, state); err != nil {
		log.Println("预热好友关系缓存失败：", err)
	}
	if err := fa.friendCache.SetRelation(ctx, record.PeerUserId, record.ApplicantUserId, state); err != nil {
		log.Println("预热反向好友关系缓存失败：", err)
	}
	return nil
}

func (fa *FriendRequestApplication) createInitialMessageOutboxes(
	ctx context.Context,
	tx any,
	record *friendentity.FriendRequest,
	conversation *conversationentity.Conversation,
	messages []*messageentity.Message,
) error {
	if fa.outboxRepository == nil || record == nil || conversation == nil {
		return fmt.Errorf("好友消息出箱组件未配置")
	}
	repository := fa.outboxRepository.WithTx(tx)
	for _, message := range messages {
		if message == nil {
			continue
		}
		receiverID := record.ApplicantUserId
		if message.SenderId == record.ApplicantUserId {
			receiverID = record.PeerUserId
		}
		eventPayload, err := json.Marshal(protocol.MessageEvent{
			MessageId:      message.MessageId,
			ConversationId: conversation.ConversationId,
			SenderId:       message.SenderId,
			RecvId:         receiverID,
			Seq:            message.Seq,
			ConvType:       protocol.PrivateChat,
			CType:          int(message.Type),
			Content:        message.Content,
			SendTime:       message.SendTime,
		})
		if err != nil {
			return err
		}
		if err := repository.Create(ctx, &outboxport.Entry{
			EventType:    protocol.EventTypeSendMessage,
			PartitionKey: conversation.ConversationId,
			Payload:      eventPayload,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (fa *FriendRequestApplication) ListFriendRequestsByUserID(userId string) ([]FriendRequestDTO, error) {
	records, err := fa.friendRequestRepository.ListByUserID(userId)
	if err != nil {
		return nil, err
	}

	friendRequestDTOs := make([]FriendRequestDTO, 0, len(records))
	for _, r := range records {
		dto := toFriendRequestDTO(r)
		fa.fillApplicantNickName(&dto)
		friendRequestDTOs = append(friendRequestDTOs, dto)
	}

	return friendRequestDTOs, nil
}

// fillApplicantNickName 返回申请人的有效展示昵称；历史数据为空时使用手机号兜底。
func (fa *FriendRequestApplication) fillApplicantNickName(dto *FriendRequestDTO) {
	if dto == nil || dto.ApplicantUserID == "" {
		return
	}

	user, err := fa.userRepository.FindByUserID(dto.ApplicantUserID)
	if err != nil || user == nil {
		return
	}

	dto.ApplicantNickName = user.NickName
	if dto.ApplicantNickName == "" {
		dto.ApplicantNickName = string(user.Phone)
	}
}
