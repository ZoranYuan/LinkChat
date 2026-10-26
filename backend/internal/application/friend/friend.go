package friend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"
	"unicode/utf8"

	"IM_backend/configs"
	conversationapp "IM_backend/internal/application/conversation"
	idport "IM_backend/internal/application/ports/id"
	"IM_backend/internal/application/ports/messaging"
	outboxport "IM_backend/internal/application/ports/outbox"
	"IM_backend/internal/application/ports/transaction"
	domainconversation "IM_backend/internal/domain/conversation"
	domainfriend "IM_backend/internal/domain/friend"
	domainmessage "IM_backend/internal/domain/message"
	domainuser "IM_backend/internal/domain/user"
)

type FriendApplication struct {
	friendRepository domainfriend.FriendRepository
	userRepository   domainuser.UserRepository
}

func NewFriendApplication(
	friendRepository domainfriend.FriendRepository,
	userRepository domainuser.UserRepository,
) *FriendApplication {
	return &FriendApplication{
		friendRepository: friendRepository,
		userRepository:   userRepository,
	}
}

func (fa *FriendApplication) GetUserFriendList(userID string) ([]FriendAppDTO, error) {
	// TODO 权衡这里是否有必要加入 userId 的查询，判断当前用户是否存在
	friends, err := fa.friendRepository.GetUserFriendList(
		userID,
		int(domainfriend.StatusFriend),
	)
	if err != nil {
		return nil, err
	}

	userIDs := make([]string, 0, len(friends))

	for _, f := range friends {
		userIDs = append(userIDs, f.FriendUserID)
	}

	users, err := fa.userRepository.ListUsersByUserIDs(userIDs)

	if err != nil {
		return nil, err
	}

	userByID := make(map[string]struct {
		avatar   string
		userName string
		nickName string
	}, len(users))
	for _, user := range users {
		userByID[user.UserID] = struct {
			avatar   string
			userName string
			nickName string
		}{
			avatar:   user.Avatar,
			userName: user.Username,
			nickName: user.Nickname,
		}
	}

	friendListApp := make([]FriendAppDTO, 0, len(friends))
	for _, friend := range friends {
		user, ok := userByID[friend.FriendUserID]
		if !ok {
			continue
		}
		friendListApp = append(friendListApp, FriendAppDTO{
			FriendUserID:    friend.FriendUserID,
			FriendAvatarURL: user.avatar,
			FriendUsername:  user.userName,
			FriendNickname:  user.nickName,
			Status:          int(friend.Status),
			Remarks:         friend.Remarks,
		})
	}

	return friendListApp, nil
}

type FriendRequestApplication struct {
	config                  configs.FriendConfig
	friendRequestRepository domainfriend.FriendRequestRepository
	userRepository          domainuser.UserRepository
	messageRepository       domainmessage.MessageRepository
	friendRepository        domainfriend.FriendRepository
	conversationRepository  domainconversation.ConversationRepository
	conversationApplication *conversationapp.ConversationApplication
	userConvRepository      domainconversation.UserConversationRepository
	txManager               transaction.Manager
	idGenerator             idport.Generator
	outboxRepository        outboxport.OutboxRepository
	producerNotifier        messaging.ProducerNotifier
}

func NewFriendRequestApplication(
	friendRequestRepository domainfriend.FriendRequestRepository,
	userRepository domainuser.UserRepository,
	messageRepository domainmessage.MessageRepository,
	conversationRepository domainconversation.ConversationRepository,
	userConvRepository domainconversation.UserConversationRepository,
	friendRepository domainfriend.FriendRepository,
	txManager transaction.Manager,
	idGenerator idport.Generator,
	outboxRepository outboxport.OutboxRepository,
	config configs.FriendConfig,
) *FriendRequestApplication {
	return &FriendRequestApplication{
		config:                  config,
		friendRequestRepository: friendRequestRepository,
		userRepository:          userRepository,
		messageRepository:       messageRepository,
		userConvRepository:      userConvRepository,
		conversationRepository:  conversationRepository,
		friendRepository:        friendRepository,
		txManager:               txManager,
		idGenerator:             idGenerator,
		outboxRepository:        outboxRepository,
	}
}

// SetProducerNotifier 在事务提交后唤醒 Kafka Producer。
func (fa *FriendRequestApplication) SetProducerNotifier(notifier messaging.ProducerNotifier) {
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
	repository domainfriend.FriendRequestRepository,
	outboxRepository outboxport.OutboxRepository,
) (*FriendRequestDTO, error) {
	requestID, err := fa.idGenerator.Generate()
	if err != nil {
		return nil, fmt.Errorf("生成好友申请 ID 失败：%w", err)
	}

	request, err := domainfriend.NewFriendRequest(
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
	record *domainfriend.FriendRequest,
	repository outboxport.OutboxRepository,
) error {
	if repository == nil || record == nil {
		return nil
	}

	payload, err := json.Marshal(messaging.FriendRequestCreatedEvent{
		RequestID:       record.RequestID,
		ApplicantUserID: record.ApplicantUserID,
		PeerUserID:      record.PeerUserID,
		Message:         record.Message,
		ApplyTime:       record.ApplyTime,
	})
	if err != nil {
		return err
	}

	return repository.Create(ctx, &outboxport.Entry{
		EventType:    messaging.TypeFriendRequestCreated,
		PartitionKey: record.PeerUserID,
		Payload:      payload,
	})
}

func (fa *FriendRequestApplication) reRequest(
	ctx context.Context,
	record *domainfriend.FriendRequest,
	message string,
	repository domainfriend.FriendRequestRepository,
	outboxRepository outboxport.OutboxRepository,
) (*FriendRequestDTO, error) {
	requestID, err := fa.idGenerator.Generate()
	if err != nil {
		return nil, fmt.Errorf("生成好友申请 ID 失败：%w", err)
	}

	if err := record.ReRequest(requestID, message, time.Duration(fa.config.RequestCooldownSeconds)*time.Second); err != nil {
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
		return ErrEmptyUserID
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

	relation, err := fa.friendRepository.FindFriendByUserIDAndFriendUserID(userID, peerUserID)
	if err != nil {
		return nil, fmt.Errorf("查询好友关系失败：%w", err)
	}
	if relation != nil {
		return nil, ErrAlreadyFriends
	}

	user, err := fa.userRepository.FindUserByUserID(peerUserID)
	if err != nil {
		return nil, fmt.Errorf("查询目标用户失败：%w", err)
	}
	if user == nil {
		return nil, ErrUserNotFound
	}

	reverse, err := fa.friendRequestRepository.FindLatestFriendRequestByApplicantUserIDAndPeerUserID(peerUserID, userID)
	if err != nil {
		return nil, fmt.Errorf("查询反向好友申请失败：%w", err)
	}
	if reverse != nil && reverse.Status == domainfriend.Pending {
		if err := reverse.Accept(userID); err != nil {
			if errors.Is(err, domainfriend.ErrDuplicateRequestOperation) {
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

	record, err := fa.friendRequestRepository.FindLatestFriendRequestByApplicantUserIDAndPeerUserID(
		userID,
		peerUserID,
	)
	if err != nil {
		return nil, fmt.Errorf("查询最新好友申请失败：%w", err)
	}

	if record == nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(fa.config.RequestTimeoutSeconds)*time.Second)
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(fa.config.RequestTimeoutSeconds)*time.Second)
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

func (fa *FriendRequestApplication) Refuse(requestID string, userID string) error {
	record, err := fa.friendRequestRepository.FindFriendRequestByRequestID(requestID)

	if err != nil {
		return err
	}

	if err := record.Refuse(userID); err != nil {
		return err
	}

	if err := fa.friendRequestRepository.OperateRequest(record.RequestID, int(domainfriend.Pending), int(record.Status)); err != nil {
		return err
	}

	return nil
}

func (fa *FriendRequestApplication) Accept(requestID string, userID string) error {
	record, err := fa.friendRequestRepository.FindFriendRequestByRequestID(requestID)

	if err != nil {
		return err
	}

	if err := record.Accept(userID); err != nil {
		if errors.Is(err, domainfriend.ErrDuplicateRequestOperation) {
			return ErrDuplicateOperation
		} else {
			return ErrUnknown
		}
	}

	return fa.acceptFriendRequest(record, userID)
}

func (fa *FriendRequestApplication) acceptFriendRequest(
	record *domainfriend.FriendRequest,
	userID string,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(fa.config.RequestTimeoutSeconds)*time.Second)
	defer cancel()

	// 初始化元信息
	convID, err := fa.idGenerator.Generate()
	if err != nil {
		return err
	}

	// 创建会话
	conv := domainconversation.NewPrivateConversation(
		convID,
		record.ApplicantUserID,
		record.PeerUserID,
	)

	toUserConv := domainconversation.BuildUserConversation(
		userID,
		convID,
		0,
	)

	fromUserConv := domainconversation.BuildUserConversation(
		record.ApplicantUserID,
		convID,
		0,
	)

	messages := []*domainmessage.Message{}

	// 初始化数据
	if record.Message != "" {
		greetMessageID, err := fa.idGenerator.Generate()
		if err != nil {
			return err
		}
		greetReplyMessageID, err := fa.idGenerator.Generate()
		if err != nil {
			return err
		}

		messages = append(messages, []*domainmessage.Message{{
			MessageID:      greetMessageID,
			ConversationID: convID,
			SenderID:       record.ApplicantUserID,
			Seq:            1,
			Type:           domainmessage.Text,
			Content:        record.Message,
			SendTime:       record.ApplyTime,
			Status:         domainmessage.Normal,
		}, {
			MessageID:      greetReplyMessageID,
			ConversationID: convID,
			SenderID:       record.PeerUserID,
			Seq:            2,
			Type:           domainmessage.Text,
			Content:        "我们已经是好友了，开始聊天吧~",
			SendTime:       time.Now().UnixMilli(),
			Status:         domainmessage.Normal,
		}}...)
	} else {
		// 请求方需要主动插入请求时附带的消息
		greetMessageID, err := fa.idGenerator.Generate()
		if err != nil {
			return err
		}

		messages = append(messages, &domainmessage.Message{
			MessageID:      greetMessageID,
			ConversationID: convID,
			SenderID:       record.PeerUserID,
			Seq:            1,
			Type:           domainmessage.Text,
			Content:        "我们已经是好友了，开始聊天吧~",
			SendTime:       time.Now().UnixMilli(),
			Status:         domainmessage.Normal,
		})
	}

	conv.LatestMessageID = messages[len(messages)-1].MessageID
	conv.LatestSeq = int64(len(messages))
	toUserConv.LastReadSeq = int64(len(messages))

	fromUserConv.LastReadSeq = int64(len(messages))

	if err := fa.txManager.WithinTransaction(ctx, func(tx any) error {
		conversationRepo := fa.conversationRepository.WithTx(tx)
		if err := conversationRepo.LockPrivateParticipants(ctx, record.ApplicantUserID, record.PeerUserID); err != nil {
			return err
		}
		existing, err := conversationRepo.FindConversationByUserIDs(ctx, record.ApplicantUserID, record.PeerUserID)
		if err != nil {
			return err
		}
		if err := fa.friendRequestRepository.WithTx(tx).OperateRequest(record.RequestID, int(domainfriend.Pending), int(record.Status)); err != nil {
			fmt.Println("处理好友申请失败：", err)
			return ErrOperationFailed
		}

		if err := fa.friendRepository.WithTx(tx).Create([]domainfriend.Friend{
			{
				UserID:       record.ApplicantUserID,
				FriendUserID: record.PeerUserID,
				Status:       domainfriend.Status(domainfriend.StatusFriend),
			},
			{
				UserID:       record.PeerUserID,
				FriendUserID: record.ApplicantUserID,
				Status:       domainfriend.Status(domainfriend.StatusFriend),
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
			firstSeq, err := conversationRepo.ReserveSequenceRange(ctx, existing.ConversationID, len(messages), messages[len(messages)-1].MessageID)
			if err != nil {
				return err
			}
			conv = existing
			conv.LatestSeq = firstSeq + int64(len(messages)) - 1
			conv.LatestMessageID = messages[len(messages)-1].MessageID
			for i, message := range messages {
				message.ConversationID = conv.ConversationID
				message.Seq = firstSeq + int64(i)
			}
			toUserConv.ConversationID, fromUserConv.ConversationID = conv.ConversationID, conv.ConversationID
			toUserConv.LastReadSeq, fromUserConv.LastReadSeq = conv.LatestSeq, conv.LatestSeq
		}

		if err := fa.userConvRepository.WithTx(tx).CreateUserConversation(ctx, toUserConv); err != nil && !errors.Is(err, domainconversation.ErrDuplicateCreation) {
			return ErrCreateUserConvFailed
		}

		if err := fa.userConvRepository.WithTx(tx).CreateUserConversation(ctx, fromUserConv); err != nil && !errors.Is(err, domainconversation.ErrDuplicateCreation) {
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

	fa.conversationApplication.CacheConversationInfo(ctx, conv)
	return nil
}

func (fa *FriendRequestApplication) createInitialMessageOutboxes(
	ctx context.Context,
	tx any,
	record *domainfriend.FriendRequest,
	conversation *domainconversation.Conversation,
	messages []*domainmessage.Message,
) error {
	if fa.outboxRepository == nil || record == nil || conversation == nil {
		return fmt.Errorf("好友消息出箱组件未配置")
	}
	repository := fa.outboxRepository.WithTx(tx)
	for _, message := range messages {
		if message == nil {
			continue
		}
		receiverID := record.ApplicantUserID
		if message.SenderID == record.ApplicantUserID {
			receiverID = record.PeerUserID
		}
		eventPayload, err := json.Marshal(messaging.MessageEvent{
			MessageID:      message.MessageID,
			ConversationID: conversation.ConversationID,
			SenderID:       message.SenderID,
			RecvID:         receiverID,
			Seq:            message.Seq,
			ConvType:       messaging.PrivateChat,
			MessageType:    int(message.Type),
			Content:        message.Content,
			SendTime:       message.SendTime,
		})
		if err != nil {
			return err
		}
		if err := repository.Create(ctx, &outboxport.Entry{
			EventType:    messaging.TypeMessage,
			PartitionKey: conversation.ConversationID,
			Payload:      eventPayload,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (fa *FriendRequestApplication) ListFriendRequestsByUserID(userID string) ([]FriendRequestDTO, error) {
	records, err := fa.friendRequestRepository.ListFriendRequestsByPeerUserID(userID)
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

	user, err := fa.userRepository.FindUserByUserID(dto.ApplicantUserID)
	if err != nil || user == nil {
		return
	}

	dto.ApplicantNickName = user.Nickname
	if dto.ApplicantNickName == "" {
		dto.ApplicantNickName = string(user.Phone)
	}
}
