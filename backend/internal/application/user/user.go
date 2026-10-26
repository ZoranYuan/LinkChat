package user

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"IM_backend/configs"
	authport "IM_backend/internal/application/ports/cache/auth"
	idport "IM_backend/internal/application/ports/id"
	securityport "IM_backend/internal/application/ports/security"
	"IM_backend/internal/application/ports/transaction"
	domainuser "IM_backend/internal/domain/user"
)

type UserApplication struct {
	userRepository domainuser.UserRepository
	tokenIssuer    securityport.TokenIssuer
	config         configs.JWT
	authCache      authport.AuthCache

	idGenerator    idport.Generator
	passwordHasher securityport.PasswordHasher
	txManager      transaction.Manager
}

func NewUserApplication(userRepository domainuser.UserRepository, config configs.JWT, authCache authport.AuthCache, tokenIssuer securityport.TokenIssuer, idGenerator idport.Generator, passwordHasher securityport.PasswordHasher, txManager transaction.Manager) *UserApplication {
	return &UserApplication{
		userRepository: userRepository,
		config:         config,
		authCache:      authCache,
		tokenIssuer:    tokenIssuer,
		idGenerator:    idGenerator,
		passwordHasher: passwordHasher,
		txManager:      txManager,
	}
}

func (ua *UserApplication) issueTokensAndCache(userID string) (string, string, error) {
	accessToken, refreshToken, sessionID, err := ua.tokenIssuer.IssueToken(userID)
	if err != nil {
		return "", "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ua.config.SessionOperationTimeoutSeconds)*time.Second)
	defer cancel()

	if err := ua.authCache.CreateSession(ctx, refreshToken, authport.RefreshSession{
		UserID:    userID,
		SessionID: sessionID,
	}, time.Duration(ua.config.RefreshExpireHours)*time.Hour); err != nil {
		return "", "", err
	}

	return accessToken, refreshToken, nil
}

func (ua *UserApplication) RegisterWithPhone(password string, phone string, reconfirmPassword string) (*UserAppDTO, error) {
	if !domainuser.Phone(phone).Validate() {
		return nil, ErrInvalidPhoneNumber
	}

	user, err := ua.userRepository.FindUserByPhone(phone)
	if err != nil {
		log.Println("注册用户失败：", err)
		return nil, err
	}
	if user != nil {
		return nil, ErrUserAlreadyExists
	}

	if password != reconfirmPassword {
		return nil, ErrPasswordMismatch
	}

	hashedPassword, err := ua.passwordHasher.Hash(password)
	if err != nil {
		return nil, err
	}

	newUser, err := domainuser.RegisterWithPhone(
		domainuser.Phone(phone),
		domainuser.Password(hashedPassword),
	)

	if err != nil {
		return nil, err
	}

	// 生成 UserId
	userID, err := ua.idGenerator.Generate()
	if err != nil {
		return nil, err
	}

	newUser.UserID = userID
	err = ua.userRepository.Create(newUser)

	if err != nil {
		return nil, err
	}

	userApp := &UserAppDTO{
		UserID:    newUser.UserID,
		Username:  newUser.Username,
		Nickname:  newUser.Nickname,
		AvatarURL: newUser.Avatar,
		Phone:     string(newUser.Phone),
	}

	return userApp, nil
}

func (ua *UserApplication) Login(account, password string) (*UserAppDTO, error) {
	account = strings.TrimSpace(account)
	if account == "" {
		return nil, ErrInvalidLoginAccount
	}

	user, err := ua.userRepository.FindUserByUsernameOrPhone(account)
	if err != nil {
		log.Println("账号登录失败：", err)
		return nil, err
	}
	if user == nil {
		return nil, ErrUserNotFound
	}

	if !ua.passwordHasher.Verify(password, string(user.Password)) {
		return nil, ErrIncorrectPassword
	}

	// 更新当前用户上线时间
	user.OnlineTime = time.Now()
	if err := ua.userRepository.UpdateOnlineTime(user.UserID, string(user.Phone), user.OnlineTime); err != nil {
		return nil, err
	}

	accessToken, refreshToken, err := ua.issueTokensAndCache(user.UserID)
	if err != nil {
		return nil, err
	}

	return &UserAppDTO{
		UserID:       user.UserID,
		Username:     user.Username,
		Nickname:     user.Nickname,
		AvatarURL:    user.Avatar,
		Phone:        string(user.Phone),
		RefreshToken: refreshToken,
		AccessToken:  accessToken,
	}, nil
}

func (ua *UserApplication) UpdateUserProfile(ctx context.Context, userID string, avatar, nickName, userName *string) (*UserAppDTO, error) {
	updates := make(map[string]any)

	if avatar != nil {
		updates["avatar"] = strings.TrimSpace(*avatar)
	}
	if nickName != nil {
		updates["nick_name"] = strings.TrimSpace(*nickName)
	}
	if userName != nil {
		updates["user_name"] = strings.TrimSpace(*userName)
	}

	if len(updates) == 0 {
		return nil, ErrNoProfileFields
	}

	if ua.txManager == nil {
		return nil, errors.New("用户资料更新事务管理器未配置")
	}

	var user *domainuser.User
	err := ua.txManager.WithinTransaction(ctx, func(tx any) error {
		var err error
		user, err = ua.userRepository.WithTx(tx).UpdateUserProfile(ctx, userID, updates)
		return err
	})
	if err != nil {
		return nil, err
	}

	return &UserAppDTO{
		UserID:    user.UserID,
		Username:  user.Username,
		Nickname:  user.Nickname,
		Phone:     string(user.Phone),
		AvatarURL: user.Avatar,
	}, nil
}

func (ua *UserApplication) FindUserByPhoneOrUsername(keyword string) (*UserAppDTO, error) {
	user, err := ua.userRepository.FindUserByUsernameOrPhone(keyword)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, ErrUserNotFound
	}

	return &UserAppDTO{
		UserID:    user.UserID,
		Username:  user.Username,
		Nickname:  user.Nickname,
		AvatarURL: user.Avatar,
	}, nil
}

func (ua *UserApplication) Logout(userID, sessionID string) error {
	user, err := ua.userRepository.FindUserByUserID(userID)
	if err != nil {
		return err
	}

	if user == nil {
		return ErrUserNotFound
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ua.config.SessionOperationTimeoutSeconds)*time.Second)
	defer cancel()
	if err := ua.authCache.DeleteSession(ctx, userID, sessionID); err != nil {
		return err
	}
	return ua.userRepository.UpdateOfflineTime(user.UserID, string(user.Phone), time.Now())
}

func (ua *UserApplication) Refresh(refreshToken string) (*UserAppDTO, error) {
	if refreshToken == "" {
		return nil, ErrUserNotFound
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ua.config.SessionOperationTimeoutSeconds)*time.Second)
	defer cancel()

	session, found, err := ua.authCache.GetRefreshSession(ctx, refreshToken)
	if err != nil {
		return nil, err
	}
	if !found || session.UserID == "" {
		return nil, ErrUserNotFound
	}
	user, err := ua.userRepository.FindUserByUserID(session.UserID)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, ErrUserNotFound
	}
	access, refresh, err := ua.tokenIssuer.IssueTokensForSession(session.UserID, session.SessionID)
	if err != nil {
		return nil, err
	}
	rotated, err := ua.authCache.RotateRefreshSession(ctx, refreshToken, refresh, authport.RefreshSession{
		UserID:    session.UserID,
		SessionID: session.SessionID,
	}, time.Duration(ua.config.RefreshExpireHours)*time.Hour)
	if err != nil {
		return nil, err
	}
	if !rotated {
		return nil, ErrUserNotFound
	}
	return &UserAppDTO{UserID: user.UserID, Username: user.Username, Nickname: user.Nickname, Phone: string(user.Phone), AvatarURL: user.Avatar, AccessToken: access, RefreshToken: refresh}, nil
}
