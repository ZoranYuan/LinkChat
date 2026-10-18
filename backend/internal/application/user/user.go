package user

import (
	idport "IM_backend/internal/application/ports/id"
	authport "IM_backend/internal/application/ports/persistence/cache/auth"
	usercache "IM_backend/internal/application/ports/persistence/cache/user"
	userrepo "IM_backend/internal/application/ports/persistence/repository/user"
	txmanager "IM_backend/internal/application/ports/persistence/tx_manager"
	securityport "IM_backend/internal/application/ports/security"
	userentity "IM_backend/internal/domain/user/entity"
	uservo "IM_backend/internal/domain/user/value_object"
	"context"
	"errors"
	"log"
	"strings"
	"time"
)

type UserApplication struct {
	userRepository userrepo.UserRepository
	tokenIssuer    securityport.TokenIssuer
	options        Options
	authCache      authport.AuthCache
	userCache      usercache.UserCache

	idGenerator    idport.Generator
	passwordHasher securityport.PasswordHasher
	txManager      txmanager.TxManager
}

type Options struct {
	AccessTokenTTL  time.Duration
	RefreshTokenTTL time.Duration
}

func NewUserApplication(userRepository userrepo.UserRepository, options Options, userCache usercache.UserCache, authCache authport.AuthCache, tokenIssuer securityport.TokenIssuer, idGenerator idport.Generator, passwordHasher securityport.PasswordHasher, txManager txmanager.TxManager) *UserApplication {
	return &UserApplication{
		userRepository: userRepository,
		options:        options,
		authCache:      authCache,
		userCache:      userCache,
		tokenIssuer:    tokenIssuer,
		idGenerator:    idGenerator,
		passwordHasher: passwordHasher,
		txManager:      txManager,
	}
}

func (ua *UserApplication) issueTokensAndCache(userId string) (string, string, error) {
	accessToken, refreshToken, sessionID, err := ua.tokenIssuer.IssueToken(userId)
	if err != nil {
		return "", "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := ua.authCache.CreateSession(ctx, refreshToken, authport.RefreshSession{
		UserID:    userId,
		SessionID: sessionID,
	}, ua.options.RefreshTokenTTL); err != nil {
		return "", "", err
	}

	return accessToken, refreshToken, nil
}

func (ua *UserApplication) RegisterWithPhone(password string, phone string, reconfirmPassword string) (*UserAppDTO, error) {
	if !uservo.Phone(phone).Validate() {
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

	newUser, err := userentity.RegisterWithPhone(
		uservo.Phone(phone),
		uservo.Password(hashedPassword),
	)

	if err != nil {
		return nil, err
	}

	// 生成 UserId
	userId, err := ua.idGenerator.Generate()
	if err != nil {
		return nil, err
	}

	newUser.UserId = userId
	err = ua.userRepository.Create(newUser)

	if err != nil {
		return nil, err
	}

	userApp := &UserAppDTO{
		UserID:    newUser.UserId,
		Username:  newUser.UserName,
		Nickname:  newUser.NickName,
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

	user, err := ua.userRepository.FindByUsernameOrPhone(account)
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
	user.OnLineTime = time.Now()
	if err := ua.userRepository.UpdateOnlineTime(user.UserId, string(user.Phone), user.OnLineTime); err != nil {
		return nil, err
	}

	accessToken, refreshToken, err := ua.issueTokensAndCache(user.UserId)
	if err != nil {
		return nil, err
	}

	return &UserAppDTO{
		UserID:       user.UserId,
		Username:     user.UserName,
		Nickname:     user.NickName,
		AvatarURL:    user.Avatar,
		Phone:        string(user.Phone),
		RefreshToken: refreshToken,
		AccessToken:  accessToken,
	}, nil
}

func (ua *UserApplication) UpdateUserProfile(ctx context.Context, userId string, avatar, nickName, userName *string) (*UserAppDTO, error) {
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

	var user *userentity.User
	err := ua.txManager.WithinTransaction(ctx, func(tx any) error {
		var err error
		user, err = ua.userRepository.WithTx(tx).UpdateUserProfile(ctx, userId, updates)
		return err
	})
	if err != nil {
		return nil, err
	}

	if err := ua.userCache.DeleteUserProfiles(ctx, []string{userId}); err != nil {
		log.Printf("删除用户缓存失败 %s", err.Error())
	}

	return &UserAppDTO{
		UserID:    user.UserId,
		Username:  user.UserName,
		Nickname:  user.NickName,
		Phone:     string(user.Phone),
		AvatarURL: user.Avatar,
	}, nil
}

func (ua *UserApplication) FindUserByPhoneAndUserName(keyword string) (*UserAppDTO, error) {
	user, err := ua.userRepository.FindByUsernameOrPhone(keyword)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, ErrUserNotFound
	}

	return &UserAppDTO{
		UserID:    user.UserId,
		Username:  user.UserName,
		Nickname:  user.NickName,
		AvatarURL: user.Avatar,
	}, nil
}

func (ua *UserApplication) Logout(userId, sessionID string) error {
	user, err := ua.userRepository.FindByUserID(userId)
	if err != nil {
		return err
	}

	if user == nil {
		return ErrUserNotFound
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := ua.authCache.DeleteSession(ctx, userId, sessionID); err != nil {
		return err
	}
	return ua.userRepository.UpdateOfflineTime(user.UserId, string(user.Phone), time.Now())
}

func (ua *UserApplication) Refresh(refreshToken string) (*UserAppDTO, error) {
	if refreshToken == "" {
		return nil, ErrUserNotFound
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	session, found, err := ua.authCache.GetRefreshSession(ctx, refreshToken)
	if err != nil {
		return nil, err
	}
	if !found || session.UserID == "" {
		return nil, ErrUserNotFound
	}
	user, err := ua.userRepository.FindByUserID(session.UserID)
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
	}, ua.options.RefreshTokenTTL)
	if err != nil {
		return nil, err
	}
	if !rotated {
		return nil, ErrUserNotFound
	}
	return &UserAppDTO{UserID: user.UserId, Username: user.UserName, Nickname: user.NickName, Phone: string(user.Phone), AvatarURL: user.Avatar, AccessToken: access, RefreshToken: refresh}, nil
}
