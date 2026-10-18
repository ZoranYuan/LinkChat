package user

import (
	userapp "IM_backend/internal/application/user"
	uservo "IM_backend/internal/domain/user/value_object"
	"IM_backend/internal/transport/http/response"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type UserHandle struct {
	app                 *userapp.UserApplication
	sessionCloser       AuthSessionCloser
	accessCookieMaxAge  int
	refreshCookieMaxAge int
}

type AuthSessionCloser interface {
	CloseAuthSession(userID, sessionID string) int
}

const (
	accessTokenCookieName  = "access_token"
	refreshTokenCookieName = "refresh_token"
	accessTokenCookiePath  = "/"
	refreshTokenCookiePath = "/api/v1/users"
)

func NewUserHandle(app *userapp.UserApplication, accessTTL, refreshTTL time.Duration, sessionCloser AuthSessionCloser) *UserHandle {
	return &UserHandle{
		app:                 app,
		sessionCloser:       sessionCloser,
		accessCookieMaxAge:  int(accessTTL.Seconds()),
		refreshCookieMaxAge: int(refreshTTL.Seconds()),
	}
}

func (uh *UserHandle) Login(c *gin.Context) {
	var req = UserLoginRequest{}

	defer func() {
		if r := recover(); r != nil {
			log.Println("用户接口发生异常：", r)
			c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "未知错误"))
		}
	}()

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "请求参数错误"))
		return
	}

	userApp, err := uh.app.Login(req.Account, req.Password)

	if err != nil {
		switch {
		case errors.Is(err, userapp.ErrUserNotFound):
			c.JSON(http.StatusNotFound, response.Error(http.StatusNotFound, err.Error()))
		case errors.Is(err, userapp.ErrIncorrectPassword):
			c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, err.Error()))
		case errors.Is(err, userapp.ErrInvalidLoginAccount), errors.Is(err, userapp.ErrInvalidPhoneNumber), errors.Is(err, userapp.ErrPasswordMismatch):
			c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, err.Error()))
		default:
			c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "登录失败"))
		}
		return
	}

	var res = UserProfileResponse{
		UserID:    userApp.UserID,
		Username:  userApp.Username,
		Nickname:  userApp.Nickname,
		Phone:     userApp.Phone,
		AvatarURL: userApp.AvatarURL,
	}

	c.SetSameSite(http.SameSiteLaxMode)
	uh.setTokenCookies(c, userApp)
	c.JSON(http.StatusOK, response.Success(res))
}

func (uh *UserHandle) Logout(c *gin.Context) {
	userId := c.GetString("userId")

	if userId == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "请登录之后再操作"))
		return
	}

	sessionID := c.GetString("authSessionID")
	if err := uh.app.Logout(userId, sessionID); err != nil {
		log.Println("退出登录失败：", err)
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "退出登录失败"))
		return
	}
	if uh.sessionCloser != nil {
		uh.sessionCloser.CloseAuthSession(userId, sessionID)
	}

	c.SetSameSite(http.SameSiteLaxMode)
	uh.clearTokenCookies(c)
	c.JSON(http.StatusOK, response.Success(nil))

}

func (uh *UserHandle) Refresh(c *gin.Context) {
	refreshToken, err := c.Cookie(refreshTokenCookieName)
	if err != nil || refreshToken == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "刷新令牌无效"))
		return
	}
	userApp, err := uh.app.Refresh(refreshToken)
	if err != nil {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "刷新令牌无效"))
		return
	}
	c.SetSameSite(http.SameSiteLaxMode)
	uh.setTokenCookies(c, userApp)
	c.JSON(http.StatusOK, response.Success(UserProfileResponse{UserID: userApp.UserID, Username: userApp.Username, Nickname: userApp.Nickname, Phone: userApp.Phone, AvatarURL: userApp.AvatarURL}))
}

func (uh *UserHandle) Register(c *gin.Context) {
	var req = UserRegisterRequest{}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "请求参数错误"))
		return
	}

	var (
		userApp *userapp.UserAppDTO
		err     error
	)
	switch req.LoginType {
	case int(uservo.PhoneType):
		// 验证密码，手机号字段
		if req.Phone == "" || req.Password == "" || req.ReconfirmPassword == "" {
			c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
			return
		}
		userApp, err = uh.app.RegisterWithPhone(req.Password, req.Phone, req.ReconfirmPassword)
	case int(uservo.WxType):
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "暂不支持该登录方式"))
		return
	default:
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}

	if err != nil {
		switch {
		case errors.Is(err, userapp.ErrUserAlreadyExists):
			c.JSON(http.StatusConflict, response.Error(http.StatusConflict, err.Error()))
		case errors.Is(err, userapp.ErrInvalidPhoneNumber), errors.Is(err, userapp.ErrPasswordMismatch):
			c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, err.Error()))
		default:
			c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "注册失败"))
		}
		return
	}

	var res = UserProfileResponse{
		UserID:    userApp.UserID,
		Username:  userApp.Username,
		Nickname:  userApp.Nickname,
		Phone:     userApp.Phone,
		AvatarURL: userApp.AvatarURL,
	}

	c.JSON(http.StatusOK, response.Success(res))
}

func (uh *UserHandle) cookieSecure(c *gin.Context) bool {
	return c.Request.TLS != nil || strings.EqualFold(c.GetHeader("X-Forwarded-Proto"), "https")
}

func (uh *UserHandle) setTokenCookies(c *gin.Context, userApp *userapp.UserAppDTO) {
	secure := uh.cookieSecure(c)
	c.SetCookie(accessTokenCookieName, userApp.AccessToken, uh.accessCookieMaxAge, accessTokenCookiePath, "", secure, true)
	c.SetCookie(refreshTokenCookieName, userApp.RefreshToken, uh.refreshCookieMaxAge, refreshTokenCookiePath, "", secure, true)
}

func (uh *UserHandle) clearTokenCookies(c *gin.Context) {
	secure := uh.cookieSecure(c)
	c.SetCookie(accessTokenCookieName, "", -1, accessTokenCookiePath, "", secure, true)
	c.SetCookie(refreshTokenCookieName, "", -1, refreshTokenCookiePath, "", secure, true)
}

func (uh *UserHandle) FindUserByPhoneAndUserName(c *gin.Context) {
	keyword := strings.TrimSpace(c.Query("keyword"))
	if keyword == "" {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "手机号或用户名不能为空"))
		return
	}

	userApp, err := uh.app.FindUserByPhoneAndUserName(keyword)
	if err != nil {
		log.Println("根据手机号或用户名查询用户失败：", err)
		c.JSON(http.StatusNotFound, response.Error(http.StatusNotFound, "用户不存在"))
		return
	}

	c.JSON(http.StatusOK, response.Success(UserProfileResponse{
		UserID:    userApp.UserID,
		Username:  userApp.Username,
		Nickname:  userApp.Nickname,
		AvatarURL: userApp.AvatarURL,
	}))
}

func (uh *UserHandle) UpdateUserProfile(c *gin.Context) {
	userId := c.GetString("userId")
	if userId == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "请先登录"))
		return
	}

	var updateUserProfileReq UpdateUserProfileRequest
	if err := c.ShouldBindJSON(&updateUserProfileReq); err != nil {
		c.JSON(http.StatusBadRequest, response.Error(201, "参数错误"))
		return
	}
	updatedUser, err := uh.app.UpdateUserProfile(c.Request.Context(), userId, updateUserProfileReq.AvatarURL, updateUserProfileReq.Nickname, updateUserProfileReq.Username)
	if err != nil {
		switch {
		case errors.Is(err, userapp.ErrNoProfileFields):
			c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, err.Error()))
		case errors.Is(err, userapp.ErrUserNotFound):
			c.JSON(http.StatusNotFound, response.Error(http.StatusNotFound, err.Error()))
		default:
			log.Println("修改用户资料失败：", err)
			c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "修改用户资料失败"))
		}
		return
	}

	c.JSON(http.StatusOK, response.Success(UserProfileResponse{
		UserID:    updatedUser.UserID,
		Username:  updatedUser.Username,
		Nickname:  updatedUser.Nickname,
		AvatarURL: updatedUser.AvatarURL,
	}))
}
