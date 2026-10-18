package middleware

import (
	"IM_backend/configs"
	authport "IM_backend/internal/application/ports/persistence/cache/auth"
	"IM_backend/internal/infrastructure/security/jwt"
	"IM_backend/internal/transport/http/response"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
)

type AuthMiddleware struct {
	config    configs.Config
	authCache authport.AuthCache
}

func NewAuthMiddleware(cfg configs.Config, authCache authport.AuthCache) *AuthMiddleware {
	return &AuthMiddleware{
		config:    cfg,
		authCache: authCache,
	}
}

func (a *AuthMiddleware) JWTAuthMiddleware() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		token := ctx.GetHeader("Authorization")

		if token != "" {
			if !strings.HasPrefix(token, "Bearer ") {
				ctx.AbortWithStatusJSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
				return
			}
			token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
		} else if cookieToken, err := ctx.Cookie("access_token"); err == nil {
			token = cookieToken
		}

		token = strings.TrimSpace(token)
		if token == "" {
			ctx.AbortWithStatusJSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
			return
		}

		claim, err := jwt.ValidateToken(token, a.config.JWT.Secret)
		if err != nil {
			ctx.AbortWithStatusJSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
			return
		}
		if a.authCache == nil {
			ctx.AbortWithStatusJSON(http.StatusServiceUnavailable, response.Error(http.StatusServiceUnavailable, "认证服务暂时不可用"))
			return
		}
		active, err := a.authCache.IsSessionActive(ctx, claim.UserID, claim.SessionID)
		if err != nil {
			ctx.AbortWithStatusJSON(http.StatusServiceUnavailable, response.Error(http.StatusServiceUnavailable, "认证服务暂时不可用"))
			return
		}
		if !active {
			ctx.AbortWithStatusJSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录已失效"))
			return
		}

		ctx.Set("userId", claim.UserID)
		ctx.Set("authSessionID", claim.SessionID)
		ctx.Next()
	}
}

func (a *AuthMiddleware) CookieOriginProtectionMiddleware() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		// 防止 CSFR 攻击，允许 GET 请求或者没有身份的请求访问
		if !isUnsafeMethod(ctx.Request.Method) || !hasAuthCookie(ctx) {
			ctx.Next()
			return
		}

		// 当请求会产生副作用时，比如 PUT、DEL、POST 请求，必须查看其是否来自可信任源
		origin := strings.TrimSpace(ctx.GetHeader("Origin"))
		if origin == "" || !a.isAllowedOrigin(ctx, origin) {
			ctx.AbortWithStatusJSON(http.StatusForbidden, response.Error(http.StatusForbidden, "请求来源不受信任"))
			return
		}

		ctx.Next()
	}
}

func hasAuthCookie(ctx *gin.Context) bool {
	if _, err := ctx.Cookie("access_token"); err == nil {
		return true
	}
	if _, err := ctx.Cookie("refresh_token"); err == nil {
		return true
	}
	return false
}

func isUnsafeMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func (a *AuthMiddleware) isAllowedOrigin(ctx *gin.Context, origin string) bool {
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return false
	}

	for _, allowed := range strings.Split(a.config.Security.AllowedOrigins, ",") {
		if strings.TrimSpace(allowed) == origin {
			return true
		}
	}

	requestScheme := "http"
	if ctx.Request.TLS != nil || strings.EqualFold(ctx.GetHeader("X-Forwarded-Proto"), "https") {
		requestScheme = "https"
	}

	return origin == requestScheme+"://"+ctx.Request.Host
}
