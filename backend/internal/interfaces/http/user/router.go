package user

import (
	"github.com/gin-gonic/gin"

	ratelimitport "IM_backend/internal/application/ports/ratelimit"
	"IM_backend/internal/interfaces/http/middleware"
)

func RegisterRoutes(
	r *gin.RouterGroup,
	uh *Handler,
	auth *middleware.AuthMiddleware,
	limiter *middleware.RateLimitMiddleware,
	userAPIPolicy ratelimitport.Policy,
) {
	ug := r.Group("/users")
	ug.POST("/login", limiter.ByIP("login", userAPIPolicy), uh.Login)
	ug.POST("/register", limiter.ByIP("register", userAPIPolicy), uh.Register)
	ug.POST("/refresh", limiter.ByIP("refresh", userAPIPolicy), uh.Refresh)

	ug.Use(auth.JWTAuthMiddleware())
	ug.PATCH("/me", limiter.ByIP("user_profile_update", userAPIPolicy), uh.UpdateUserProfile)
	ug.GET("/resolve", uh.FindUserByPhoneOrUsername)
	ug.POST("/logout", uh.Logout)
}
