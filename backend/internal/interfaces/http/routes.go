package httpapi

import (
	"github.com/gin-gonic/gin"

	ratelimitport "IM_backend/internal/application/ports/ratelimit"
	conversationhttp "IM_backend/internal/interfaces/http/conversation"
	filehttp "IM_backend/internal/interfaces/http/file"
	friendhttp "IM_backend/internal/interfaces/http/friend"
	friendrequesthttp "IM_backend/internal/interfaces/http/friendrequest"
	messagehttp "IM_backend/internal/interfaces/http/message"
	"IM_backend/internal/interfaces/http/middleware"
	roomhttp "IM_backend/internal/interfaces/http/room"
	userhttp "IM_backend/internal/interfaces/http/user"
)

func RegisterUserRouter(
	r *gin.RouterGroup,
	uh *userhttp.Handler,
	auth *middleware.AuthMiddleware,
	limiter *middleware.RateLimitMiddleware,
	userAPIPolicy ratelimitport.Policy,
) {
	userGroup := r.Group("/users")
	userhttp.RegisterRoutes(userGroup, uh, auth, limiter, userAPIPolicy)
}

func RegisterConversationRouter(r *gin.RouterGroup, ch *conversationhttp.Handler, auth *middleware.AuthMiddleware) {
	ucGroup := r.Group("/conversations").Use(auth.JWTAuthMiddleware())
	conversationhttp.RegisterRoutes(ucGroup, ch)
}

func RegisterFriendRequestRouter(r *gin.RouterGroup, fh *friendrequesthttp.Handler, authMiddle *middleware.AuthMiddleware) {
	friendRequestGroup := r.Group("/friend-requests").Use(authMiddle.JWTAuthMiddleware())
	friendrequesthttp.RegisterRoutes(friendRequestGroup, fh)
}

func RegisterFriendRouter(r *gin.RouterGroup, fh *friendhttp.Handler, authMiddle *middleware.AuthMiddleware) {
	friendGroup := r.Group("/friends").Use(authMiddle.JWTAuthMiddleware())
	friendhttp.RegisterRoutes(friendGroup, fh)
}

func RegisterRoomRouter(r *gin.RouterGroup, rh *roomhttp.Handler, authMiddle *middleware.AuthMiddleware) {
	roomGroup := r.Group("/rooms").Use(authMiddle.JWTAuthMiddleware())
	roomhttp.RegisterRoutes(roomGroup, rh)
}

func RegisterMessagesRouter(r *gin.RouterGroup, mh *messagehttp.Handler, authMiddle *middleware.AuthMiddleware) {
	messageGroup := r.Group("/messages").Use(authMiddle.JWTAuthMiddleware())
	messagehttp.RegisterRoutes(messageGroup, mh)
}

func RegisterFileRouter(r *gin.RouterGroup, fh *filehttp.Handler, authMiddle *middleware.AuthMiddleware, limiter *middleware.RateLimitMiddleware, policy ratelimitport.Policy) {
	fileGroup := r.Group("/files").Use(authMiddle.JWTAuthMiddleware())
	filehttp.RegisterRoutes(fileGroup, fh, limiter, policy)
}
