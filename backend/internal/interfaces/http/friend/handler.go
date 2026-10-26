package friend

import (
	"net/http"

	"github.com/gin-gonic/gin"

	friendapp "IM_backend/internal/application/friend"
	"IM_backend/internal/interfaces/http/response"
)

type Handler struct {
	app *friendapp.FriendApplication
}

func NewHandler(app *friendapp.FriendApplication) *Handler {
	return &Handler{
		app: app,
	}
}

func (fh *Handler) GetFriendList(c *gin.Context) {
	userID := c.GetString("userId")
	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}

	friendListApp, err := fh.app.GetUserFriendList(userID)

	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "网络错误"))
		return
	}

	c.JSON(http.StatusOK, response.Success(toFriendListResponse(friendListApp)))
}
