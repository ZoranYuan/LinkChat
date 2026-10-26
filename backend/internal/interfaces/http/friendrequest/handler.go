package friendrequest

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"

	friendapp "IM_backend/internal/application/friend"
	"IM_backend/internal/interfaces/http/response"
)

type Handler struct {
	app *friendapp.FriendRequestApplication
}

func NewHandler(app *friendapp.FriendRequestApplication) *Handler {
	return &Handler{
		app: app,
	}
}

func (fh *Handler) Create(c *gin.Context) {
	userID := c.GetString("userId")

	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}

	var newFriendRequest FriendRequestRequest
	if err := c.ShouldBindJSON(&newFriendRequest); err != nil {
		log.Println("解析好友申请参数失败：", err)
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}

	friendRequestApp, err := fh.app.CreateFriendRequest(userID, newFriendRequest.PeerUserID, newFriendRequest.Message)

	if err != nil {
		c.JSON(http.StatusConflict, response.Error(http.StatusBadRequest, err.Error()))
		return
	}

	c.JSON(http.StatusOK, response.Success(&FriendRequestResponse{
		RequestID:         friendRequestApp.RequestID,
		ApplicantUserID:   friendRequestApp.ApplicantUserID,
		ApplicantNickName: friendRequestApp.ApplicantNickName,
		Message:           friendRequestApp.Message,
		Status:            friendRequestApp.Status,
		ApplyTime:         friendRequestApp.ApplyTime,
	}))
}

func (fh *Handler) OperateRequest(c *gin.Context) {
	var res OperateRequestRequest

	userID := c.GetString("userId")
	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}

	if err := c.ShouldBindJSON(&res); err != nil {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}
	if res.Action != ActionAccept && res.Action != ActionReject {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "不支持的操作"))
		return
	}

	var err error
	if res.Action == ActionAccept {
		err = fh.app.Accept(res.RequestID, userID)
	} else {
		err = fh.app.Refuse(res.RequestID, userID)
	}

	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, err.Error()))
		return
	}

	c.JSON(http.StatusOK, response.Success(nil))
}

func (fh *Handler) List(c *gin.Context) {
	userID := c.GetString("userId")

	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}

	requestListApp, err := fh.app.ListFriendRequestsByUserID(userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "服务端错误"))
		return
	}

	res := make([]FriendRequestResponse, 0, len(requestListApp))
	for _, r := range requestListApp {
		res = append(res, FriendRequestResponse{
			RequestID:         r.RequestID,
			ApplicantUserID:   r.ApplicantUserID,
			ApplicantNickName: r.ApplicantNickName,
			Message:           r.Message,
			Status:            r.Status,
			ApplyTime:         r.ApplyTime,
		})
	}

	c.JSON(http.StatusOK, response.Success(res))
}
