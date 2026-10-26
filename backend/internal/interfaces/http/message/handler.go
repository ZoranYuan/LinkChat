package message

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"IM_backend/configs"
	messageapp "IM_backend/internal/application/message"
	"IM_backend/internal/interfaces/http/response"
)

type Handler struct {
	app    *messageapp.MessageApplication
	config configs.MessageConfig
}

func NewHandler(app *messageapp.MessageApplication, config configs.MessageConfig) *Handler {
	return &Handler{
		app: app, config: config,
	}
}

func (mh *Handler) GetHistoryMessages(c *gin.Context) {
	userID := c.GetString("userId")
	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}
	var req MessageHistoryRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}
	messagesApp, nextCursor, hasMore, err := mh.app.GetHistoryMessages(c.Request.Context(), req.ConversationID, userID, req.Limit, req.Cursor)
	if err != nil {
		switch {
		case errors.Is(err, messageapp.ErrConversationNotFound):
			c.JSON(http.StatusNotFound, response.Error(http.StatusNotFound, "会话不存在"))
		case errors.Is(err, messageapp.ErrForbidden):
			c.JSON(http.StatusForbidden, response.Error(http.StatusForbidden, "无权查看该会话"))
		default:
			c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "获取消息失败，请稍后再试"))
		}
		return
	}
	c.JSON(http.StatusOK, response.Success(toHistoryMessageResponse(messagesApp, nextCursor, hasMore)))
}

func (mh *Handler) GetOfflineMessages(c *gin.Context) {
	userID := c.GetString("userId")
	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}

	var req MessageOfflineRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}
	if req.AfterSeq < 0 || req.Limit < 0 {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}

	messagesApp, nextCursor, hasMore, err := mh.app.GetOfflineMessages(
		c.Request.Context(),
		req.ConversationID,
		userID,
		req.AfterSeq,
		req.SnapshotSeq,
		req.Limit,
	)
	if err != nil {
		switch {
		case errors.Is(err, messageapp.ErrConversationNotFound):
			c.JSON(http.StatusNotFound, response.Error(http.StatusNotFound, "会话不存在"))
		case errors.Is(err, messageapp.ErrForbidden):
			c.JSON(http.StatusForbidden, response.Error(http.StatusForbidden, "无权查看该会话"))
		case errors.Is(err, messageapp.ErrMessageSeq):
			c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "快照序号无效"))
		default:
			c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "获取离线消息失败，请稍后再试"))
		}
		return
	}

	c.JSON(http.StatusOK, response.Success(toOfflineMessageResponse(messagesApp, nextCursor, hasMore)))
}

func (mh *Handler) SyncMessages(c *gin.Context) {
	userID := c.GetString("userId")
	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}

	var req MessageSyncRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}

	if req.ConversationID == "" {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}

	syncResult, err := mh.app.SyncMessages(
		c.Request.Context(),
		req.ConversationID,
		userID,
		req.AfterSeq,
	)
	if err != nil {
		switch {
		case errors.Is(err, messageapp.ErrConversationNotFound):
			c.JSON(http.StatusNotFound, response.Error(http.StatusNotFound, "会话不存在"))
		case errors.Is(err, messageapp.ErrForbidden):
			c.JSON(http.StatusForbidden, response.Error(http.StatusForbidden, "无权查看该会话"))
		default:
			c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "同步消息失败，请稍后再试"))
		}
		return
	}

	c.JSON(http.StatusOK, response.Success(toSyncMessageResponse(syncResult)))
}

func (mh *Handler) GetMessagesBySeqs(c *gin.Context) {
	userID := c.GetString("userId")
	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}

	var req MessageSeqsRequest
	if err := c.ShouldBindQuery(&req); err != nil || req.ConversationID == "" {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}

	seqs, err := parseMessageSeqs(req.Seqs)
	if err != nil || len(seqs) == 0 || len(seqs) > mh.config.MaxSyncSeqs {
		c.JSON(http.StatusBadRequest, response.Error(http.StatusBadRequest, "参数错误"))
		return
	}

	messagesApp, err := mh.app.GetMessagesBySeqs(c.Request.Context(), req.ConversationID, userID, seqs)
	if err != nil {
		switch {
		case errors.Is(err, messageapp.ErrConversationNotFound):
			c.JSON(http.StatusNotFound, response.Error(http.StatusNotFound, "会话不存在"))
		case errors.Is(err, messageapp.ErrForbidden):
			c.JSON(http.StatusForbidden, response.Error(http.StatusForbidden, "无权查看该会话"))
		default:
			c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "查询消息失败，请稍后再试"))
		}
		return
	}

	c.JSON(http.StatusOK, response.Success(toMessageSeqsResponse(messagesApp)))
}

func parseMessageSeqs(raw string) ([]int64, error) {
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	})
	seqs := make([]int64, 0, len(parts))
	seen := make(map[int64]struct{}, len(parts))
	for _, part := range parts {
		seq, err := strconv.ParseInt(part, 10, 64)
		if err != nil || seq <= 0 {
			return nil, err
		}
		if _, ok := seen[seq]; ok {
			continue
		}
		seen[seq] = struct{}{}
		seqs = append(seqs, seq)
	}
	return seqs, nil
}
