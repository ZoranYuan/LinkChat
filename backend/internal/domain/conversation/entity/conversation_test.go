package entity

import (
	conversationvo "IM_backend/internal/domain/conversation/value_object"
	"testing"
)

func TestNewConversation(t *testing.T) {
	t.Run("私聊保存双方用户", func(t *testing.T) {
		conversation := NewConversation("c1", "u1", "u2", int(conversationvo.PrivateChat), 0, "")

		if conversation.UserId1 != "u1" || conversation.UserId2 != "u2" || conversation.RoomId != "" {
			t.Fatalf("私聊会话参与者错误: %+v", conversation)
		}
	})

	t.Run("群聊保存目标房间", func(t *testing.T) {
		conversation := NewConversation("c1", "u1", "r1", int(conversationvo.RoomChat), 0, "")

		if conversation.ConversationId != "c1" || conversation.UserId1 != "u1" || conversation.UserId2 != "" || conversation.RoomId != "r1" {
			t.Fatalf("群聊会话参与者错误: %+v", conversation)
		}
	})
}
