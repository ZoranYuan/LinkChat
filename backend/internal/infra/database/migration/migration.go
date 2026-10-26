package migration

import (
	"gorm.io/gorm"

	infraconversation "IM_backend/internal/infra/database/conversation"
	infrafile "IM_backend/internal/infra/database/file"
	infrafriend "IM_backend/internal/infra/database/friend"
	infrainbox "IM_backend/internal/infra/database/inbox"
	inframessage "IM_backend/internal/infra/database/message"
	infraoutbox "IM_backend/internal/infra/database/outbox"
	infraroom "IM_backend/internal/infra/database/room"
	infrauser "IM_backend/internal/infra/database/user"
)

// Migrate 按当前模型创建或更新表结构。
func Migrate(db *gorm.DB) error {
	return db.AutoMigrate(
		&infrauser.UserModel{},
		&infraoutbox.OutboxModel{},
		&infrainbox.InboxModel{},
		&infrafriend.FriendRequestModel{},
		&infrafriend.FriendModel{},
		&infraroom.RoomModel{},
		&infraroom.RoomMemberModel{},
		&infraconversation.ConversationModel{},
		&infraconversation.UserConversationModel{},
		&inframessage.MessageModel{},
		&inframessage.MessageStickerModel{},
		&inframessage.MessageAttachmentModel{},
		&infrafile.FileModel{},
		&infrafile.FileUploadModel{},
	)
}
