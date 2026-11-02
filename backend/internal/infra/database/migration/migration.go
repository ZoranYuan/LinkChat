package migration

import (
	"gorm.io/gorm"

	infraconversation "IM_backend/internal/infra/database/conversation"
	infrafile "IM_backend/internal/infra/database/file"
	infrafriend "IM_backend/internal/infra/database/friend"
	infrainbox "IM_backend/internal/infra/database/inbox"
	inframessage "IM_backend/internal/infra/database/message"
	infraroom "IM_backend/internal/infra/database/room"
	infrauser "IM_backend/internal/infra/database/user"
)

// Migrate 按当前模型创建或更新表结构。
func Migrate(db *gorm.DB) error {
	if db.Migrator().HasTable("outboxes") {
		if err := db.Migrator().DropTable("outboxes"); err != nil {
			return err
		}
	}

	messageModel := &inframessage.MessageModel{}
	if db.Migrator().HasIndex(messageModel, "uq_sender_client_msg") {
		if err := db.Migrator().DropIndex(messageModel, "uq_sender_client_msg"); err != nil {
			return err
		}
	}

	return db.AutoMigrate(
		&infrauser.UserModel{},
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
