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
	if inboxNeedsRebuild(db) {
		if err := db.Migrator().DropTable(&infrainbox.InboxModel{}); err != nil {
			return err
		}
	}
	if err := migrateInboxIndexes(db); err != nil {
		return err
	}

	messageModel := &inframessage.MessageModel{}
	if db.Migrator().HasIndex(messageModel, "uq_sender_client_msg") {
		if err := db.Migrator().DropIndex(messageModel, "uq_sender_client_msg"); err != nil {
			return err
		}
	}

	if err := db.AutoMigrate(
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
	); err != nil {
		return err
	}
	if err := backfillInbox(db); err != nil {
		return err
	}
	if db.Migrator().HasColumn("messages", "request_hash") {
		return db.Migrator().DropColumn("messages", "request_hash")
	}
	return nil
}

func migrateInboxIndexes(db *gorm.DB) error {
	model := &infrainbox.InboxModel{}
	if !db.Migrator().HasTable(model) {
		return nil
	}
	if !db.Migrator().HasColumn(model, "id") {
		alter := "ALTER TABLE inboxes DROP PRIMARY KEY, ADD COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST"
		if !db.Migrator().HasIndex(model, "uq_inboxes_sender_client") {
			alter += ", ADD UNIQUE KEY uq_inboxes_sender_client (sender_id, client_msg_id)"
		}
		if err := db.Exec(alter).Error; err != nil {
			return err
		}
	}
	if !db.Migrator().HasIndex(model, "uq_inboxes_sender_client") {
		if err := db.Exec("CREATE UNIQUE INDEX uq_inboxes_sender_client ON inboxes (sender_id, client_msg_id)").Error; err != nil {
			return err
		}
	}
	if db.Migrator().HasIndex(model, "idx_inboxes_lock_token") {
		return db.Migrator().DropIndex(model, "idx_inboxes_lock_token")
	}
	return nil
}

func inboxNeedsRebuild(db *gorm.DB) bool {
	model := &infrainbox.InboxModel{}
	if !db.Migrator().HasTable(model) {
		return false
	}
	for _, column := range []string{"event_id", "event_type", "attempt_count", "finished_at"} {
		if db.Migrator().HasColumn("inboxes", column) {
			return true
		}
	}
	for _, column := range []string{"sender_id", "client_msg_id", "request_hash", "status", "lock_token", "locked_at", "last_error"} {
		if !db.Migrator().HasColumn("inboxes", column) {
			return true
		}
	}
	return false
}

func backfillInbox(db *gorm.DB) error {
	if !db.Migrator().HasTable(&inframessage.MessageModel{}) {
		return nil
	}
	requestHash := "''"
	if db.Migrator().HasColumn("messages", "request_hash") {
		requestHash = "COALESCE(request_hash, '')"
	}
	query := `INSERT IGNORE INTO inboxes
		(sender_id, client_msg_id, request_hash, status, lock_token, locked_at, last_error)
		SELECT sender_id, client_msg_id, ` + requestHash + `, 'completed', '', NULL, ''
		FROM messages
		WHERE client_msg_id IS NOT NULL AND client_msg_id <> ''`
	return db.Exec(query).Error
}
