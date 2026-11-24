package migration

import (
	"gorm.io/gorm"

	infraconversation "IM_backend/core/infra/database/conversation"
	infrafile "IM_backend/core/infra/database/file"
	infrafriend "IM_backend/core/infra/database/friend"
	infrainbox "IM_backend/core/infra/database/inbox"
	inframessage "IM_backend/core/infra/database/message"
	infragroup "IM_backend/core/infra/database/group"
	infrauser "IM_backend/core/infra/database/user"
)

// Migrate 按当前模型创建或更新表结构。
func Migrate(db *gorm.DB) error {
	// room → group 就地重命名（保留数据）。必须在 AutoMigrate 之前，
	// 否则 AutoMigrate 会按新模型建出空的 groups/group_members 表。
	if err := renameRoomToGroup(db); err != nil {
		return err
	}

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
		&infragroup.GroupModel{},
		&infragroup.GroupMemberModel{},
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
	// kind 是消息 cType 在附件表里的冗余拷贝，application 层已无读取方。
	if db.Migrator().HasColumn("message_attachments", "kind") {
		return db.Migrator().DropColumn("message_attachments", "kind")
	}
	return nil
}

// renameRoomToGroup 把「房间」时期的表/列/索引就地重命名为「群聊」命名。
// 每一步都做存在性判断，可重复执行；新名已存在时跳过，避免误覆盖。
func renameRoomToGroup(db *gorm.DB) error {
	m := db.Migrator()

	// 1. 表：rooms → groups，room_members → group_members
	// 注意：groups 是 MySQL 8 保留字（窗口函数 GROUPS），表名必须加反引号。
	if m.HasTable("rooms") && !m.HasTable("groups") {
		if err := db.Exec("RENAME TABLE `rooms` TO `groups`").Error; err != nil {
			return err
		}
	}
	if m.HasTable("room_members") && !m.HasTable("group_members") {
		if err := db.Exec("RENAME TABLE `room_members` TO `group_members`").Error; err != nil {
			return err
		}
	}

	// 2. 列：room_id → group_id（groups / group_members / conversations）
	for _, table := range []string{"groups", "group_members", "conversations"} {
		if m.HasTable(table) && m.HasColumn(table, "room_id") && !m.HasColumn(table, "group_id") {
			if err := db.Exec("ALTER TABLE `" + table + "` RENAME COLUMN room_id TO group_id").Error; err != nil {
				return err
			}
		}
	}
	// groups 表：room_name → group_name
	if m.HasTable("groups") && m.HasColumn("groups", "room_name") && !m.HasColumn("groups", "group_name") {
		if err := db.Exec("ALTER TABLE `groups` RENAME COLUMN room_name TO group_name").Error; err != nil {
			return err
		}
	}

	// 3. 索引：RENAME COLUMN 不会改索引名，显式重命名以对齐 GORM 模型
	// idx_room_members_room_id → idx_group_members_group_id
	if m.HasTable("group_members") && m.HasIndex("group_members", "idx_room_members_room_id") {
		if err := db.Exec("ALTER TABLE `group_members` RENAME INDEX idx_room_members_room_id TO idx_group_members_group_id").Error; err != nil {
			return err
		}
	}
	// uk_conversation_room → uk_conversation_group
	if m.HasTable("conversations") && m.HasIndex("conversations", "uk_conversation_room") {
		if err := db.Exec("ALTER TABLE `conversations` RENAME INDEX uk_conversation_room TO uk_conversation_group").Error; err != nil {
			return err
		}
	}
	return nil
}

// inboxNeedsRebuild 检测是否为旧版 inbox 表（sender_id/lock_token 等列）。
// 旧表列结构与当前模型不兼容，且存在 NOT NULL 老列会导致新记录插入失败，需重建。
func inboxNeedsRebuild(db *gorm.DB) bool {
	if !db.Migrator().HasTable("inboxes") {
		return false
	}
	for _, legacyColumn := range []string{"sender_id", "client_msg_id", "status", "lock_token", "locked_at", "attempt_count", "finished_at", "last_error"} {
		if db.Migrator().HasColumn("inboxes", legacyColumn) {
			return true
		}
	}
	// 早期版本把 event_id 拼成 sender_id:client_msg_id，与运行时不一致，需重建并按当前格式回填。
	var legacyRows int64
	if err := db.Table("inboxes").
		Where("event_type = ? AND event_id LIKE ?", "message_send", "%:%").
		Count(&legacyRows).Error; err == nil && legacyRows > 0 {
		return true
	}
	return false
}

// backfillInbox 从已有消息回填幂等指纹，避免历史消息在升级后被当作新事件重复处理。
// event_id 必须与运行时保持一致：直接用 client_msg_id。
// 前提是 clientMsgID 全局唯一（客户端生成 UUID），因此无需再拼发送方维度。
func backfillInbox(db *gorm.DB) error {
	if !db.Migrator().HasTable(&inframessage.MessageModel{}) {
		return nil
	}
	requestHash := "''"
	if db.Migrator().HasColumn("messages", "request_hash") {
		requestHash = "COALESCE(request_hash, '')"
	}
	query := `INSERT IGNORE INTO inboxes (event_type, event_id, request_hash)
		SELECT 'message_send', client_msg_id, ` + requestHash + `
		FROM messages
		WHERE client_msg_id IS NOT NULL AND client_msg_id <> ''`
	return db.Exec(query).Error
}
