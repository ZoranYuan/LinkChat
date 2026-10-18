package mysql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	outboxport "IM_backend/internal/application/ports/outbox"
	"IM_backend/internal/infrastructure/persistence/mysql/model"
	"IM_backend/internal/shared/protocol"
	"gorm.io/gorm"
)

func verifyPendingEventFormat(db *gorm.DB, table string) error {
	var count int64
	// payload 是 LONGBLOB，先转换为 UTF-8 文本再检查 JSON，避免二进制字符集导致误判。
	if err := db.Table(table).Where("status IN ?", []string{outboxport.StatusPending, outboxport.StatusProcessing}).
		Where("event_type = ? OR CASE WHEN JSON_VALID(CAST(payload AS CHAR CHARACTER SET utf8mb4)) THEN JSON_CONTAINS_PATH(CAST(payload AS CHAR CHARACTER SET utf8mb4), 'one', '$.Payload') ELSE 1 END = 1", "file_card_warmup").
		Count(&count).Error; err != nil {
		return fmt.Errorf("检查待投递事件格式失败：%w", err)
	}
	if count > 0 {
		return fmt.Errorf("仍有 %d 个旧格式或已移除事件，请执行 go run ./cmd/migrate -event-delivery 完成迁移", count)
	}
	return nil
}

// MigrateEventDelivery 仅由停服迁移命令调用，旧字段和旧事件格式不进入运行期处理链路。
func MigrateEventDelivery(db *gorm.DB) error {
	return migrateEventDeliveryTables(db, "outboxes", "inboxes")
}

func migrateEventDeliveryTables(db *gorm.DB, outboxTable, inboxTable string) error {
	for _, column := range [][3]string{
		{outboxTable, "id", "event_id"},
		{outboxTable, "message_key", "partition_key"},
		{inboxTable, "retry_count", "attempt_count"},
		{inboxTable, "processed_at", "finished_at"},
	} {
		migrator := db.Migrator()
		if !migrator.HasTable(column[0]) || !migrator.HasColumn(column[0], column[1]) {
			continue
		}
		if migrator.HasColumn(column[0], column[2]) {
			return fmt.Errorf("%s 同时存在 %s 和 %s，请先核对迁移状态", column[0], column[1], column[2])
		}
		if err := migrator.RenameColumn(column[0], column[1], column[2]); err != nil {
			return err
		}
	}
	if err := db.Table(outboxTable).AutoMigrate(&model.OutboxRecord{}); err != nil {
		return err
	}
	if err := db.Table(inboxTable).AutoMigrate(&model.InboxRecord{}); err != nil {
		return err
	}

	// 旧终态记录只保留结束时间，历史创建时间采用已有时间近似回填。
	now := time.Now()
	if err := db.Table(inboxTable).Where("created_at IS NULL").UpdateColumn("created_at", gorm.Expr("COALESCE(finished_at, ?)", now)).Error; err != nil {
		return err
	}
	if err := db.Table(inboxTable).Where("updated_at IS NULL").UpdateColumn("updated_at", gorm.Expr("COALESCE(finished_at, created_at, ?)", now)).Error; err != nil {
		return err
	}
	if err := db.Table(inboxTable).Where("status NOT IN ?", []string{"completed", "dead"}).UpdateColumn("finished_at", nil).Error; err != nil {
		return err
	}

	// 文件预热已由附件读取接口按需完成，旧待投递预热任务结束为 dead 并保留审计数据。
	if err := db.Table(outboxTable).Where("event_type = ? AND status IN ?", "file_card_warmup", []string{outboxport.StatusPending, outboxport.StatusProcessing}).Updates(map[string]any{
		"status": outboxport.StatusDead, "locked_at": nil, "lock_token": "",
		"last_error": "文件预热事件已移除，附件读取接口按需回源并回填缓存", "updated_at": now,
	}).Error; err != nil {
		return err
	}

	lastID := ""
	for {
		var rows []model.OutboxRecord
		if err := db.Table(outboxTable).Where("event_id > ?", lastID).Order("event_id ASC").Limit(100).Find(&rows).Error; err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		if err := db.Transaction(func(tx *gorm.DB) error {
			for _, row := range rows {
				if row.EventType == "file_card_warmup" {
					continue
				}
				payload, err := migrateStoredEventPayload(row.EventType, row.Payload)
				if err != nil {
					return fmt.Errorf("转换事件 %s 失败：%w", row.EventID, err)
				}
				if !bytes.Equal(payload, row.Payload) {
					if err := tx.Table(outboxTable).Where("event_id = ?", row.EventID).Update("payload", payload).Error; err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			return err
		}
		lastID = rows[len(rows)-1].EventID
	}
}

func migrateStoredEventPayload(eventType string, payload []byte) ([]byte, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil, err
	}
	var from, to string
	if _, wrapped := fields["Payload"]; wrapped {
		var stored struct {
			From    string
			To      string
			Payload []byte
		}
		if err := json.Unmarshal(payload, &stored); err != nil {
			return nil, err
		}
		from, to, payload = stored.From, stored.To, stored.Payload
	}
	switch eventType {
	case protocol.EventTypeSendMessage:
		var event protocol.MessageEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return nil, err
		}
		if event.SenderId == "" {
			event.SenderId = from
		}
		if event.RecvId == "" {
			event.RecvId = to
		}
		if event.MessageId == "" || event.SenderId == "" || event.RecvId == "" || event.ConversationId == "" || event.Seq <= 0 {
			return nil, fmt.Errorf("消息事件缺少必要字段")
		}
		return json.Marshal(event)
	case protocol.EventReadMessageCommitted:
		var event protocol.MessageReadCommittedEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return nil, err
		}
		if event.RecipientUserId == "" {
			event.RecipientUserId = to
		}
		if event.ReaderId == "" || event.RecipientUserId == "" || event.ConversationId == "" || event.LastReadSeq <= 0 {
			return nil, fmt.Errorf("已读事件缺少必要字段")
		}
		return json.Marshal(event)
	case protocol.EventFriendRequestCreated:
		var event protocol.FriendRequestCreatedEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return nil, err
		}
		if event.PeerUserId == "" {
			event.PeerUserId = to
		}
		if event.ApplicantUserId == "" {
			event.ApplicantUserId = from
		}
		if event.RequestId == "" || event.ApplicantUserId == "" || event.PeerUserId == "" {
			return nil, fmt.Errorf("好友申请事件缺少必要字段")
		}
		return json.Marshal(event)
	case protocol.EventRoomMemberChanged:
		var event protocol.RoomMemberChangedEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return nil, err
		}
		if event.RoomID == "" || event.UserID == "" || event.Version <= 0 {
			return nil, fmt.Errorf("成员变更事件缺少必要字段")
		}
		return json.Marshal(event)
	default:
		return nil, fmt.Errorf("未知事件类型：%s", eventType)
	}
}
