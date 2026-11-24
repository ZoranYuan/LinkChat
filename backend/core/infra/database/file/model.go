package infrafile

type FileModel struct {
	FileID      string `gorm:"type:bigint;primaryKey" json:"fileId"`
	UploaderID  string `gorm:"type:bigint;not null;index:idx_file_uploader" json:"uploaderId"`
	Bucket      string `gorm:"size:128;not null" json:"bucket"`
	ObjectKey   string `gorm:"size:255;not null;uniqueIndex" json:"objectKey"`
	FileName    string `gorm:"size:255;not null" json:"fileName"`
	ContentType string `gorm:"size:128" json:"contentType"`
	// 标准方案：内容 hash 全局唯一，秒传命中即复用同一物理对象。
	// 注意：从旧的 uk_file_uploader_hash 迁移时必须先清理同 hash 的重复行。
	FileHash  *string `gorm:"size:128;uniqueIndex:uk_file_hash" json:"fileHash,omitempty"`
	Size      int64   `gorm:"not null" json:"size"`
	CreatedAt int64   `gorm:"not null;index:idx_file_created_at" json:"createdAt"`
	Status    string  `gorm:"size:16;not null;default:uploaded" json:"status"`
}

func (FileModel) TableName() string { return "files" }

type FileUploadModel struct {
	UploadID        string  `gorm:"type:bigint;primaryKey" json:"uploadId"`
	FileID          string  `gorm:"type:bigint;not null" json:"fileId"`
	UploaderID      string  `gorm:"type:bigint;not null;index:idx_file_upload_hash_status_created,priority:1" json:"uploaderId"`
	UploadMode      string  `gorm:"size:16;not null" json:"uploadMode"`
	StorageUploadID *string `gorm:"size:128" json:"storageUploadId,omitempty"`

	FileHash     string `gorm:"size:128;index:idx_file_upload_hash_status_created,priority:2" json:"fileHash,omitempty"`
	ObjectKey    string `gorm:"size:255;not null;uniqueIndex:uk_file_upload_object" json:"objectKey"`
	FileName     string `gorm:"size:255;not null" json:"fileName"`
	ContentType  string `gorm:"size:128" json:"contentType,omitempty"`
	ExpectedSize int64  `gorm:"not null" json:"expectedSize"`

	ChunkSize   *int64 `json:"chunkSize,omitempty"`
	TotalChunks *int   `json:"totalChunks,omitempty"`

	Status      string `gorm:"size:16;not null;index:idx_file_upload_status_expire,priority:1;index:idx_file_upload_hash_status_created,priority:3" json:"status"`
	ExpiresAt   int64  `gorm:"not null;index:idx_file_upload_status_expire,priority:2" json:"expiresAt"`
	CreatedAt   int64  `gorm:"not null;index:idx_file_upload_hash_status_created,priority:4" json:"createdAt"`
	UpdatedAt   int64  `gorm:"not null" json:"updatedAt"`
	CompletedAt *int64 `json:"completedAt,omitempty"`
}

func (FileUploadModel) TableName() string {
	return "file_uploads"
}
