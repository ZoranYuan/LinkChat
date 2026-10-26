package domainfile

type Upload struct {
	UploadID        string `json:"UploadId"`
	FileID          string `json:"FileId"`
	UploaderID      string `json:"UploaderId"`
	UploadMode      string
	StorageUploadID string `json:"StorageUploadId"`
	FileHash        string
	ObjectKey       string
	FileName        string
	ContentType     string
	ExpectedSize    int64
	ChunkSize       int64
	TotalChunks     int
	Status          string
	ExpiresAt       int64
	RetryCount      int
	NextRetryAt     int64
	LockedAt        *int64
	LockToken       string
	LastError       string
	CreatedAt       int64
	UpdatedAt       int64
}
