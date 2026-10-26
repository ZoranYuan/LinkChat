package infrafile

import (
	domainfile "IM_backend/internal/domain/file"
)

func toFileModel(f *domainfile.File) *FileModel {
	var fileHash *string
	if f.FileHash != "" {
		fileHash = &f.FileHash
	}
	return &FileModel{
		FileID:      f.FileID,
		UploaderID:  f.UploaderID,
		Bucket:      f.Bucket,
		ObjectKey:   f.ObjectKey,
		FileName:    f.FileName,
		ContentType: f.ContentType,
		FileHash:    fileHash,
		Size:        f.Size,
		CreatedAt:   f.CreatedAt,
		Status:      f.Status,
	}
}

func toFileDomain(m *FileModel) *domainfile.File {
	return &domainfile.File{
		FileID:      m.FileID,
		UploaderID:  m.UploaderID,
		Bucket:      m.Bucket,
		ObjectKey:   m.ObjectKey,
		FileName:    m.FileName,
		ContentType: m.ContentType,
		FileHash:    stringValue(m.FileHash),
		Size:        m.Size,
		CreatedAt:   m.CreatedAt,
		Status:      m.Status,
	}
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
