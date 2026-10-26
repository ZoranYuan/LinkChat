package domainfile

import (
	"errors"
)

var (
	ErrFileHashConflict     = errors.New("文件哈希冲突")
	ErrFileIdentityConflict = errors.New("文件身份冲突")
)
