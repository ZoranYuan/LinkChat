package domainuser

import (
	"regexp"
)

type LoginType int

var (
	PhoneType LoginType = 1
	WxType    LoginType = 3
)

type Password string

type Phone string

func (p Phone) Validate() bool {
	pattern := `^1[3-9]\d{9}$`
	macthed, _ := regexp.MatchString(pattern, string(p))
	return macthed
}

type Status int

var (
	StatusActivate Status = 0
)
