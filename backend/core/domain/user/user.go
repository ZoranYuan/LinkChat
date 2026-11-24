package domainuser

import (
	"time"
)

type User struct {
	UserID      string     `json:"userId"`
	Username    string     `json:"username"`
	Nickname    string     `json:"nickName"`
	Password    Password   `json:"password"`
	Phone       Phone      `json:"phone"`
	Avatar      string     `json:"avatar"`
	Status      Status     `json:"status"`
	OnlineTime  time.Time  `json:"onLineTime"`
	OfflineTime *time.Time `json:"offLineTime"`

	// 用于可以利用微信、手机号、QQ 登录
	LoginType LoginType `json:"loginType"`
	WxOpenID  *string   `json:"-"`
	WxUnionID *string   `json:"-"`
}

func RegisterWithPhone(phone Phone, password Password) (*User, error) {
	if !phone.Validate() {
		return nil, ErrInvalidPhoneNumber
	}

	var newUser = User{
		Phone:       phone,
		LoginType:   PhoneType,
		Status:      StatusActivate,
		Password:    password,
		Username:    string(phone),
		Nickname:    string(phone),
		OnlineTime:  time.Now(),
		OfflineTime: nil,
	}

	return &newUser, nil
}
