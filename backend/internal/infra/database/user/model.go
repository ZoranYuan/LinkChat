package infrauser

import (
	"time"
)

type UserModel struct {
	UserID      string     `json:"userId" gorm:"size:32;primaryKey;not null;comment:用户ID"`
	Username    string     `json:"username" gorm:"column:user_name;size:16;uniqueIndex;not null"`
	Nickname    string     `json:"nickName" gorm:"column:nick_name;size:64"`
	Password    string     `json:"password" gorm:"size:255"`
	Phone       string     `json:"phone" gorm:"size:20;uniqueIndex"`
	Avatar      string     `json:"avatar" gorm:"size:100"`
	Status      int        `json:"status" gorm:"default:1;comment:1正常2禁用"`
	OnlineTime  time.Time  `json:"onLineTime" gorm:"column:on_line_time"`
	OfflineTime *time.Time `json:"offLineTime" gorm:"column:off_line_time"`

	// 用于可以利用微信、手机号、QQ 登录
	LoginType int     `json:"loginType"`
	WxOpenID  *string `json:"-" gorm:"type:varchar(64);uniqueIndex;comment:微信OpenID(预留)"`
	WxUnionID *string `json:"-" gorm:"type:varchar(64);uniqueIndex;comment:微信UnionID(预留)"`

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (u *UserModel) TableName() string {
	return "users"
}
