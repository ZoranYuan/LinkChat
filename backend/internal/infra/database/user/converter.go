package infrauser

import (
	domainuser "IM_backend/internal/domain/user"
)

func toDomain(m UserModel) domainuser.User {
	return domainuser.User{
		UserID:      m.UserID,
		Username:    m.Username,
		Nickname:    m.Nickname,
		Password:    domainuser.Password(m.Password),
		Phone:       domainuser.Phone(m.Phone),
		Avatar:      m.Avatar,
		Status:      domainuser.Status(m.Status),
		OnlineTime:  m.OnlineTime,
		OfflineTime: m.OfflineTime,

		// 用于可以利用微信、手机号、QQ 登录
		LoginType: domainuser.LoginType(m.LoginType),
		WxOpenID:  m.WxOpenID,
		WxUnionID: m.WxUnionID,
	}
}
