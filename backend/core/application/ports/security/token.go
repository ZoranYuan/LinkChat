package security

type TokenIssuer interface {
	IssueToken(userID string) (accessToken string, refreshToken string, sessionID string, err error)
	IssueTokensForSession(userID, sessionID string) (accessToken string, refreshToken string, err error)
}

// TokenValidator 返回已验证令牌中的用户与登录会话标识。
type TokenValidator interface {
	ValidateAccessToken(token string) (userID string, sessionID string, err error)
}
