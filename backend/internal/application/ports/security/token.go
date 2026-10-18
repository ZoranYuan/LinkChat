package security

type TokenIssuer interface {
	IssueToken(userId string) (accessToken string, refreshToken string, sessionID string, err error)
	IssueTokensForSession(userId, sessionID string) (accessToken string, refreshToken string, err error)
}
