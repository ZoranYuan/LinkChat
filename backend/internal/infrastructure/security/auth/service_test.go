package auth

import (
	"testing"
	"time"

	"IM_backend/internal/infrastructure/security/jwt"
)

func TestIssueTokensForSessionKeepsSessionID(t *testing.T) {
	issuer := NewTokenIssuer(Options{
		Secret:         "test-secret",
		AccessTokenTTL: time.Minute,
	})

	_, _, sessionID, err := issuer.IssueToken("user-1")
	if err != nil {
		t.Fatal(err)
	}
	accessToken, refreshToken, err := issuer.IssueTokensForSession("user-1", sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if refreshToken == "" {
		t.Fatal("refresh token should not be empty")
	}

	claims, err := jwt.ValidateToken(accessToken, "test-secret")
	if err != nil {
		t.Fatal(err)
	}
	if claims.UserID != "user-1" || claims.SessionID != sessionID {
		t.Fatalf("refreshed token claims = user=%q session=%q, want user=%q session=%q", claims.UserID, claims.SessionID, "user-1", sessionID)
	}
}
