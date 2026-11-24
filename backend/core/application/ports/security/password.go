package security

type PasswordHasher interface {
	Hash(password string) (string, error)
	Verify(password, hashedPassword string) bool
}
