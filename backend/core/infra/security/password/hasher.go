package password

import (
	"golang.org/x/crypto/bcrypt"

	"IM_backend/configs"
)

type Hasher struct{ config configs.SecurityConfig }

func NewHasher(config configs.SecurityConfig) *Hasher {
	return &Hasher{config: config}
}

func (h *Hasher) Hash(password string) (string, error) {
	hashed, err := bcrypt.GenerateFromPassword([]byte(password), h.config.BcryptCost)
	return string(hashed), err
}

func (h *Hasher) Verify(password, hashedPassword string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password)) == nil
}
