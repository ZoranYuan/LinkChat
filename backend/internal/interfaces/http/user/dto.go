package user

type UserRegisterRequest struct {
	Phone             string `json:"phone"`
	Password          string `json:"password"`
	ReconfirmPassword string `json:"reconfirmPassword"`
	LoginType         int    `json:"loginType" binding:"required"`
}

// UserProfileResponse is the public user payload returned by auth and profile APIs.
type UserProfileResponse struct {
	UserID    string `json:"userId"`
	Username  string `json:"username"`
	Nickname  string `json:"nickName"`
	Phone     string `json:"phone"`
	AvatarURL string `json:"avatar"`
}

type UserLoginRequest struct {
	Account  string `json:"account" binding:"required"`
	Password string `json:"password" binding:"required"`
}

type UpdateUserProfileRequest struct {
	Username  *string `json:"username"`
	Nickname  *string `json:"nickName"`
	AvatarURL *string `json:"avatar"`
}
