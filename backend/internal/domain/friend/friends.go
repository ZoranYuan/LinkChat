package domainfriend

type Friend struct {
	UserID       string `json:"UserId"`
	FriendUserID string `json:"FriendUserId"`
	Remarks      string
	Status       Status
}
