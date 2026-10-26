package domainroom

type Role int

var (
	RegularUser Role = 1
	HomeOwner   Role = 3
)

type RoomStatus int

var (
	Normal RoomStatus = 1
)

type RoomMemberStatus int

var (
	Active RoomMemberStatus = 1
	Muted  RoomMemberStatus = 2
	Kicked RoomMemberStatus = 3
	Left   RoomMemberStatus = 4
)
