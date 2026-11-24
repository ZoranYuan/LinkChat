package domaingroup

type Role int

var (
	RegularUser Role = 1
	HomeOwner   Role = 3
)

type GroupStatus int

var (
	Normal GroupStatus = 1
)

type GroupMemberStatus int

var (
	Active GroupMemberStatus = 1
	Muted  GroupMemberStatus = 2
	Kicked GroupMemberStatus = 3
	Left   GroupMemberStatus = 4
)
