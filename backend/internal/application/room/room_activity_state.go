package room

import (
	"context"
	"sync"
	"time"
)

// RoomActivityState 在当前节点共享 ACTIVE 标识；实时升级，最后一次 ACTIVE 后延迟降级。
type RoomActivityState struct {
	mu          sync.Mutex
	activeUntil map[string]time.Time
	hold        time.Duration
}

func NewRoomActivityState(hold time.Duration) *RoomActivityState {
	if hold <= 0 {
		panic("房间 ACTIVE 保持时间必须大于 0")
	}
	return &RoomActivityState{activeUntil: make(map[string]time.Time), hold: hold}
}

func (s *RoomActivityState) MarkActive(roomID string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	until := now.Add(s.hold)
	if until.After(s.activeUntil[roomID]) {
		s.activeUntil[roomID] = until
	}
}

// IsActive 只读取有效标识，不续期；过期检查和删除原子执行，避免误删并发续期结果。
func (s *RoomActivityState) IsActive(roomID string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	until, exists := s.activeUntil[roomID]
	if !exists {
		return false
	}
	if !now.Before(until) {
		delete(s.activeUntil, roomID)
		return false
	}
	return true
}

func (s *RoomActivityState) DeleteExpired(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for roomID, until := range s.activeUntil {
		if !now.Before(until) {
			delete(s.activeUntil, roomID)
		}
	}
}

// Run 仅回收无人访问的过期标识，投递时的逻辑过期不依赖定时器。
func (s *RoomActivityState) Run(ctx context.Context) {
	ticker := time.NewTicker(min(s.hold, time.Minute))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.DeleteExpired(time.Now())
		}
	}
}
