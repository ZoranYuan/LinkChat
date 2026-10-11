package room

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestRoomActivityStateExpiresWithoutReadRenewal(t *testing.T) {
	s := NewRoomActivityState(2 * time.Minute)
	now := time.Now()
	if s.IsActive("room", now) {
		t.Fatal("unmarked room must not be active")
	}
	s.MarkActive("room", now)
	if !s.IsActive("room", now.Add(119*time.Second)) {
		t.Fatal("room expired before hold duration")
	}
	if s.IsActive("room", now.Add(2*time.Minute)) {
		t.Fatal("read must not renew ACTIVE")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.activeUntil) != 0 {
		t.Fatal("expired entry was not removed")
	}
}

func TestRoomActivityStateConcurrentRenewalNeverShortensDeadline(t *testing.T) {
	s := NewRoomActivityState(2 * time.Minute)
	now := time.Now()
	s.MarkActive("room", now)
	var wg sync.WaitGroup
	for i := 0; i <= 100; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			s.MarkActive("room", now.Add(time.Duration(offset)*time.Second))
			s.IsActive("room", now.Add(120*time.Second))
			s.DeleteExpired(now.Add(120 * time.Second))
		}(i)
	}
	wg.Wait()
	if !s.IsActive("room", now.Add(219*time.Second)) {
		t.Fatal("concurrent cleanup or older renewal removed latest deadline")
	}
	if s.IsActive("room", now.Add(220*time.Second)) {
		t.Fatal("room must expire after latest ACTIVE plus hold duration")
	}
}

func TestRoomActivityStateCleanupAndShutdown(t *testing.T) {
	s := NewRoomActivityState(2 * time.Minute)
	now := time.Now()
	s.MarkActive("expired", now.Add(-3*time.Minute))
	s.MarkActive("active", now)
	s.DeleteExpired(now)
	s.mu.Lock()
	count := len(s.activeUntil)
	s.mu.Unlock()
	if count != 1 || !s.IsActive("active", now) {
		t.Fatal("cleanup must retain unexpired room")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cleanup goroutine did not stop")
	}
}
