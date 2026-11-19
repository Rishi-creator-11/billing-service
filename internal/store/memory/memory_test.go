package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/Rishi-creator-11/billing-service/internal/store"
	"github.com/Rishi-creator-11/billing-service/internal/store/memory"
	"github.com/Rishi-creator-11/billing-service/internal/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(*testing.T) store.Store { return memory.New() })
}

func TestLockRespectsContext(t *testing.T) {
	s := memory.New()
	release := make(chan struct{})
	started := make(chan struct{})
	go func() {
		_ = s.ReadWrite(context.Background(), func(store.Tx) error { close(started); <-release; return nil })
	}()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := s.ReadWrite(ctx, func(store.Tx) error { return nil })
	close(release)
	if err != context.DeadlineExceeded {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
}
