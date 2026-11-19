package storetest

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Rishi-creator-11/billing-service/internal/store"
	"github.com/Rishi-creator-11/billing-service/internal/store/spanner"
)

// NewSpanner returns a store on a fresh, uniquely named database in the Cloud
// Spanner emulator. It skips the test when SPANNER_EMULATOR_HOST is not set.
func NewSpanner(t *testing.T) store.Store {
	t.Helper()
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("SPANNER_EMULATOR_HOST not set; start the emulator with `make emulator`")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	dbID := "t" + strings.ReplaceAll(uuid.NewString(), "-", "")[:20]
	if err := spanner.EnsureDatabase(ctx, "test-project", "test-instance", dbID); err != nil {
		t.Fatalf("ensure database: %v", err)
	}
	s, err := spanner.Open(ctx, "projects/test-project/instances/test-instance/databases/"+dbID)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return s
}
