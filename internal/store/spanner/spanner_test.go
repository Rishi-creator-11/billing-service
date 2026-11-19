package spanner_test

import (
	"testing"

	"github.com/Rishi-creator-11/billing-service/internal/store"
	"github.com/Rishi-creator-11/billing-service/internal/store/storetest"
)

// Runs the shared conformance suite against the Cloud Spanner emulator.
func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store { return storetest.NewSpanner(t) })
}
