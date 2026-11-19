package service_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Rishi-creator-11/billing-service/internal/store/memory"
)

// Measures the service layer on the in-memory store (no network, no disk).
func BenchmarkChargeInvoiceUniqueKeys(b *testing.B) {
	f := newFixture(b, memory.New())
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.svc.ChargeInvoice(ctx, f.chargeReq(fmt.Sprint("k", i), 100)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkChargeInvoiceReplay(b *testing.B) {
	f := newFixture(b, memory.New())
	ctx := context.Background()
	req := f.chargeReq("same", 100)
	if _, err := f.svc.ChargeInvoice(ctx, req); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := f.svc.ChargeInvoice(ctx, req)
		if err != nil || !r.IdempotentReplay {
			b.Fatal(err)
		}
	}
}
