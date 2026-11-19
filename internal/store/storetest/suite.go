// Package storetest is a conformance suite every store.Store implementation
// must pass.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Rishi-creator-11/billing-service/internal/billing"
	"github.com/Rishi-creator-11/billing-service/internal/store"
)

func id() string { return uuid.NewString() }

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return c
}

// Run executes the suite. newStore must return an empty, isolated store.
func Run(t *testing.T, newStore func(t *testing.T) store.Store) {
	tests := map[string]func(*testing.T, store.Store){
		"CustomerRoundTrip":        customerRoundTrip,
		"DuplicateEmailRejected":   duplicateEmail,
		"SubscriptionRoundTrip":    subscriptionRoundTrip,
		"ChargeRoundTrip":          chargeRoundTrip,
		"LedgerPaginationBalances": ledgerPagination,
		"IdempotencyRecord":        idempotencyRecord,
		"FailedTxRollsBack":        rollback,
		"ReadOnlyRejectsWrites":    readOnly,
		"ReadWriteIsSerializable":  serializable,
		"NotFoundErrors":           notFound,
		"AtomicMultiTableWrite":    atomicMulti,
	}
	for name, fn := range tests {
		t.Run(name, func(t *testing.T) {
			s := newStore(t)
			t.Cleanup(s.Close)
			fn(t, s)
		})
	}
}

func mustRW(t *testing.T, s store.Store, fn func(store.Tx) error) {
	t.Helper()
	if err := s.ReadWrite(ctx(t), fn); err != nil {
		t.Fatalf("ReadWrite: %v", err)
	}
}

func now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

func customerRoundTrip(t *testing.T, s store.Store) {
	c := billing.Customer{ID: id(), Email: "a@example.com", Name: "A", CreatedAt: now()}
	mustRW(t, s, func(tx store.Tx) error { return tx.CreateCustomer(c) })
	var got billing.Customer
	err := s.ReadOnly(ctx(t), func(tx store.Tx) (err error) { got, err = tx.GetCustomer(c.ID); return })
	if err != nil || got.Email != c.Email || got.Name != c.Name || !got.CreatedAt.Equal(c.CreatedAt) {
		t.Fatalf("got %+v err %v", got, err)
	}
}

func duplicateEmail(t *testing.T, s store.Store) {
	mustRW(t, s, func(tx store.Tx) error {
		return tx.CreateCustomer(billing.Customer{ID: id(), Email: "dup@example.com", CreatedAt: now()})
	})
	err := s.ReadWrite(ctx(t), func(tx store.Tx) error {
		return tx.CreateCustomer(billing.Customer{ID: id(), Email: "dup@example.com", CreatedAt: now()})
	})
	if !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("want ErrAlreadyExists, got %v", err)
	}
}

func subscriptionRoundTrip(t *testing.T, s store.Store) {
	sub := billing.Subscription{ID: id(), CustomerID: id(), PlanID: "pro", Price: billing.Money{AmountMinor: 999, Currency: "USD"},
		Interval: billing.IntervalMonthly, State: billing.StatePending, CreatedAt: now(), UpdatedAt: now()}
	mustRW(t, s, func(tx store.Tx) error { return tx.CreateSubscription(sub) })
	cancelAt := now()
	sub.State, sub.CanceledAt, sub.UpdatedAt = billing.StateCanceled, &cancelAt, cancelAt
	mustRW(t, s, func(tx store.Tx) error { return tx.UpdateSubscription(sub) })
	var got billing.Subscription
	if err := s.ReadOnly(ctx(t), func(tx store.Tx) (err error) { got, err = tx.GetSubscription(sub.ID); return }); err != nil {
		t.Fatal(err)
	}
	if got.State != billing.StateCanceled || got.CanceledAt == nil || !got.CanceledAt.Equal(cancelAt) ||
		got.Price != sub.Price || got.Interval != sub.Interval || got.PlanID != "pro" {
		t.Fatalf("got %+v", got)
	}
}

func chargeRoundTrip(t *testing.T, s store.Store) {
	c := billing.Charge{ID: id(), CustomerID: id(), SubscriptionID: id(), InvoiceID: "inv-1",
		Amount: billing.Money{AmountMinor: 5000, Currency: "EUR"}, Status: billing.ChargeSucceeded, CreatedAt: now()}
	mustRW(t, s, func(tx store.Tx) error { return tx.CreateCharge(c) })
	c.Status, c.RefundedMinor = billing.ChargePartiallyRefunded, 1200
	mustRW(t, s, func(tx store.Tx) error {
		if err := tx.CreateRefund(billing.Refund{ID: id(), ChargeID: c.ID, Amount: billing.Money{AmountMinor: 1200, Currency: "EUR"}, Reason: "r", CreatedAt: now()}); err != nil {
			return err
		}
		return tx.UpdateCharge(c)
	})
	var got billing.Charge
	if err := s.ReadOnly(ctx(t), func(tx store.Tx) (err error) { got, err = tx.GetCharge(c.ID); return }); err != nil {
		t.Fatal(err)
	}
	if got.Status != billing.ChargePartiallyRefunded || got.RefundedMinor != 1200 || got.Amount != c.Amount || got.InvoiceID != "inv-1" {
		t.Fatalf("got %+v", got)
	}
}

func ledgerPagination(t *testing.T, s store.Store) {
	cust, other := id(), id()
	base := now()
	var want []billing.LedgerEntry
	for i := 0; i < 5; i++ { // five transactions, two entries each
		at := base.Add(time.Duration(i) * time.Millisecond)
		es := billing.ChargePostings(id(), cust, id(), billing.Money{AmountMinor: int64(100 * (i + 1)), Currency: "USD"}, at, id)
		want = append(want, es...)
		mustRW(t, s, func(tx store.Tx) error { return tx.AppendLedger(es) })
	}
	mustRW(t, s, func(tx store.Tx) error {
		return tx.AppendLedger(billing.ChargePostings(id(), other, id(), billing.Money{AmountMinor: 7, Currency: "USD"}, base, id))
	})

	// Page through with limit 3 and check we see every entry exactly once.
	seen := map[string]bool{}
	var cur *store.LedgerCursor
	var prev time.Time
	for pages := 0; pages < 10; pages++ {
		var page []billing.LedgerEntry
		err := s.ReadOnly(ctx(t), func(tx store.Tx) (err error) { page, err = tx.ListLedger(cust, cur, 3); return })
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, e := range page {
			if seen[e.ID] {
				t.Fatalf("entry %s returned twice", e.ID)
			}
			if e.CustomerID != cust {
				t.Fatal("leaked another customer's entry")
			}
			if e.CreatedAt.Before(prev) {
				t.Fatal("not ordered by time")
			}
			prev = e.CreatedAt
			seen[e.ID] = true
		}
		last := page[len(page)-1]
		cur = &store.LedgerCursor{CreatedAt: last.CreatedAt, EntryID: last.ID}
	}
	if len(seen) != len(want) {
		t.Fatalf("saw %d entries, want %d", len(seen), len(want))
	}

	var bal []store.AccountBalance
	var sums map[string]int64
	err := s.ReadOnly(ctx(t), func(tx store.Tx) (err error) {
		if bal, err = tx.Balances(cust); err != nil {
			return err
		}
		sums, err = tx.LedgerSums()
		return
	})
	if err != nil {
		t.Fatal(err)
	}
	// 100+200+300+400+500 = 1500 debited to cash, credited to revenue.
	if len(bal) != 2 || bal[0].Account != billing.AccountCash || bal[0].Balance != 1500 ||
		bal[1].Account != billing.AccountRevenue || bal[1].Balance != -1500 {
		t.Fatalf("balances %+v", bal)
	}
	if sums["USD"] != 0 {
		t.Fatalf("ledger does not sum to zero: %v", sums)
	}
}

func idempotencyRecord(t *testing.T, s store.Store) {
	rec := billing.IdempotencyRecord{Key: "k1", Method: "ChargeInvoice", Fingerprint: make([]byte, 32), Response: []byte("resp"), CreatedAt: now()}
	err := s.ReadOnly(ctx(t), func(tx store.Tx) error { _, err := tx.GetIdempotency("k1"); return err })
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	mustRW(t, s, func(tx store.Tx) error { return tx.PutIdempotency(rec) })
	var got billing.IdempotencyRecord
	if err := s.ReadOnly(ctx(t), func(tx store.Tx) (err error) { got, err = tx.GetIdempotency("k1"); return }); err != nil {
		t.Fatal(err)
	}
	if got.Method != rec.Method || string(got.Response) != "resp" || len(got.Fingerprint) != 32 {
		t.Fatalf("got %+v", got)
	}
	err = s.ReadWrite(ctx(t), func(tx store.Tx) error { return tx.PutIdempotency(rec) })
	if !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("want ErrAlreadyExists, got %v", err)
	}
}

func rollback(t *testing.T, s store.Store) {
	c := billing.Customer{ID: id(), Email: "rb@example.com", CreatedAt: now()}
	boom := errors.New("boom")
	err := s.ReadWrite(ctx(t), func(tx store.Tx) error {
		if err := tx.CreateCustomer(c); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	err = s.ReadOnly(ctx(t), func(tx store.Tx) error { _, err := tx.GetCustomer(c.ID); return err })
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("write survived rollback: %v", err)
	}
	// The email must be reusable.
	mustRW(t, s, func(tx store.Tx) error {
		return tx.CreateCustomer(billing.Customer{ID: id(), Email: c.Email, CreatedAt: now()})
	})
}

func atomicMulti(t *testing.T, s store.Store) {
	cust := id()
	err := s.ReadWrite(ctx(t), func(tx store.Tx) error {
		if err := tx.AppendLedger(billing.ChargePostings(id(), cust, id(), billing.Money{AmountMinor: 1, Currency: "USD"}, now(), id)); err != nil {
			return err
		}
		return errors.New("abort after ledger write")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	var es []billing.LedgerEntry
	if err := s.ReadOnly(ctx(t), func(tx store.Tx) (err error) { es, err = tx.ListLedger(cust, nil, 10); return }); err != nil {
		t.Fatal(err)
	}
	if len(es) != 0 {
		t.Fatalf("ledger entries leaked from aborted tx: %d", len(es))
	}
}

func readOnly(t *testing.T, s store.Store) {
	err := s.ReadOnly(ctx(t), func(tx store.Tx) error {
		return tx.CreateCustomer(billing.Customer{ID: id(), Email: "ro@example.com", CreatedAt: now()})
	})
	if !errors.Is(err, store.ErrReadOnly) {
		t.Fatalf("want ErrReadOnly, got %v", err)
	}
}

func notFound(t *testing.T, s store.Store) {
	err := s.ReadOnly(ctx(t), func(tx store.Tx) error {
		if _, err := tx.GetCustomer("nope"); !errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("customer: %v", err)
		}
		if _, err := tx.GetSubscription("nope"); !errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("subscription: %v", err)
		}
		if _, err := tx.GetCharge("nope"); !errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("charge: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// serializable runs N read-modify-write transactions concurrently against one
// row. If transactions were not serializable some increments would be lost.
func serializable(t *testing.T, s store.Store) {
	c := billing.Charge{ID: id(), CustomerID: id(), SubscriptionID: id(), InvoiceID: "i",
		Amount: billing.Money{AmountMinor: 1_000_000, Currency: "USD"}, Status: billing.ChargeSucceeded, CreatedAt: now()}
	mustRW(t, s, func(tx store.Tx) error { return tx.CreateCharge(c) })

	const n = 25
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.ReadWrite(ctx(t), func(tx store.Tx) error {
				cur, err := tx.GetCharge(c.ID)
				if err != nil {
					return err
				}
				cur.RefundedMinor++
				return tx.UpdateCharge(cur)
			})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var got billing.Charge
	if err := s.ReadOnly(ctx(t), func(tx store.Tx) (err error) { got, err = tx.GetCharge(c.ID); return }); err != nil {
		t.Fatal(err)
	}
	if got.RefundedMinor != n {
		t.Fatalf("lost updates: counter %d, want %d", got.RefundedMinor, n)
	}
}
