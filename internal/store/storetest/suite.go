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
		"CustomerUpdate":           customerUpdate,
		"CustomerDelete":           customerDelete,
		"ListCustomersPagination":  listCustomers,
		"SubscriptionMetadata":     subscriptionMetadata,
		"ListSubscriptions":        listSubscriptions,
		"UpdateMissingRows":        updateMissing,
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

func customerUpdate(t *testing.T, s store.Store) {
	a := billing.Customer{ID: id(), Email: "a@example.com", Name: "A", CreatedAt: now()}
	b := billing.Customer{ID: id(), Email: "b@example.com", Name: "B", CreatedAt: now()}
	mustRW(t, s, func(tx store.Tx) error {
		if err := tx.CreateCustomer(a); err != nil {
			return err
		}
		return tx.CreateCustomer(b)
	})

	a.Name, a.Email = "A2", "a2@example.com"
	mustRW(t, s, func(tx store.Tx) error { return tx.UpdateCustomer(a) })
	var got billing.Customer
	if err := s.ReadOnly(ctx(t), func(tx store.Tx) (err error) { got, err = tx.GetCustomer(a.ID); return }); err != nil {
		t.Fatal(err)
	}
	if got.Name != "A2" || got.Email != "a2@example.com" || !got.CreatedAt.Equal(a.CreatedAt) {
		t.Fatalf("got %+v", got)
	}

	// Saving a customer with its own, unchanged email is not a conflict.
	mustRW(t, s, func(tx store.Tx) error { return tx.UpdateCustomer(a) })

	// The old email is free again; another customer's email is not.
	mustRW(t, s, func(tx store.Tx) error {
		return tx.CreateCustomer(billing.Customer{ID: id(), Email: "a@example.com", CreatedAt: now()})
	})
	b.Email = "a2@example.com"
	err := s.ReadWrite(ctx(t), func(tx store.Tx) error { return tx.UpdateCustomer(b) })
	if !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("want ErrAlreadyExists, got %v", err)
	}
	if err := s.ReadOnly(ctx(t), func(tx store.Tx) (err error) { got, err = tx.GetCustomer(b.ID); return }); err != nil {
		t.Fatal(err)
	}
	if got.Email != "b@example.com" {
		t.Fatalf("failed update changed the row: %+v", got)
	}
}

func customerDelete(t *testing.T, s store.Store) {
	c := billing.Customer{ID: id(), Email: "del@example.com", CreatedAt: now()}
	mustRW(t, s, func(tx store.Tx) error { return tx.CreateCustomer(c) })

	// A rolled-back delete leaves the customer in place.
	boom := errors.New("boom")
	if err := s.ReadWrite(ctx(t), func(tx store.Tx) error {
		if err := tx.DeleteCustomer(c.ID); err != nil {
			return err
		}
		return boom
	}); !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	err := s.ReadOnly(ctx(t), func(tx store.Tx) error { _, err := tx.GetCustomer(c.ID); return err })
	if err != nil {
		t.Fatalf("customer lost after rollback: %v", err)
	}

	mustRW(t, s, func(tx store.Tx) error { return tx.DeleteCustomer(c.ID) })
	err = s.ReadOnly(ctx(t), func(tx store.Tx) error { _, err := tx.GetCustomer(c.ID); return err })
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound after delete, got %v", err)
	}
	err = s.ReadWrite(ctx(t), func(tx store.Tx) error { return tx.DeleteCustomer(c.ID) })
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("deleting twice: want ErrNotFound, got %v", err)
	}
	// The email is reusable.
	mustRW(t, s, func(tx store.Tx) error {
		return tx.CreateCustomer(billing.Customer{ID: id(), Email: c.Email, CreatedAt: now()})
	})
}

func listCustomers(t *testing.T, s store.Store) {
	base := now()
	want := map[string]bool{}
	for i := 0; i < 7; i++ {
		// Pairs share a timestamp so the id tie-break is exercised.
		c := billing.Customer{ID: id(), Email: fmt.Sprintf("c%d@example.com", i), CreatedAt: base.Add(time.Duration(i/2) * time.Millisecond)}
		want[c.ID] = true
		mustRW(t, s, func(tx store.Tx) error { return tx.CreateCustomer(c) })
	}

	var all []billing.Customer
	var cur *store.ListCursor
	pages := 0
	for ; pages < 10; pages++ {
		var page []billing.Customer
		err := s.ReadOnly(ctx(t), func(tx store.Tx) (err error) { page, err = tx.ListCustomers(cur, 3); return })
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		all = append(all, page...)
		last := page[len(page)-1]
		cur = &store.ListCursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	if len(all) != 7 || pages != 3 {
		t.Fatalf("saw %d customers in %d pages, want 7 in 3", len(all), pages)
	}
	for i, c := range all {
		if !want[c.ID] {
			t.Fatalf("unexpected customer %s", c.ID)
		}
		delete(want, c.ID)
		if i > 0 && (all[i-1].CreatedAt.After(c.CreatedAt) || (all[i-1].CreatedAt.Equal(c.CreatedAt) && all[i-1].ID >= c.ID)) {
			t.Fatal("not ordered by (created_at, id)")
		}
	}

	var unlimited []billing.Customer
	if err := s.ReadOnly(ctx(t), func(tx store.Tx) (err error) { unlimited, err = tx.ListCustomers(nil, 0); return }); err != nil || len(unlimited) != 7 {
		t.Fatalf("limit 0 should mean no limit: %d rows, err %v", len(unlimited), err)
	}
}

func subscriptionMetadata(t *testing.T, s store.Store) {
	sub := billing.Subscription{ID: id(), CustomerID: id(), PlanID: "pro", Price: billing.Money{AmountMinor: 100, Currency: "USD"},
		Interval: billing.IntervalYearly, State: billing.StatePending, CreatedAt: now(), UpdatedAt: now(),
		Metadata: map[string]string{"team": "core", "seats": "5"}}
	mustRW(t, s, func(tx store.Tx) error { return tx.CreateSubscription(sub) })
	read := func() billing.Subscription {
		var got billing.Subscription
		if err := s.ReadOnly(ctx(t), func(tx store.Tx) (err error) { got, err = tx.GetSubscription(sub.ID); return }); err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := read(); len(got.Metadata) != 2 || got.Metadata["team"] != "core" || got.Metadata["seats"] != "5" {
		t.Fatalf("metadata %v", got.Metadata)
	}
	sub.PlanID, sub.Metadata = "team", nil
	mustRW(t, s, func(tx store.Tx) error { return tx.UpdateSubscription(sub) })
	if got := read(); got.PlanID != "team" || len(got.Metadata) != 0 {
		t.Fatalf("after clearing: %+v", got)
	}
}

func listSubscriptions(t *testing.T, s store.Store) {
	custA, custB := id(), id()
	base := now()
	mk := func(cust string, i int) billing.Subscription {
		return billing.Subscription{ID: id(), CustomerID: cust, PlanID: fmt.Sprintf("p%d", i), Price: billing.Money{AmountMinor: 100, Currency: "USD"},
			Interval: billing.IntervalMonthly, State: billing.StatePending, CreatedAt: base.Add(time.Duration(i) * time.Millisecond), UpdatedAt: base}
	}
	var aIDs []string
	for i := 0; i < 5; i++ {
		sub := mk(custA, i)
		aIDs = append(aIDs, sub.ID)
		mustRW(t, s, func(tx store.Tx) error { return tx.CreateSubscription(sub) })
	}
	for i := 0; i < 2; i++ {
		sub := mk(custB, i+10)
		mustRW(t, s, func(tx store.Tx) error { return tx.CreateSubscription(sub) })
	}

	// Walk custA's subscriptions two at a time.
	var got []string
	var cur *store.ListCursor
	for pages := 0; pages < 10; pages++ {
		var page []billing.Subscription
		err := s.ReadOnly(ctx(t), func(tx store.Tx) (err error) { page, err = tx.ListSubscriptions(custA, cur, 2); return })
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, sub := range page {
			if sub.CustomerID != custA {
				t.Fatal("leaked another customer's subscription")
			}
			got = append(got, sub.ID)
		}
		last := page[len(page)-1]
		cur = &store.ListCursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	if fmt.Sprint(got) != fmt.Sprint(aIDs) {
		t.Fatalf("got %v, want %v (creation order)", got, aIDs)
	}

	var everyone []billing.Subscription
	if err := s.ReadOnly(ctx(t), func(tx store.Tx) (err error) { everyone, err = tx.ListSubscriptions("", nil, 0); return }); err != nil || len(everyone) != 7 {
		t.Fatalf("unfiltered list: %d rows, err %v", len(everyone), err)
	}
	var none []billing.Subscription
	if err := s.ReadOnly(ctx(t), func(tx store.Tx) (err error) { none, err = tx.ListSubscriptions(id(), nil, 10); return }); err != nil || len(none) != 0 {
		t.Fatalf("unknown customer: %d rows, err %v", len(none), err)
	}
}

func updateMissing(t *testing.T, s store.Store) {
	err := s.ReadWrite(ctx(t), func(tx store.Tx) error {
		return tx.UpdateCustomer(billing.Customer{ID: id(), Email: "ghost@example.com", CreatedAt: now()})
	})
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("UpdateCustomer: want ErrNotFound, got %v", err)
	}
	err = s.ReadWrite(ctx(t), func(tx store.Tx) error {
		return tx.UpdateSubscription(billing.Subscription{ID: id(), CustomerID: id(), PlanID: "p", Price: billing.Money{AmountMinor: 1, Currency: "USD"},
			Interval: billing.IntervalMonthly, State: billing.StatePending, CreatedAt: now(), UpdatedAt: now()})
	})
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("UpdateSubscription: want ErrNotFound, got %v", err)
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
			return fmt.Errorf("customer: %w", err)
		}
		if _, err := tx.GetSubscription("nope"); !errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("subscription: %w", err)
		}
		if _, err := tx.GetCharge("nope"); !errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("charge: %w", err)
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
