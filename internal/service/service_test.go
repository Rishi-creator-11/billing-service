package service_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	billingv1 "github.com/Rishi-creator-11/billing-service/gen/billing/v1"
	"github.com/Rishi-creator-11/billing-service/internal/billing"
	"github.com/Rishi-creator-11/billing-service/internal/service"
	"github.com/Rishi-creator-11/billing-service/internal/store"
	"github.com/Rishi-creator-11/billing-service/internal/store/memory"
	"github.com/Rishi-creator-11/billing-service/internal/store/storetest"
)

// Every test runs against the in-memory store and, when SPANNER_EMULATOR_HOST
// is set, against the Cloud Spanner emulator too.
func forEachStore(t *testing.T, fn func(t *testing.T, st store.Store)) {
	t.Run("memory", func(t *testing.T) {
		st := memory.New()
		t.Cleanup(st.Close)
		fn(t, st)
	})
	t.Run("spanner", func(t *testing.T) {
		st := storetest.NewSpanner(t) // skips without the emulator
		t.Cleanup(st.Close)
		fn(t, st)
	})
}

func usd(n int64) *billingv1.Money { return &billingv1.Money{AmountMinor: n, CurrencyCode: "USD"} }

type fixture struct {
	t    *testing.T
	svc  *service.Service
	st   store.Store
	cust *billingv1.Customer
	sub  *billingv1.Subscription
}

func newFixture(t *testing.T, st store.Store, opts ...service.Option) *fixture {
	t.Helper()
	svc := service.New(st, opts...)
	ctx := context.Background()
	c, err := svc.CreateCustomer(ctx, &billingv1.CreateCustomerRequest{Email: fmt.Sprintf("u%d@example.com", time.Now().UnixNano()), Name: "Test"})
	if err != nil {
		t.Fatal(err)
	}
	s, err := svc.CreateSubscription(ctx, &billingv1.CreateSubscriptionRequest{
		CustomerId: c.Customer.Id, PlanId: "pro", Price: usd(1999), Interval: billingv1.BillingInterval_BILLING_INTERVAL_MONTHLY})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{t: t, svc: svc, st: st, cust: c.Customer, sub: s.Subscription}
}

func (f *fixture) chargeReq(key string, amount int64) *billingv1.ChargeInvoiceRequest {
	return &billingv1.ChargeInvoiceRequest{IdempotencyKey: key, CustomerId: f.cust.Id, SubscriptionId: f.sub.Id,
		InvoiceId: "inv-" + key, Amount: usd(amount), PaymentMethodId: "pm_ok"}
}

func (f *fixture) ledger() []*billingv1.LedgerEntry {
	f.t.Helper()
	var all []*billingv1.LedgerEntry
	tok := ""
	for {
		r, err := f.svc.GetLedger(context.Background(), &billingv1.GetLedgerRequest{CustomerId: f.cust.Id, PageSize: 100, PageToken: tok})
		if err != nil {
			f.t.Fatal(err)
		}
		all = append(all, r.Entries...)
		if tok = r.NextPageToken; tok == "" {
			return all
		}
	}
}

func (f *fixture) assertBalanced() {
	f.t.Helper()
	var sum int64
	byTxn := map[string]int64{}
	for _, e := range f.ledger() {
		v := e.Amount.AmountMinor
		if e.Direction == billingv1.EntryDirection_ENTRY_DIRECTION_CREDIT {
			v = -v
		}
		sum += v
		byTxn[e.TransactionId] += v
	}
	if sum != 0 {
		f.t.Fatalf("customer ledger sums to %d, want 0", sum)
	}
	for id, v := range byTxn {
		if v != 0 {
			f.t.Fatalf("transaction %s is unbalanced: %d", id, v)
		}
	}
	var global map[string]int64
	if err := f.st.ReadOnly(context.Background(), func(tx store.Tx) (err error) { global, err = tx.LedgerSums(); return }); err != nil {
		f.t.Fatal(err)
	}
	for cur, v := range global {
		if v != 0 {
			f.t.Fatalf("global ledger %s sums to %d", cur, v)
		}
	}
}

func code(err error) codes.Code { return status.Code(err) }

func TestChargeAndReplay(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store) {
		f := newFixture(t, st)
		ctx := context.Background()
		first, err := f.svc.ChargeInvoice(ctx, f.chargeReq("k-1", 1999))
		if err != nil {
			t.Fatal(err)
		}
		if first.IdempotentReplay || first.Charge.Status != billingv1.ChargeStatus_CHARGE_STATUS_SUCCEEDED {
			t.Fatalf("unexpected first response %+v", first)
		}
		second, err := f.svc.ChargeInvoice(ctx, f.chargeReq("k-1", 1999))
		if err != nil {
			t.Fatal(err)
		}
		if !second.IdempotentReplay || second.Charge.Id != first.Charge.Id {
			t.Fatalf("expected replay of %s, got %+v", first.Charge.Id, second)
		}
		if n := len(f.ledger()); n != 2 {
			t.Fatalf("ledger has %d entries after a replay, want 2", n)
		}
		f.assertBalanced()

		got, _ := f.svc.GetSubscription(ctx, &billingv1.GetSubscriptionRequest{SubscriptionId: f.sub.Id})
		if got.Subscription.State != billingv1.SubscriptionState_SUBSCRIPTION_STATE_ACTIVE {
			t.Fatalf("subscription state %v, want ACTIVE after first payment", got.Subscription.State)
		}
		bal, err := f.svc.GetBalance(ctx, &billingv1.GetBalanceRequest{CustomerId: f.cust.Id})
		if err != nil || len(bal.NetCollected) != 1 || bal.NetCollected[0].AmountMinor != 1999 {
			t.Fatalf("balance %+v err %v", bal, err)
		}
	})
}

func TestKeyReuseWithDifferentPayload(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store) {
		f := newFixture(t, st)
		ctx := context.Background()
		if _, err := f.svc.ChargeInvoice(ctx, f.chargeReq("k-1", 1999)); err != nil {
			t.Fatal(err)
		}
		tests := []struct {
			name string
			mut  func(*billingv1.ChargeInvoiceRequest)
		}{
			{"amount", func(r *billingv1.ChargeInvoiceRequest) { r.Amount = usd(2000) }},
			{"invoice", func(r *billingv1.ChargeInvoiceRequest) { r.InvoiceId = "other" }},
			{"payment method", func(r *billingv1.ChargeInvoiceRequest) { r.PaymentMethodId = "pm_other" }},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				req := f.chargeReq("k-1", 1999)
				tt.mut(req)
				_, err := f.svc.ChargeInvoice(ctx, req)
				if code(err) != codes.FailedPrecondition {
					t.Fatalf("want FAILED_PRECONDITION, got %v", err)
				}
			})
		}
		if n := len(f.ledger()); n != 2 {
			t.Fatalf("ledger has %d entries, want 2", n)
		}
	})
}

func TestKeyReuseAcrossMethods(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store) {
		f := newFixture(t, st)
		ctx := context.Background()
		ch, err := f.svc.ChargeInvoice(ctx, f.chargeReq("shared", 1000))
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.svc.RefundCharge(ctx, &billingv1.RefundChargeRequest{IdempotencyKey: "shared", ChargeId: ch.Charge.Id, Amount: usd(1000)})
		if code(err) != codes.FailedPrecondition {
			t.Fatalf("want FAILED_PRECONDITION, got %v", err)
		}
	})
}

// The core guarantee: many simultaneous requests with one key charge once.
func TestConcurrentDuplicatesChargeOnce(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store) {
		f := newFixture(t, st)
		const n = 50
		var wg sync.WaitGroup
		var fresh, replays atomic.Int32
		ids := make(chan string, n)
		errs := make(chan error, n)
		start := make(chan struct{})
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				r, err := f.svc.ChargeInvoice(ctx, f.chargeReq("race", 1999))
				if err != nil {
					errs <- err
					return
				}
				if r.IdempotentReplay {
					replays.Add(1)
				} else {
					fresh.Add(1)
				}
				ids <- r.Charge.Id
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		close(ids)
		for err := range errs {
			t.Fatalf("request failed: %v", err)
		}
		distinct := map[string]bool{}
		for id := range ids {
			distinct[id] = true
		}
		if len(distinct) != 1 {
			t.Fatalf("%d distinct charge ids across duplicates, want exactly 1", len(distinct))
		}
		if fresh.Load() != 1 || replays.Load() != n-1 {
			t.Fatalf("fresh=%d replays=%d, want 1 and %d", fresh.Load(), replays.Load(), n-1)
		}
		if got := len(f.ledger()); got != 2 {
			t.Fatalf("ledger has %d entries, want 2 (one charge)", got)
		}
		f.assertBalanced()
	})
}

func TestConcurrentDistinctKeys(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store) {
		f := newFixture(t, st)
		const n = 20
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := f.svc.ChargeInvoice(context.Background(), f.chargeReq(fmt.Sprintf("k-%d", i), 100)); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		bal, _ := f.svc.GetBalance(context.Background(), &billingv1.GetBalanceRequest{CustomerId: f.cust.Id})
		if bal.NetCollected[0].AmountMinor != n*100 {
			t.Fatalf("net collected %d, want %d", bal.NetCollected[0].AmountMinor, n*100)
		}
		f.assertBalanced()
	})
}

func TestRefunds(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store) {
		f := newFixture(t, st)
		ctx := context.Background()
		ch, err := f.svc.ChargeInvoice(ctx, f.chargeReq("c1", 1000))
		if err != nil {
			t.Fatal(err)
		}
		refund := func(key string, amt int64) (*billingv1.RefundChargeResponse, error) {
			return f.svc.RefundCharge(ctx, &billingv1.RefundChargeRequest{IdempotencyKey: key, ChargeId: ch.Charge.Id, Amount: usd(amt), Reason: "test"})
		}
		r1, err := refund("r1", 400)
		if err != nil || r1.Charge.Status != billingv1.ChargeStatus_CHARGE_STATUS_PARTIALLY_REFUNDED || r1.Charge.Refunded.AmountMinor != 400 {
			t.Fatalf("partial refund: %+v %v", r1, err)
		}
		again, err := refund("r1", 400)
		if err != nil || !again.IdempotentReplay || again.Refund.Id != r1.Refund.Id {
			t.Fatalf("replay: %+v %v", again, err)
		}
		if _, err := refund("r2", 700); code(err) != codes.FailedPrecondition {
			t.Fatalf("over-refund should be FAILED_PRECONDITION, got %v", err)
		}
		r3, err := refund("r3", 600)
		if err != nil || r3.Charge.Status != billingv1.ChargeStatus_CHARGE_STATUS_REFUNDED {
			t.Fatalf("final refund: %+v %v", r3, err)
		}
		if _, err := refund("r4", 1); code(err) != codes.FailedPrecondition {
			t.Fatalf("refund of a fully refunded charge: %v", err)
		}
		bal, _ := f.svc.GetBalance(ctx, &billingv1.GetBalanceRequest{CustomerId: f.cust.Id})
		if bal.NetCollected[0].AmountMinor != 0 {
			t.Fatalf("net collected %d after full refund", bal.NetCollected[0].AmountMinor)
		}
		f.assertBalanced()
	})
}

// Concurrent refunds with different keys must never refund more than was charged.
func TestConcurrentRefundsCannotExceedCharge(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store) {
		f := newFixture(t, st)
		ctx := context.Background()
		ch, err := f.svc.ChargeInvoice(ctx, f.chargeReq("c1", 1000))
		if err != nil {
			t.Fatal(err)
		}
		const n = 20 // 20 x 100 attempted against 1000 refundable
		var wg sync.WaitGroup
		var ok atomic.Int32
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := f.svc.RefundCharge(ctx, &billingv1.RefundChargeRequest{IdempotencyKey: fmt.Sprintf("r-%d", i), ChargeId: ch.Charge.Id, Amount: usd(100)})
				switch code(err) {
				case codes.OK:
					ok.Add(1)
				case codes.FailedPrecondition:
				default:
					t.Errorf("unexpected error %v", err)
				}
			}()
		}
		wg.Wait()
		if ok.Load() != 10 {
			t.Fatalf("%d refunds succeeded, want exactly 10", ok.Load())
		}
		bal, _ := f.svc.GetBalance(ctx, &billingv1.GetBalanceRequest{CustomerId: f.cust.Id})
		if bal.NetCollected[0].AmountMinor != 0 {
			t.Fatalf("net collected %d, want 0", bal.NetCollected[0].AmountMinor)
		}
		f.assertBalanced()
	})
}

func TestDeclinedChargeIsRecordedAndReplayed(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store) {
		f := newFixture(t, st)
		ctx := context.Background()
		// Activate first so a decline moves the subscription to PAST_DUE.
		if _, err := f.svc.ChargeInvoice(ctx, f.chargeReq("ok", 500)); err != nil {
			t.Fatal(err)
		}
		req := f.chargeReq("bad", 500)
		req.PaymentMethodId = service.DeclineMethod
		r, err := f.svc.ChargeInvoice(ctx, req)
		if err != nil || r.Charge.Status != billingv1.ChargeStatus_CHARGE_STATUS_DECLINED || r.Charge.DeclineReason == "" {
			t.Fatalf("%+v %v", r, err)
		}
		again, err := f.svc.ChargeInvoice(ctx, req)
		if err != nil || !again.IdempotentReplay || again.Charge.Id != r.Charge.Id {
			t.Fatalf("decline was not replayed: %+v %v", again, err)
		}
		sub, _ := f.svc.GetSubscription(ctx, &billingv1.GetSubscriptionRequest{SubscriptionId: f.sub.Id})
		if sub.Subscription.State != billingv1.SubscriptionState_SUBSCRIPTION_STATE_PAST_DUE {
			t.Fatalf("state %v, want PAST_DUE", sub.Subscription.State)
		}
		if n := len(f.ledger()); n != 2 {
			t.Fatalf("declined charge posted ledger entries: %d total", n)
		}
		// A later successful payment recovers the subscription.
		if _, err := f.svc.ChargeInvoice(ctx, f.chargeReq("retry", 500)); err != nil {
			t.Fatal(err)
		}
		sub, _ = f.svc.GetSubscription(ctx, &billingv1.GetSubscriptionRequest{SubscriptionId: f.sub.Id})
		if sub.Subscription.State != billingv1.SubscriptionState_SUBSCRIPTION_STATE_ACTIVE {
			t.Fatalf("state %v, want ACTIVE", sub.Subscription.State)
		}
		// Refunding a declined charge is not allowed.
		_, err = f.svc.RefundCharge(ctx, &billingv1.RefundChargeRequest{IdempotencyKey: "rf", ChargeId: r.Charge.Id, Amount: usd(1)})
		if code(err) != codes.FailedPrecondition {
			t.Fatalf("refund of declined charge: %v", err)
		}
		f.assertBalanced()
	})
}

func TestCancelSubscription(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store) {
		f := newFixture(t, st)
		ctx := context.Background()
		c1, err := f.svc.CancelSubscription(ctx, &billingv1.CancelSubscriptionRequest{SubscriptionId: f.sub.Id})
		if err != nil || c1.Subscription.State != billingv1.SubscriptionState_SUBSCRIPTION_STATE_CANCELED || c1.Subscription.CancelTime == nil {
			t.Fatalf("%+v %v", c1, err)
		}
		c2, err := f.svc.CancelSubscription(ctx, &billingv1.CancelSubscriptionRequest{SubscriptionId: f.sub.Id})
		if err != nil || !c2.Subscription.CancelTime.AsTime().Equal(c1.Subscription.CancelTime.AsTime()) {
			t.Fatalf("second cancel must be a no-op: %+v %v", c2, err)
		}
		_, err = f.svc.ChargeInvoice(ctx, f.chargeReq("after-cancel", 100))
		if code(err) != codes.FailedPrecondition {
			t.Fatalf("charge on canceled subscription: %v", err)
		}
		_, err = f.svc.CancelSubscription(ctx, &billingv1.CancelSubscriptionRequest{SubscriptionId: "missing"})
		if code(err) != codes.NotFound {
			t.Fatalf("want NOT_FOUND, got %v", err)
		}
	})
}

func TestValidationAndErrors(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store) {
		f := newFixture(t, st)
		ctx := context.Background()
		charge := func(mut func(*billingv1.ChargeInvoiceRequest)) error {
			r := f.chargeReq("v", 100)
			mut(r)
			_, err := f.svc.ChargeInvoice(ctx, r)
			return err
		}
		tests := []struct {
			name string
			err  error
			want codes.Code
		}{
			{"missing key", charge(func(r *billingv1.ChargeInvoiceRequest) { r.IdempotencyKey = "" }), codes.InvalidArgument},
			{"key with space", charge(func(r *billingv1.ChargeInvoiceRequest) { r.IdempotencyKey = "a b" }), codes.InvalidArgument},
			{"key too long", charge(func(r *billingv1.ChargeInvoiceRequest) { r.IdempotencyKey = string(make([]byte, 256)) }), codes.InvalidArgument},
			{"zero amount", charge(func(r *billingv1.ChargeInvoiceRequest) { r.Amount = usd(0) }), codes.InvalidArgument},
			{"negative amount", charge(func(r *billingv1.ChargeInvoiceRequest) { r.Amount = usd(-5) }), codes.InvalidArgument},
			{"nil amount", charge(func(r *billingv1.ChargeInvoiceRequest) { r.Amount = nil }), codes.InvalidArgument},
			{"bad currency", charge(func(r *billingv1.ChargeInvoiceRequest) { r.Amount.CurrencyCode = "dollars" }), codes.InvalidArgument},
			{"currency mismatch", charge(func(r *billingv1.ChargeInvoiceRequest) { r.Amount.CurrencyCode = "EUR" }), codes.FailedPrecondition},
			{"unknown subscription", charge(func(r *billingv1.ChargeInvoiceRequest) { r.SubscriptionId = "nope" }), codes.NotFound},
			{"wrong customer", charge(func(r *billingv1.ChargeInvoiceRequest) { r.CustomerId = "someone-else" }), codes.InvalidArgument},
			{"missing payment method", charge(func(r *billingv1.ChargeInvoiceRequest) { r.PaymentMethodId = "" }), codes.InvalidArgument},
		}
		for _, tt := range tests {
			if code(tt.err) != tt.want {
				t.Errorf("%s: got %v, want code %v", tt.name, tt.err, tt.want)
			}
		}

		_, err := f.svc.CreateCustomer(ctx, &billingv1.CreateCustomerRequest{Email: "not-an-email"})
		if code(err) != codes.InvalidArgument {
			t.Errorf("bad email: %v", err)
		}
		_, err = f.svc.CreateCustomer(ctx, &billingv1.CreateCustomerRequest{Email: f.cust.Email})
		if code(err) != codes.AlreadyExists {
			t.Errorf("duplicate email: %v", err)
		}
		_, err = f.svc.CreateSubscription(ctx, &billingv1.CreateSubscriptionRequest{CustomerId: "nope", PlanId: "p", Price: usd(1), Interval: billingv1.BillingInterval_BILLING_INTERVAL_YEARLY})
		if code(err) != codes.NotFound {
			t.Errorf("unknown customer: %v", err)
		}
		_, err = f.svc.CreateSubscription(ctx, &billingv1.CreateSubscriptionRequest{CustomerId: f.cust.Id, PlanId: "p", Price: usd(1)})
		if code(err) != codes.InvalidArgument {
			t.Errorf("missing interval: %v", err)
		}
		_, err = f.svc.RefundCharge(ctx, &billingv1.RefundChargeRequest{IdempotencyKey: "x", ChargeId: "nope", Amount: usd(1)})
		if code(err) != codes.NotFound {
			t.Errorf("refund unknown charge: %v", err)
		}
		_, err = f.svc.GetLedger(ctx, &billingv1.GetLedgerRequest{CustomerId: f.cust.Id, PageToken: "!!"})
		if code(err) != codes.InvalidArgument {
			t.Errorf("bad page token: %v", err)
		}
		_, err = f.svc.GetBalance(ctx, &billingv1.GetBalanceRequest{CustomerId: "nope"})
		if code(err) != codes.NotFound {
			t.Errorf("balance unknown customer: %v", err)
		}
	})
}

func TestLedgerPagination(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store) {
		f := newFixture(t, st)
		ctx := context.Background()
		for i := 0; i < 5; i++ {
			if _, err := f.svc.ChargeInvoice(ctx, f.chargeReq(fmt.Sprintf("p%d", i), int64(100+i))); err != nil {
				t.Fatal(err)
			}
		}
		seen := map[string]bool{}
		tok := ""
		pages := 0
		for {
			r, err := f.svc.GetLedger(ctx, &billingv1.GetLedgerRequest{CustomerId: f.cust.Id, PageSize: 4, PageToken: tok})
			if err != nil {
				t.Fatal(err)
			}
			pages++
			for _, e := range r.Entries {
				if seen[e.Id] {
					t.Fatalf("duplicate entry %s across pages", e.Id)
				}
				seen[e.Id] = true
			}
			if tok = r.NextPageToken; tok == "" {
				break
			}
		}
		if len(seen) != 10 || pages != 3 {
			t.Fatalf("saw %d entries over %d pages, want 10 over 3", len(seen), pages)
		}
	})
}

// failingStore makes AppendLedger fail so we can prove a charge and its
// ledger entries commit or fail together.
type failingStore struct {
	store.Store
	fail atomic.Bool
}

type failingTx struct {
	store.Tx
	fs *failingStore
}

func (f *failingStore) ReadWrite(ctx context.Context, fn func(store.Tx) error) error {
	return f.Store.ReadWrite(ctx, func(tx store.Tx) error { return fn(&failingTx{Tx: tx, fs: f}) })
}

func (t *failingTx) AppendLedger(e []billing.LedgerEntry) error {
	if t.fs.fail.Load() {
		return errors.New("injected ledger failure")
	}
	return t.Tx.AppendLedger(e)
}

func TestChargeIsAtomicWithLedgerAndKey(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store) {
		fs := &failingStore{Store: st}
		f := newFixture(t, fs)
		ctx := context.Background()
		fs.fail.Store(true)
		_, err := f.svc.ChargeInvoice(ctx, f.chargeReq("atomic", 1999))
		if code(err) != codes.Internal {
			t.Fatalf("want INTERNAL, got %v", err)
		}
		// Nothing from the failed attempt may be visible: no ledger, still PENDING.
		if n := len(f.ledger()); n != 0 {
			t.Fatalf("%d ledger entries leaked", n)
		}
		sub, _ := f.svc.GetSubscription(ctx, &billingv1.GetSubscriptionRequest{SubscriptionId: f.sub.Id})
		if sub.Subscription.State != billingv1.SubscriptionState_SUBSCRIPTION_STATE_PENDING {
			t.Fatalf("subscription advanced to %v by a failed charge", sub.Subscription.State)
		}
		// The key was not consumed: retrying with it now succeeds as a fresh charge.
		fs.fail.Store(false)
		r, err := f.svc.ChargeInvoice(ctx, f.chargeReq("atomic", 1999))
		if err != nil || r.IdempotentReplay {
			t.Fatalf("retry after failure: %+v %v", r, err)
		}
		f.assertBalanced()
	})
}

func TestDeadlineHonored(t *testing.T) {
	st := memory.New()
	f := newFixture(t, st)
	// Hold the store's write lock so the request can only wait.
	release := make(chan struct{})
	held := make(chan struct{})
	go func() {
		_ = st.ReadWrite(context.Background(), func(store.Tx) error { close(held); <-release; return nil })
	}()
	<-held
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := f.svc.ChargeInvoice(ctx, f.chargeReq("slow", 100))
	if code(err) != codes.DeadlineExceeded {
		t.Fatalf("want DEADLINE_EXCEEDED, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("deadline not honored promptly")
	}
}
