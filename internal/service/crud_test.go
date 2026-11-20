package service_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	billingv1 "github.com/Rishi-creator-11/billing-service/gen/billing/v1"
	"github.com/Rishi-creator-11/billing-service/internal/service"
	"github.com/Rishi-creator-11/billing-service/internal/store"
)

func mask(paths ...string) *fieldmaskpb.FieldMask { return &fieldmaskpb.FieldMask{Paths: paths} }

func reason(err error) string {
	for _, d := range status.Convert(err).Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info.Reason
		}
	}
	return ""
}

func monthly() billingv1.BillingInterval { return billingv1.BillingInterval_BILLING_INTERVAL_MONTHLY }

func newCustomer(t *testing.T, svc *service.Service, email string) *billingv1.Customer {
	t.Helper()
	r, err := svc.CreateCustomer(context.Background(), &billingv1.CreateCustomerRequest{Email: email, Name: "N"})
	if err != nil {
		t.Fatal(err)
	}
	return r.Customer
}

func TestGetCustomer(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store) {
		f := newFixture(t, st)
		got, err := f.svc.GetCustomer(context.Background(), &billingv1.GetCustomerRequest{CustomerId: f.cust.Id})
		if err != nil || got.Customer.Id != f.cust.Id || got.Customer.Email != f.cust.Email || got.Customer.Name != "Test" || got.Customer.CreateTime == nil {
			t.Fatalf("got %+v err %v", got, err)
		}
		_, err = f.svc.GetCustomer(context.Background(), &billingv1.GetCustomerRequest{CustomerId: "missing"})
		if msg := status.Convert(err).Message(); msg != `customer "missing" not found` {
			t.Errorf("error message %q leaks store internals", msg)
		}
		for _, id := range []string{"missing", ""} {
			if _, err := f.svc.GetCustomer(context.Background(), &billingv1.GetCustomerRequest{CustomerId: id}); code(err) != codes.NotFound {
				t.Errorf("id %q: want NOT_FOUND, got %v", id, err)
			}
		}
	})
}

func TestUpdateCustomer(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store) {
		f := newFixture(t, st)
		ctx := context.Background()
		other := newCustomer(t, f.svc, "other@example.com")
		upd := func(r *billingv1.UpdateCustomerRequest) (*billingv1.UpdateCustomerResponse, error) {
			r.CustomerId = f.cust.Id
			return f.svc.UpdateCustomer(ctx, r)
		}

		// Only masked fields change.
		r, err := upd(&billingv1.UpdateCustomerRequest{Name: "Renamed", Email: "ignored@example.com", UpdateMask: mask("name")})
		if err != nil || r.Customer.Name != "Renamed" || r.Customer.Email != f.cust.Email {
			t.Fatalf("name-only update: %+v %v", r, err)
		}
		r, err = upd(&billingv1.UpdateCustomerRequest{Email: "New.Address@Example.com", UpdateMask: mask("email")})
		if err != nil || r.Customer.Email != "new.address@example.com" || r.Customer.Name != "Renamed" {
			t.Fatalf("email-only update: %+v %v", r, err)
		}
		r, err = upd(&billingv1.UpdateCustomerRequest{Name: "", Email: "both@example.com", UpdateMask: mask("name", "email")})
		if err != nil || r.Customer.Name != "" || r.Customer.Email != "both@example.com" {
			t.Fatalf("clearing the name: %+v %v", r, err)
		}
		if !r.Customer.CreateTime.AsTime().Equal(f.cust.CreateTime.AsTime()) || r.Customer.Id != f.cust.Id {
			t.Fatal("update changed id or create_time")
		}
		got, _ := f.svc.GetCustomer(ctx, &billingv1.GetCustomerRequest{CustomerId: f.cust.Id})
		if got.Customer.Email != "both@example.com" {
			t.Fatalf("update was not persisted: %+v", got.Customer)
		}
		// The previous email is free for someone else.
		newCustomer(t, f.svc, f.cust.Email)

		// Validation.
		bad := []struct {
			name string
			req  *billingv1.UpdateCustomerRequest
			want codes.Code
		}{
			{"no mask", &billingv1.UpdateCustomerRequest{Name: "x"}, codes.InvalidArgument},
			{"unknown path", &billingv1.UpdateCustomerRequest{Name: "x", UpdateMask: mask("id")}, codes.InvalidArgument},
			{"create_time path", &billingv1.UpdateCustomerRequest{UpdateMask: mask("create_time")}, codes.InvalidArgument},
			{"bad email", &billingv1.UpdateCustomerRequest{Email: "nope", UpdateMask: mask("email")}, codes.InvalidArgument},
			{"empty email", &billingv1.UpdateCustomerRequest{UpdateMask: mask("email")}, codes.InvalidArgument},
			{"name too long", &billingv1.UpdateCustomerRequest{Name: strings.Repeat("n", 256), UpdateMask: mask("name")}, codes.InvalidArgument},
			{"email taken", &billingv1.UpdateCustomerRequest{Email: other.Email, UpdateMask: mask("email")}, codes.AlreadyExists},
		}
		for _, tt := range bad {
			t.Run(tt.name, func(t *testing.T) {
				if _, err := upd(tt.req); code(err) != tt.want {
					t.Fatalf("got %v, want %v", err, tt.want)
				}
			})
		}
		_, err = upd(&billingv1.UpdateCustomerRequest{Email: strings.ToUpper(other.Email), UpdateMask: mask("email")})
		if code(err) != codes.AlreadyExists || reason(err) != "CUSTOMER_EXISTS" {
			t.Fatalf("case-insensitive duplicate: %v reason %q", err, reason(err))
		}
		// A rejected update leaves the customer unchanged.
		got, _ = f.svc.GetCustomer(ctx, &billingv1.GetCustomerRequest{CustomerId: f.cust.Id})
		if got.Customer.Email != "both@example.com" {
			t.Fatalf("rejected update changed the customer: %+v", got.Customer)
		}

		// Setting your own email again is not a conflict.
		if _, err := upd(&billingv1.UpdateCustomerRequest{Email: "both@example.com", UpdateMask: mask("email")}); err != nil {
			t.Fatalf("same email: %v", err)
		}
		_, err = f.svc.UpdateCustomer(ctx, &billingv1.UpdateCustomerRequest{CustomerId: "missing", Name: "x", UpdateMask: mask("name")})
		if code(err) != codes.NotFound {
			t.Fatalf("missing customer: %v", err)
		}
	})
}

func TestDeleteCustomer(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store) {
		f := newFixture(t, st) // has a PENDING subscription
		ctx := context.Background()
		del := func(id string) error {
			_, err := f.svc.DeleteCustomer(ctx, &billingv1.DeleteCustomerRequest{CustomerId: id})
			return err
		}
		stillThere := func(id string) {
			t.Helper()
			if _, err := f.svc.GetCustomer(ctx, &billingv1.GetCustomerRequest{CustomerId: id}); err != nil {
				t.Fatalf("customer %s should still exist: %v", id, err)
			}
		}

		// A customer with a subscription is protected, whatever the subscription's state.
		err := del(f.cust.Id)
		if code(err) != codes.FailedPrecondition || reason(err) != "CUSTOMER_HAS_ACTIVITY" {
			t.Fatalf("pending subscription: %v reason %q", err, reason(err))
		}
		if _, err := f.svc.CancelSubscription(ctx, &billingv1.CancelSubscriptionRequest{SubscriptionId: f.sub.Id}); err != nil {
			t.Fatal(err)
		}
		if err := del(f.cust.Id); code(err) != codes.FailedPrecondition {
			t.Fatalf("canceled subscription: %v", err)
		}
		stillThere(f.cust.Id)

		// So is a customer with ledger entries.
		paid := newFixture(t, st)
		if _, err := paid.svc.ChargeInvoice(ctx, paid.chargeReq("del-1", 500)); err != nil {
			t.Fatal(err)
		}
		if err := del(paid.cust.Id); code(err) != codes.FailedPrecondition || reason(err) != "CUSTOMER_HAS_ACTIVITY" {
			t.Fatalf("customer with ledger entries: %v", err)
		}
		stillThere(paid.cust.Id)
		paid.assertBalanced()

		// A customer with no dependants is deleted and its email is released.
		lone := newCustomer(t, f.svc, "lone@example.com")
		if err := del(lone.Id); err != nil {
			t.Fatal(err)
		}
		if _, err := f.svc.GetCustomer(ctx, &billingv1.GetCustomerRequest{CustomerId: lone.Id}); code(err) != codes.NotFound {
			t.Fatalf("deleted customer still readable: %v", err)
		}
		if err := del(lone.Id); code(err) != codes.NotFound {
			t.Fatalf("second delete: %v", err)
		}
		if err := del("missing"); code(err) != codes.NotFound {
			t.Fatalf("unknown id: %v", err)
		}
		newCustomer(t, f.svc, "lone@example.com")
		if _, err := f.svc.GetLedger(ctx, &billingv1.GetLedgerRequest{CustomerId: lone.Id}); code(err) != codes.NotFound {
			t.Fatalf("ledger of a deleted customer: %v", err)
		}
	})
}

func TestListCustomers(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store) {
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		tick := 0
		svc := service.New(st, service.WithClock(func() time.Time { tick++; return now.Add(time.Duration(tick/2) * time.Second) })) // pairs share a timestamp
		ctx := context.Background()
		var want []string
		for i := 0; i < 7; i++ {
			want = append(want, newCustomer(t, svc, fmt.Sprintf("list%d@example.com", i)).Id)
		}

		// Walk with page size 3: 3 + 3 + 1, every customer exactly once.
		var seen []string
		var sizes []int
		tok := ""
		for {
			r, err := svc.ListCustomers(ctx, &billingv1.ListCustomersRequest{PageSize: 3, PageToken: tok})
			if err != nil {
				t.Fatal(err)
			}
			sizes = append(sizes, len(r.Customers))
			for _, c := range r.Customers {
				seen = append(seen, c.Id)
			}
			if tok = r.NextPageToken; tok == "" {
				break
			}
		}
		if fmt.Sprint(sizes) != "[3 3 1]" {
			t.Fatalf("page sizes %v, want [3 3 1]", sizes)
		}
		if len(seen) != 7 {
			t.Fatalf("saw %d customers", len(seen))
		}
		got := map[string]bool{}
		for _, id := range seen {
			got[id] = true
		}
		for _, id := range want {
			if !got[id] {
				t.Fatalf("customer %s missing from the listing", id)
			}
		}

		// Exactly one page's worth: no dangling token.
		r, err := svc.ListCustomers(ctx, &billingv1.ListCustomersRequest{PageSize: 7})
		if err != nil || len(r.Customers) != 7 || r.NextPageToken != "" {
			t.Fatalf("full page: %d customers, token %q, err %v", len(r.Customers), r.NextPageToken, err)
		}
		r, err = svc.ListCustomers(ctx, &billingv1.ListCustomersRequest{}) // default page size
		if err != nil || len(r.Customers) != 7 || r.NextPageToken != "" {
			t.Fatalf("default page: %d customers, err %v", len(r.Customers), err)
		}

		for name, req := range map[string]*billingv1.ListCustomersRequest{
			"garbage token":                 {PageToken: "!!"},
			"valid base64 but not a cursor": {PageToken: "bm90LWEtY3Vyc29y"},
			"negative size":                 {PageSize: -1},
			"size too large":                {PageSize: 501},
		} {
			if _, err := svc.ListCustomers(ctx, req); code(err) != codes.InvalidArgument {
				t.Errorf("%s: got %v, want INVALID_ARGUMENT", name, err)
			}
		}
	})
}

func TestListCustomersEmpty(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store) {
		r, err := service.New(st).ListCustomers(context.Background(), &billingv1.ListCustomersRequest{})
		if err != nil || len(r.Customers) != 0 || r.NextPageToken != "" {
			t.Fatalf("%+v %v", r, err)
		}
	})
}

func TestUpdateSubscription(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store) {
		f := newFixture(t, st)
		ctx := context.Background()
		upd := func(r *billingv1.UpdateSubscriptionRequest) (*billingv1.UpdateSubscriptionResponse, error) {
			r.SubscriptionId = f.sub.Id
			return f.svc.UpdateSubscription(ctx, r)
		}

		r, err := upd(&billingv1.UpdateSubscriptionRequest{PlanId: "enterprise", UpdateMask: mask("plan_id")})
		if err != nil || r.Subscription.PlanId != "enterprise" || len(r.Subscription.Metadata) != 0 {
			t.Fatalf("plan update: %+v %v", r, err)
		}
		r, err = upd(&billingv1.UpdateSubscriptionRequest{PlanId: "ignored", Metadata: map[string]string{"team": "core", "seats": "5"}, UpdateMask: mask("metadata")})
		if err != nil || r.Subscription.PlanId != "enterprise" || len(r.Subscription.Metadata) != 2 || r.Subscription.Metadata["team"] != "core" {
			t.Fatalf("metadata update: %+v %v", r, err)
		}
		// Metadata is replaced, not merged.
		r, err = upd(&billingv1.UpdateSubscriptionRequest{Metadata: map[string]string{"region": "eu"}, UpdateMask: mask("metadata")})
		if err != nil || len(r.Subscription.Metadata) != 1 || r.Subscription.Metadata["region"] != "eu" {
			t.Fatalf("metadata replace: %+v %v", r, err)
		}
		if r.Subscription.UpdateTime.AsTime().Before(f.sub.UpdateTime.AsTime()) {
			t.Fatal("update_time went backwards")
		}
		// Immutable fields and state are untouched.
		s := r.Subscription
		if s.State != billingv1.SubscriptionState_SUBSCRIPTION_STATE_PENDING || s.Price.AmountMinor != 1999 || s.Interval != monthly() || s.CustomerId != f.cust.Id {
			t.Fatalf("update changed protected fields: %+v", s)
		}
		got, _ := f.svc.GetSubscription(ctx, &billingv1.GetSubscriptionRequest{SubscriptionId: f.sub.Id})
		if got.Subscription.PlanId != "enterprise" || got.Subscription.Metadata["region"] != "eu" {
			t.Fatalf("not persisted: %+v", got.Subscription)
		}
		r, err = upd(&billingv1.UpdateSubscriptionRequest{UpdateMask: mask("metadata")}) // clear
		if err != nil || len(r.Subscription.Metadata) != 0 {
			t.Fatalf("clear metadata: %+v %v", r, err)
		}

		// A paid subscription keeps its state through an update.
		if _, err := f.svc.ChargeInvoice(ctx, f.chargeReq("upd-1", 1999)); err != nil {
			t.Fatal(err)
		}
		r, err = upd(&billingv1.UpdateSubscriptionRequest{PlanId: "pro-plus", UpdateMask: mask("plan_id")})
		if err != nil || r.Subscription.State != billingv1.SubscriptionState_SUBSCRIPTION_STATE_ACTIVE {
			t.Fatalf("update of an active subscription: %+v %v", r, err)
		}
		f.assertBalanced()

		manyKeys := map[string]string{}
		for i := 0; i < 21; i++ {
			manyKeys[fmt.Sprint("k", i)] = "v"
		}
		bad := []struct {
			name string
			req  *billingv1.UpdateSubscriptionRequest
		}{
			{"no mask", &billingv1.UpdateSubscriptionRequest{PlanId: "x"}},
			{"price is not updatable", &billingv1.UpdateSubscriptionRequest{UpdateMask: mask("price")}},
			{"state is not updatable", &billingv1.UpdateSubscriptionRequest{UpdateMask: mask("state")}},
			{"interval is not updatable", &billingv1.UpdateSubscriptionRequest{UpdateMask: mask("interval")}},
			{"empty plan", &billingv1.UpdateSubscriptionRequest{UpdateMask: mask("plan_id")}},
			{"plan too long", &billingv1.UpdateSubscriptionRequest{PlanId: strings.Repeat("p", 129), UpdateMask: mask("plan_id")}},
			{"too many metadata entries", &billingv1.UpdateSubscriptionRequest{Metadata: manyKeys, UpdateMask: mask("metadata")}},
			{"empty metadata key", &billingv1.UpdateSubscriptionRequest{Metadata: map[string]string{"": "v"}, UpdateMask: mask("metadata")}},
			{"metadata key too long", &billingv1.UpdateSubscriptionRequest{Metadata: map[string]string{strings.Repeat("k", 65): "v"}, UpdateMask: mask("metadata")}},
			{"metadata value too long", &billingv1.UpdateSubscriptionRequest{Metadata: map[string]string{"k": strings.Repeat("v", 257)}, UpdateMask: mask("metadata")}},
		}
		for _, tt := range bad {
			t.Run(tt.name, func(t *testing.T) {
				if _, err := upd(tt.req); code(err) != codes.InvalidArgument {
					t.Fatalf("got %v, want INVALID_ARGUMENT", err)
				}
			})
		}
		if _, err := f.svc.UpdateSubscription(ctx, &billingv1.UpdateSubscriptionRequest{SubscriptionId: "missing", PlanId: "x", UpdateMask: mask("plan_id")}); code(err) != codes.NotFound {
			t.Fatalf("missing subscription: %v", err)
		}

		// A canceled subscription is frozen.
		if _, err := f.svc.CancelSubscription(ctx, &billingv1.CancelSubscriptionRequest{SubscriptionId: f.sub.Id}); err != nil {
			t.Fatal(err)
		}
		_, err = upd(&billingv1.UpdateSubscriptionRequest{PlanId: "late", UpdateMask: mask("plan_id")})
		if code(err) != codes.FailedPrecondition || reason(err) != "SUBSCRIPTION_CANCELED" {
			t.Fatalf("update after cancel: %v reason %q", err, reason(err))
		}
		got, _ = f.svc.GetSubscription(ctx, &billingv1.GetSubscriptionRequest{SubscriptionId: f.sub.Id})
		if got.Subscription.PlanId != "pro-plus" {
			t.Fatalf("rejected update changed the plan: %q", got.Subscription.PlanId)
		}
	})
}

func TestListSubscriptions(t *testing.T) {
	forEachStore(t, func(t *testing.T, st store.Store) {
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		tick := 0
		svc := service.New(st, service.WithClock(func() time.Time { tick++; return now.Add(time.Duration(tick) * time.Millisecond) }))
		ctx := context.Background()
		a, b, empty := newCustomer(t, svc, "a@example.com"), newCustomer(t, svc, "b@example.com"), newCustomer(t, svc, "empty@example.com")
		sub := func(cust *billingv1.Customer, plan string) string {
			r, err := svc.CreateSubscription(ctx, &billingv1.CreateSubscriptionRequest{CustomerId: cust.Id, PlanId: plan, Price: usd(100), Interval: monthly()})
			if err != nil {
				t.Fatal(err)
			}
			return r.Subscription.Id
		}
		var aIDs []string
		for i := 0; i < 5; i++ {
			aIDs = append(aIDs, sub(a, fmt.Sprint("a", i)))
			sub(b, fmt.Sprint("b", i))
		}

		list := func(req *billingv1.ListSubscriptionsRequest) (ids []string, pages int) {
			t.Helper()
			for {
				r, err := svc.ListSubscriptions(ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				pages++
				for _, s := range r.Subscriptions {
					ids = append(ids, s.Id)
				}
				if req.PageToken = r.NextPageToken; req.PageToken == "" {
					return
				}
			}
		}

		ids, pages := list(&billingv1.ListSubscriptionsRequest{CustomerId: a.Id, PageSize: 2})
		if fmt.Sprint(ids) != fmt.Sprint(aIDs) || pages != 3 {
			t.Fatalf("customer a: %v in %d pages, want %v in 3", ids, pages, aIDs)
		}
		if ids, _ := list(&billingv1.ListSubscriptionsRequest{}); len(ids) != 10 {
			t.Fatalf("unfiltered listing has %d subscriptions, want 10", len(ids))
		}
		if ids, pages := list(&billingv1.ListSubscriptionsRequest{CustomerId: empty.Id}); len(ids) != 0 || pages != 1 {
			t.Fatalf("customer without subscriptions: %v", ids)
		}
		r, _ := svc.ListSubscriptions(ctx, &billingv1.ListSubscriptionsRequest{CustomerId: a.Id, PageSize: 5})
		if r.NextPageToken != "" {
			t.Fatal("a page that ends exactly at the last row must not return a token")
		}

		// Listing reflects updates and cancellation.
		if _, err := svc.CancelSubscription(ctx, &billingv1.CancelSubscriptionRequest{SubscriptionId: aIDs[0]}); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.UpdateSubscription(ctx, &billingv1.UpdateSubscriptionRequest{SubscriptionId: aIDs[1], Metadata: map[string]string{"k": "v"}, UpdateMask: mask("metadata")}); err != nil {
			t.Fatal(err)
		}
		r, _ = svc.ListSubscriptions(ctx, &billingv1.ListSubscriptionsRequest{CustomerId: a.Id})
		if r.Subscriptions[0].State != billingv1.SubscriptionState_SUBSCRIPTION_STATE_CANCELED || r.Subscriptions[1].Metadata["k"] != "v" {
			t.Fatalf("listing is stale: %+v", r.Subscriptions[:2])
		}

		for name, req := range map[string]*billingv1.ListSubscriptionsRequest{
			"garbage token":  {PageToken: "!!"},
			"negative size":  {PageSize: -3},
			"size too large": {PageSize: 1000},
		} {
			if _, err := svc.ListSubscriptions(ctx, req); code(err) != codes.InvalidArgument {
				t.Errorf("%s: got %v, want INVALID_ARGUMENT", name, err)
			}
		}
		if _, err := svc.ListSubscriptions(ctx, &billingv1.ListSubscriptionsRequest{CustomerId: "missing"}); code(err) != codes.NotFound {
			t.Errorf("unknown customer: %v", err)
		}
	})
}
