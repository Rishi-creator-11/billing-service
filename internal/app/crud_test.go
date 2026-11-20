package app_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	billingv1 "github.com/Rishi-creator-11/billing-service/gen/billing/v1"
)

func mask(paths ...string) *fieldmaskpb.FieldMask { return &fieldmaskpb.FieldMask{Paths: paths} }

func TestCRUDOverGRPC(t *testing.T) {
	srv, _ := start(t)
	g, _, _ := srv.Addrs()
	cc := dial(t, g)
	customers := billingv1.NewCustomerServiceClient(cc)
	subs := billingv1.NewSubscriptionServiceClient(cc)
	billingClient := billingv1.NewBillingServiceClient(cc)
	ctx := authed()

	// Every method on the new services requires the API key.
	anon := context.Background()
	for name, err := range map[string]error{
		"CreateCustomer": func() error { _, e := customers.CreateCustomer(anon, &billingv1.CreateCustomerRequest{}); return e }(),
		"GetCustomer":    func() error { _, e := customers.GetCustomer(anon, &billingv1.GetCustomerRequest{}); return e }(),
		"UpdateCustomer": func() error { _, e := customers.UpdateCustomer(anon, &billingv1.UpdateCustomerRequest{}); return e }(),
		"DeleteCustomer": func() error { _, e := customers.DeleteCustomer(anon, &billingv1.DeleteCustomerRequest{}); return e }(),
		"ListCustomers":  func() error { _, e := customers.ListCustomers(anon, &billingv1.ListCustomersRequest{}); return e }(),
		"CreateSubscription": func() error {
			_, e := subs.CreateSubscription(anon, &billingv1.CreateSubscriptionRequest{})
			return e
		}(),
		"GetSubscription": func() error { _, e := subs.GetSubscription(anon, &billingv1.GetSubscriptionRequest{}); return e }(),
		"UpdateSubscription": func() error {
			_, e := subs.UpdateSubscription(anon, &billingv1.UpdateSubscriptionRequest{})
			return e
		}(),
		"ListSubscriptions": func() error { _, e := subs.ListSubscriptions(anon, &billingv1.ListSubscriptionsRequest{}); return e }(),
		"CancelSubscription": func() error {
			_, e := subs.CancelSubscription(anon, &billingv1.CancelSubscriptionRequest{})
			return e
		}(),
	} {
		if status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s without a key: %v", name, err)
		}
	}

	// Customer lifecycle.
	created, err := customers.CreateCustomer(ctx, &billingv1.CreateCustomerRequest{Email: "crud@example.com", Name: "Crud"})
	if err != nil {
		t.Fatal(err)
	}
	cid := created.Customer.Id
	if got, err := customers.GetCustomer(ctx, &billingv1.GetCustomerRequest{CustomerId: cid}); err != nil || got.Customer.Email != "crud@example.com" {
		t.Fatalf("get: %+v %v", got, err)
	}
	if _, err := customers.GetCustomer(ctx, &billingv1.GetCustomerRequest{CustomerId: "missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("get missing: %v", err)
	}
	upd, err := customers.UpdateCustomer(ctx, &billingv1.UpdateCustomerRequest{CustomerId: cid, Name: "Crud Two", UpdateMask: mask("name")})
	if err != nil || upd.Customer.Name != "Crud Two" || upd.Customer.Email != "crud@example.com" {
		t.Fatalf("update: %+v %v", upd, err)
	}
	if _, err := customers.UpdateCustomer(ctx, &billingv1.UpdateCustomerRequest{CustomerId: cid, Name: "x"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("update without mask: %v", err)
	}
	if _, err := customers.CreateCustomer(ctx, &billingv1.CreateCustomerRequest{Email: "second@example.com"}); err != nil {
		t.Fatal(err)
	}
	if _, err := customers.UpdateCustomer(ctx, &billingv1.UpdateCustomerRequest{CustomerId: cid, Email: "second@example.com", UpdateMask: mask("email")}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate email: %v", err)
	}

	// A customer created through the legacy BillingService RPC is the same entity.
	legacy, err := billingClient.CreateCustomer(ctx, &billingv1.CreateCustomerRequest{Email: "legacy@example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := customers.GetCustomer(ctx, &billingv1.GetCustomerRequest{CustomerId: legacy.Customer.Id}); err != nil || got.Customer.Email != "legacy@example.com" {
		t.Fatalf("legacy customer via CustomerService: %+v %v", got, err)
	}

	list, err := customers.ListCustomers(ctx, &billingv1.ListCustomersRequest{PageSize: 2})
	if err != nil || len(list.Customers) != 2 || list.NextPageToken == "" {
		t.Fatalf("list page 1: %+v %v", list, err)
	}
	list, err = customers.ListCustomers(ctx, &billingv1.ListCustomersRequest{PageSize: 2, PageToken: list.NextPageToken})
	if err != nil || len(list.Customers) != 1 || list.NextPageToken != "" {
		t.Fatalf("list page 2: %+v %v", list, err)
	}

	// Subscription lifecycle.
	sub, err := subs.CreateSubscription(ctx, &billingv1.CreateSubscriptionRequest{CustomerId: cid, PlanId: "pro", Price: usd(1999), Interval: billingv1.BillingInterval_BILLING_INTERVAL_MONTHLY})
	if err != nil {
		t.Fatal(err)
	}
	sid := sub.Subscription.Id
	supd, err := subs.UpdateSubscription(ctx, &billingv1.UpdateSubscriptionRequest{SubscriptionId: sid, PlanId: "team", Metadata: map[string]string{"seats": "5"}, UpdateMask: mask("plan_id", "metadata")})
	if err != nil || supd.Subscription.PlanId != "team" || supd.Subscription.Metadata["seats"] != "5" {
		t.Fatalf("update subscription: %+v %v", supd, err)
	}
	if _, err := subs.UpdateSubscription(ctx, &billingv1.UpdateSubscriptionRequest{SubscriptionId: sid, UpdateMask: mask("state")}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("state must not be updatable: %v", err)
	}
	// The same subscription is visible through the legacy RPC.
	if got, err := billingClient.GetSubscription(ctx, &billingv1.GetSubscriptionRequest{SubscriptionId: sid}); err != nil || got.Subscription.PlanId != "team" {
		t.Fatalf("legacy get: %+v %v", got, err)
	}
	sl, err := subs.ListSubscriptions(ctx, &billingv1.ListSubscriptionsRequest{CustomerId: cid})
	if err != nil || len(sl.Subscriptions) != 1 || sl.Subscriptions[0].Id != sid {
		t.Fatalf("list subscriptions: %+v %v", sl, err)
	}
	if _, err := subs.ListSubscriptions(ctx, &billingv1.ListSubscriptionsRequest{CustomerId: "missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("list for unknown customer: %v", err)
	}

	// Delete protection, then a successful delete of an unused customer.
	_, err = customers.DeleteCustomer(ctx, &billingv1.DeleteCustomerRequest{CustomerId: cid})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("delete with subscription: %v", err)
	}
	if _, err := customers.GetCustomer(ctx, &billingv1.GetCustomerRequest{CustomerId: cid}); err != nil {
		t.Fatalf("protected customer vanished: %v", err)
	}
	if _, err := customers.DeleteCustomer(ctx, &billingv1.DeleteCustomerRequest{CustomerId: legacy.Customer.Id}); err != nil {
		t.Fatal(err)
	}
	if _, err := customers.GetCustomer(ctx, &billingv1.GetCustomerRequest{CustomerId: legacy.Customer.Id}); status.Code(err) != codes.NotFound {
		t.Fatalf("deleted customer: %v", err)
	}

	// Cancel stays on the subscription service and blocks further edits.
	if c, err := subs.CancelSubscription(ctx, &billingv1.CancelSubscriptionRequest{SubscriptionId: sid}); err != nil || c.Subscription.State != billingv1.SubscriptionState_SUBSCRIPTION_STATE_CANCELED {
		t.Fatalf("cancel: %+v %v", c, err)
	}
	if _, err := subs.UpdateSubscription(ctx, &billingv1.UpdateSubscriptionRequest{SubscriptionId: sid, PlanId: "late", UpdateMask: mask("plan_id")}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("update canceled: %v", err)
	}
}

func TestCRUDServicesAreHealthyAndReflected(t *testing.T) {
	srv, _ := start(t)
	g, _, _ := srv.Addrs()
	cc := dial(t, g)
	for _, svc := range []string{"billing.v1.BillingService", "billing.v1.CustomerService", "billing.v1.SubscriptionService"} {
		h, err := grpc_health_v1.NewHealthClient(cc).Check(context.Background(), &grpc_health_v1.HealthCheckRequest{Service: svc})
		if err != nil || h.Status != grpc_health_v1.HealthCheckResponse_SERVING {
			t.Errorf("health of %s: %v %v", svc, h, err)
		}
	}
	stream, err := grpc_reflection_v1.NewServerReflectionClient(cc).ServerReflectionInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&grpc_reflection_v1.ServerReflectionRequest{MessageRequest: &grpc_reflection_v1.ServerReflectionRequest_ListServices{ListServices: "*"}}); err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, s := range resp.GetListServicesResponse().GetService() {
		names = append(names, s.Name)
	}
	for _, want := range []string{"billing.v1.CustomerService", "billing.v1.SubscriptionService"} {
		if !strings.Contains(strings.Join(names, ","), want) {
			t.Errorf("reflection lists %v, missing %s", names, want)
		}
	}
}

func TestCRUDOverREST(t *testing.T) {
	srv, _ := start(t)
	_, h, _ := srv.Addrs()
	base := "http://" + h
	auth := map[string]string{"Authorization": "Bearer " + apiKey}
	call := func(method, path, body string, wantCode int) map[string]any {
		t.Helper()
		code, out := doJSON(t, method, base+path, body, auth)
		if code != wantCode {
			t.Fatalf("%s %s: HTTP %d, want %d: %v", method, path, code, wantCode, out)
		}
		return out
	}

	// Auth applies to every new route.
	for _, r := range [][2]string{{"GET", "/v1/customers"}, {"GET", "/v1/customers/x"}, {"PATCH", "/v1/customers/x"}, {"DELETE", "/v1/customers/x"},
		{"GET", "/v1/subscriptions"}, {"PATCH", "/v1/subscriptions/x"}} {
		if code, _ := doJSON(t, r[0], base+r[1], `{}`, nil); code != http.StatusUnauthorized {
			t.Errorf("%s %s without a key: HTTP %d", r[0], r[1], code)
		}
	}

	var ids []string
	for i := 0; i < 3; i++ {
		c := call("POST", "/v1/customers", fmt.Sprintf(`{"email":"rest%d@example.com","name":"R%d"}`, i, i), 200)
		ids = append(ids, c["customer"].(map[string]any)["id"].(string))
	}
	cid := ids[0]

	got := call("GET", "/v1/customers/"+cid, "", 200)["customer"].(map[string]any)
	if got["email"] != "rest0@example.com" || got["name"] != "R0" {
		t.Fatalf("get: %v", got)
	}
	call("GET", "/v1/customers/missing", "", http.StatusNotFound)

	upd := call("PATCH", "/v1/customers/"+cid, `{"name":"Renamed","update_mask":"name"}`, 200)["customer"].(map[string]any)
	if upd["name"] != "Renamed" || upd["email"] != "rest0@example.com" {
		t.Fatalf("patch: %v", upd)
	}
	call("PATCH", "/v1/customers/"+cid, `{"name":"x"}`, http.StatusBadRequest)                                      // no mask
	call("PATCH", "/v1/customers/"+cid, `{"email":"rest1@example.com","update_mask":"email"}`, http.StatusConflict) // taken
	call("PATCH", "/v1/customers/"+cid, `{"email":"not-an-email","update_mask":"email"}`, http.StatusBadRequest)    // invalid
	call("PATCH", "/v1/customers/missing", `{"name":"x","update_mask":"name"}`, http.StatusNotFound)                // unknown
	call("PATCH", "/v1/customers/"+cid, `{"name":"x","update_mask":"id"}`, http.StatusBadRequest)                   // not updatable

	page1 := call("GET", "/v1/customers?page_size=2", "", 200)
	tok, _ := page1["next_page_token"].(string)
	if len(page1["customers"].([]any)) != 2 || tok == "" {
		t.Fatalf("page 1: %v", page1)
	}
	page2 := call("GET", "/v1/customers?page_size=2&page_token="+tok, "", 200)
	if len(page2["customers"].([]any)) != 1 || page2["next_page_token"] != nil && page2["next_page_token"] != "" {
		t.Fatalf("page 2: %v", page2)
	}
	call("GET", "/v1/customers?page_token=!!", "", http.StatusBadRequest)
	call("GET", "/v1/customers?page_size=9999", "", http.StatusBadRequest)

	sub := call("POST", "/v1/subscriptions",
		`{"customer_id":"`+cid+`","plan_id":"pro","price":{"amount_minor":1999,"currency_code":"USD"},"interval":"BILLING_INTERVAL_MONTHLY"}`, 200)["subscription"].(map[string]any)
	sid := sub["id"].(string)
	supd := call("PATCH", "/v1/subscriptions/"+sid, `{"plan_id":"team","metadata":{"seats":"5"},"update_mask":"planId,metadata"}`, 200)["subscription"].(map[string]any)
	if supd["plan_id"] != "team" || supd["metadata"].(map[string]any)["seats"] != "5" || supd["state"] != "SUBSCRIPTION_STATE_PENDING" {
		t.Fatalf("patch subscription: %v", supd)
	}
	call("PATCH", "/v1/subscriptions/"+sid, `{"update_mask":"price"}`, http.StatusBadRequest)
	call("PATCH", "/v1/subscriptions/missing", `{"plan_id":"x","update_mask":"planId"}`, http.StatusNotFound)

	call("POST", "/v1/subscriptions", `{"customer_id":"`+ids[1]+`","plan_id":"p","price":{"amount_minor":5,"currency_code":"USD"},"interval":"BILLING_INTERVAL_YEARLY"}`, 200)
	if n := len(call("GET", "/v1/subscriptions", "", 200)["subscriptions"].([]any)); n != 2 {
		t.Fatalf("all subscriptions: %d", n)
	}
	only := call("GET", "/v1/subscriptions?customer_id="+cid, "", 200)["subscriptions"].([]any)
	if len(only) != 1 || only[0].(map[string]any)["id"] != sid {
		t.Fatalf("filtered subscriptions: %v", only)
	}
	call("GET", "/v1/subscriptions?customer_id=missing", "", http.StatusNotFound)

	// Delete: protected while subscriptions exist, allowed once unused.
	call("DELETE", "/v1/customers/"+cid, "", http.StatusBadRequest) // FAILED_PRECONDITION maps to 400
	call("GET", "/v1/customers/"+cid, "", 200)
	call("DELETE", "/v1/customers/"+ids[2], "", 200)
	call("GET", "/v1/customers/"+ids[2], "", http.StatusNotFound)
	call("DELETE", "/v1/customers/"+ids[2], "", http.StatusNotFound)

	// Existing routes are unchanged.
	call("POST", "/v1/subscriptions/"+sid+":cancel", `{}`, 200)
	call("GET", "/v1/subscriptions/"+sid, "", 200)
	call("GET", "/v1/customers/"+cid+"/balance", "", 200)
	call("GET", "/v1/customers/"+cid+"/ledger", "", 200)
}
