package app_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"

	billingv1 "github.com/Rishi-creator-11/billing-service/gen/billing/v1"
	"github.com/Rishi-creator-11/billing-service/internal/app"
	"github.com/Rishi-creator-11/billing-service/internal/billing"
	"github.com/Rishi-creator-11/billing-service/internal/service"
	"github.com/Rishi-creator-11/billing-service/internal/store/memory"
)

const apiKey = "test-key"

type slowProcessor struct{ d time.Duration }

func (p slowProcessor) Authorize(string, billing.Money) string { time.Sleep(p.d); return "" }

func start(t *testing.T, opts ...app.Option) (srv *app.Server, stop func() error) {
	t.Helper()
	cfg := app.Config{GRPCAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0", AdminAddr: "127.0.0.1:0", Store: "memory",
		APIKeys: []string{apiKey}, DefaultTimeout: 10 * time.Second, ShutdownGrace: 5 * time.Second}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := app.New(context.Background(), cfg, log, append([]app.Option{app.WithStore(memory.New())}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	var once bool
	stop = func() error {
		if !once {
			once = true
			cancel()
		}
		return <-done
	}
	t.Cleanup(func() { cancel() })
	return srv, stop
}

func dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return cc
}

func authed() context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+apiKey)
}

func usd(n int64) *billingv1.Money { return &billingv1.Money{AmountMinor: n, CurrencyCode: "USD"} }

func TestGRPCEndToEnd(t *testing.T) {
	srv, _ := start(t)
	g, _, _ := srv.Addrs()
	cc := dial(t, g)
	c := billingv1.NewBillingServiceClient(cc)

	// auth
	if _, err := c.GetBalance(context.Background(), &billingv1.GetBalanceRequest{CustomerId: "x"}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("no key: %v", err)
	}
	bad := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer wrong")
	if _, err := c.GetBalance(bad, &billingv1.GetBalanceRequest{CustomerId: "x"}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("wrong key: %v", err)
	}

	cust, err := c.CreateCustomer(authed(), &billingv1.CreateCustomerRequest{Email: "e2e@example.com", Name: "E2E"})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := c.CreateSubscription(authed(), &billingv1.CreateSubscriptionRequest{CustomerId: cust.Customer.Id, PlanId: "pro", Price: usd(1999), Interval: billingv1.BillingInterval_BILLING_INTERVAL_MONTHLY})
	if err != nil {
		t.Fatal(err)
	}
	req := &billingv1.ChargeInvoiceRequest{IdempotencyKey: "e2e-1", CustomerId: cust.Customer.Id, SubscriptionId: sub.Subscription.Id, InvoiceId: "inv-1", Amount: usd(1999), PaymentMethodId: "pm_ok"}
	var hdr metadata.MD
	first, err := c.ChargeInvoice(metadata.AppendToOutgoingContext(authed(), "x-request-id", "req-42"), req, grpc.Header(&hdr))
	if err != nil {
		t.Fatal(err)
	}
	if got := hdr.Get("x-request-id"); len(got) != 1 || got[0] != "req-42" {
		t.Fatalf("request id not echoed: %v", hdr)
	}
	again, err := c.ChargeInvoice(authed(), req)
	if err != nil || !again.IdempotentReplay || again.Charge.Id != first.Charge.Id {
		t.Fatalf("replay: %+v %v", again, err)
	}
	req.Amount = usd(5)
	if _, err := c.ChargeInvoice(authed(), req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("payload mismatch: %v", err)
	}

	// client deadline already expired
	ctx, cancel := context.WithTimeout(authed(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)
	if _, err := c.GetBalance(ctx, &billingv1.GetBalanceRequest{CustomerId: cust.Customer.Id}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("expired deadline: %v", err)
	}
}

func TestHealthAndReflection(t *testing.T) {
	srv, _ := start(t)
	g, _, _ := srv.Addrs()
	cc := dial(t, g)

	// Health and reflection need no API key.
	h, err := grpc_health_v1.NewHealthClient(cc).Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil || h.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("health: %v %v", h, err)
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
	if !strings.Contains(strings.Join(names, ","), "billing.v1.BillingService") {
		t.Fatalf("reflection lists %v", names)
	}
}

func doJSON(t *testing.T, method, url, body string, headers map[string]string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(method, url, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestRESTGatewayAndMetrics(t *testing.T) {
	srv, _ := start(t)
	_, h, a := srv.Addrs()
	base := "http://" + h
	auth := map[string]string{"Authorization": "Bearer " + apiKey}

	if code, _ := doJSON(t, "POST", base+"/v1/customers", `{"email":"r@example.com"}`, nil); code != http.StatusUnauthorized {
		t.Fatalf("no key: HTTP %d", code)
	}
	code, cust := doJSON(t, "POST", base+"/v1/customers", `{"email":"r@example.com","name":"Rest"}`, auth)
	if code != 200 {
		t.Fatalf("create customer: %d %v", code, cust)
	}
	cid := cust["customer"].(map[string]any)["id"].(string)
	if code, _ := doJSON(t, "POST", base+"/v1/customers", `{"email":"r@example.com"}`, auth); code != http.StatusConflict {
		t.Fatalf("duplicate customer: HTTP %d", code)
	}
	_, sub := doJSON(t, "POST", base+"/v1/subscriptions",
		`{"customer_id":"`+cid+`","plan_id":"pro","price":{"amount_minor":1999,"currency_code":"USD"},"interval":"BILLING_INTERVAL_MONTHLY"}`, auth)
	sid := sub["subscription"].(map[string]any)["id"].(string)

	charge := `{"idempotency_key":"rest-1","customer_id":"` + cid + `","subscription_id":"` + sid + `","invoice_id":"inv-1","amount":{"amount_minor":1999,"currency_code":"USD"},"payment_method_id":"pm_ok"}`
	code, c1 := doJSON(t, "POST", base+"/v1/charges", charge, auth)
	if code != 200 || c1["idempotent_replay"] != false {
		t.Fatalf("charge: %d %v", code, c1)
	}
	_, c2 := doJSON(t, "POST", base+"/v1/charges", charge, auth)
	if c2["idempotent_replay"] != true || c2["charge"].(map[string]any)["id"] != c1["charge"].(map[string]any)["id"] {
		t.Fatalf("replay: %v", c2)
	}
	code, e := doJSON(t, "POST", base+"/v1/charges", strings.Replace(charge, "1999,\"currency_code\":\"USD\"}", "2000,\"currency_code\":\"USD\"}", 1), auth)
	if code != http.StatusBadRequest || !strings.Contains(e["message"].(string), "different request") {
		t.Fatalf("mismatch: %d %v", code, e)
	}
	code, bal := doJSON(t, "GET", base+"/v1/customers/"+cid+"/balance", "", auth)
	if code != 200 || len(bal["net_collected"].([]any)) != 1 {
		t.Fatalf("balance: %d %v", code, bal)
	}

	resp, err := http.Get("http://" + a + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	for _, want := range []string{
		`billing_charge_outcomes_total{outcome="succeeded"} 1`,
		`billing_charge_outcomes_total{outcome="replayed"} 1`,
		`billing_charge_outcomes_total{outcome="key_conflict"} 1`,
		`billing_idempotent_replays_total{method="ChargeInvoice"} 1`,
		`billing_grpc_requests_total{code="OK",method="ChargeInvoice"} 2`,
		`billing_grpc_request_duration_seconds_bucket`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics missing %q", want)
		}
	}
	r, err := http.Get("http://" + a + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Body.Close()
	if r.StatusCode != 200 {
		t.Fatalf("readyz status %d", r.StatusCode)
	}
}

func TestGracefulShutdownFinishesInFlightRequests(t *testing.T) {
	srv, stop := start(t, app.WithServiceOptions(service.WithProcessor(slowProcessor{300 * time.Millisecond})))
	g, _, _ := srv.Addrs()
	c := billingv1.NewBillingServiceClient(dial(t, g))
	cust, err := c.CreateCustomer(authed(), &billingv1.CreateCustomerRequest{Email: "g@example.com"})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := c.CreateSubscription(authed(), &billingv1.CreateSubscriptionRequest{CustomerId: cust.Customer.Id, PlanId: "p", Price: usd(100), Interval: billingv1.BillingInterval_BILLING_INTERVAL_MONTHLY})
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() {
		_, err := c.ChargeInvoice(authed(), &billingv1.ChargeInvoiceRequest{IdempotencyKey: "g1", CustomerId: cust.Customer.Id, SubscriptionId: sub.Subscription.Id, InvoiceId: "i", Amount: usd(100), PaymentMethodId: "pm"})
		result <- err
	}()
	time.Sleep(100 * time.Millisecond) // the request is now inside the slow processor
	if err := stop(); err != nil {
		t.Fatalf("serve returned %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("in-flight request failed during shutdown: %v", err)
	}
	ctx, cancel := context.WithTimeout(authed(), time.Second)
	defer cancel()
	if _, err := c.GetBalance(ctx, &billingv1.GetBalanceRequest{CustomerId: cust.Customer.Id}); err == nil {
		t.Fatal("server still accepts requests after shutdown")
	}
}
