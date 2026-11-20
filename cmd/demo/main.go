// Command demo walks through the billing API against a running server:
// customer and subscription CRUD, delete protection, then charge, retry with
// the same key, race duplicates, refund, and inspect the ledger.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	billingv1 "github.com/Rishi-creator-11/billing-service/gen/billing/v1"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "gRPC address")
	key := flag.String("api-key", os.Getenv("BILLING_API_KEY"), "API key (or BILLING_API_KEY)")
	flag.Parse()
	if err := run(*addr, *key); err != nil {
		fmt.Fprintln(os.Stderr, "demo failed:", err)
		os.Exit(1)
	}
}

var stepNo int

func step(title string) {
	stepNo++
	fmt.Printf("\n== %d. %s\n", stepNo, title)
}

func show(label string, m proto.Message) {
	b, _ := protojson.MarshalOptions{Multiline: true, Indent: "  ", UseProtoNames: true}.Marshal(m)
	fmt.Printf("%s\n%s\n", label, b)
}

func usd(n int64) *billingv1.Money { return &billingv1.Money{AmountMinor: n, CurrencyCode: "USD"} }

func run(addr, apiKey string) error {
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = cc.Close() }()
	c := billingv1.NewBillingServiceClient(cc)
	customers := billingv1.NewCustomerServiceClient(cc)
	subscriptions := billingv1.NewSubscriptionServiceClient(cc)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiKey)
	}
	run := fmt.Sprint(time.Now().UnixNano()) // makes keys and emails unique per demo run

	step("Create a customer")
	cust, err := customers.CreateCustomer(ctx, &billingv1.CreateCustomerRequest{Email: "demo-" + run + "@example.com", Name: "Demo Customer"})
	if err != nil {
		return err
	}
	show("customer:", cust.Customer)

	step("Read, rename and list customers")
	got, err := customers.GetCustomer(ctx, &billingv1.GetCustomerRequest{CustomerId: cust.Customer.Id})
	if err != nil {
		return err
	}
	fmt.Printf("get: %s <%s>\n", got.Customer.Name, got.Customer.Email)
	renamed, err := customers.UpdateCustomer(ctx, &billingv1.UpdateCustomerRequest{
		CustomerId: cust.Customer.Id, Name: "Demo Customer (renamed)", UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}}})
	if err != nil {
		return err
	}
	fmt.Printf("update: name is now %q, email unchanged: %v\n", renamed.Customer.Name, renamed.Customer.Email == got.Customer.Email)
	page, err := customers.ListCustomers(ctx, &billingv1.ListCustomersRequest{PageSize: 100})
	if err != nil {
		return err
	}
	fmt.Printf("list: %d customer(s) on the first page\n", len(page.Customers))

	step("Subscribe to a $19.99/month plan")
	sub, err := subscriptions.CreateSubscription(ctx, &billingv1.CreateSubscriptionRequest{
		CustomerId: cust.Customer.Id, PlanId: "pro-monthly", Price: usd(1999), Interval: billingv1.BillingInterval_BILLING_INTERVAL_MONTHLY})
	if err != nil {
		return err
	}
	fmt.Println("subscription state:", sub.Subscription.State)

	step("Update the subscription's plan and metadata, then list the customer's subscriptions")
	upd, err := subscriptions.UpdateSubscription(ctx, &billingv1.UpdateSubscriptionRequest{
		SubscriptionId: sub.Subscription.Id, PlanId: "pro-monthly-v2", Metadata: map[string]string{"source": "demo"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"plan_id", "metadata"}}})
	if err != nil {
		return err
	}
	fmt.Printf("plan=%s metadata=%v state=%s (state is not editable)\n", upd.Subscription.PlanId, upd.Subscription.Metadata, upd.Subscription.State)
	subs, err := subscriptions.ListSubscriptions(ctx, &billingv1.ListSubscriptionsRequest{CustomerId: cust.Customer.Id})
	if err != nil {
		return err
	}
	fmt.Printf("list: %d subscription(s) for this customer\n", len(subs.Subscriptions))

	charge := &billingv1.ChargeInvoiceRequest{
		IdempotencyKey: "demo-" + run + "-invoice-1", CustomerId: cust.Customer.Id, SubscriptionId: sub.Subscription.Id,
		InvoiceId: "inv-1001", Amount: usd(1999), PaymentMethodId: "pm_card_visa"}

	step("Charge the first invoice")
	first, err := c.ChargeInvoice(ctx, charge)
	if err != nil {
		return err
	}
	fmt.Printf("charge %s status=%s idempotent_replay=%v\n", first.Charge.Id, first.Charge.Status, first.IdempotentReplay)

	step("Client times out and retries with the SAME key")
	retry, err := c.ChargeInvoice(ctx, charge)
	if err != nil {
		return err
	}
	fmt.Printf("charge %s status=%s idempotent_replay=%v\n", retry.Charge.Id, retry.Charge.Status, retry.IdempotentReplay)
	if retry.Charge.Id != first.Charge.Id {
		return fmt.Errorf("retry produced a different charge")
	}
	fmt.Println("same charge id returned: no second charge")

	step("Same key, different amount is rejected")
	conflict := proto.Clone(charge).(*billingv1.ChargeInvoiceRequest)
	conflict.Amount = usd(9999)
	_, err = c.ChargeInvoice(ctx, conflict)
	fmt.Printf("code=%s message=%q\n", status.Code(err), status.Convert(err).Message())

	step("50 concurrent requests with one new key")
	race := proto.Clone(charge).(*billingv1.ChargeInvoiceRequest)
	race.IdempotencyKey, race.InvoiceId = "demo-"+run+"-invoice-2", "inv-1002"
	const n = 50
	var wg sync.WaitGroup
	var mu sync.Mutex
	ids, fresh := map[string]int{}, 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := c.ChargeInvoice(ctx, race)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				fmt.Println("request error:", err)
				return
			}
			ids[r.Charge.Id]++
			if !r.IdempotentReplay {
				fresh++
			}
		}()
	}
	wg.Wait()
	fmt.Printf("requests=%d distinct charge ids=%d requests that created the charge=%d\n", n, len(ids), fresh)
	if len(ids) != 1 || fresh != 1 {
		return fmt.Errorf("expected exactly one charge from concurrent duplicates")
	}
	var raceChargeID string
	for id := range ids {
		raceChargeID = id
	}

	step("Partially refund the second charge ($5.00), then retry the refund")
	rf := &billingv1.RefundChargeRequest{IdempotencyKey: "demo-" + run + "-refund-1", ChargeId: raceChargeID, Amount: usd(500), Reason: "goodwill credit"}
	r1, err := c.RefundCharge(ctx, rf)
	if err != nil {
		return err
	}
	r2, err := c.RefundCharge(ctx, rf)
	if err != nil {
		return err
	}
	fmt.Printf("refund %s charge status=%s refunded=%d\n", r1.Refund.Id, r1.Charge.Status, r1.Charge.Refunded.AmountMinor)
	fmt.Printf("retry  %s idempotent_replay=%v (same refund id: %v)\n", r2.Refund.Id, r2.IdempotentReplay, r1.Refund.Id == r2.Refund.Id)

	step("Ledger")
	led, err := c.GetLedger(ctx, &billingv1.GetLedgerRequest{CustomerId: cust.Customer.Id})
	if err != nil {
		return err
	}
	var sum int64
	fmt.Printf("%-24s %-6s %8s\n", "account", "dir", "amount")
	for _, e := range led.Entries {
		v := e.Amount.AmountMinor
		if e.Direction == billingv1.EntryDirection_ENTRY_DIRECTION_CREDIT {
			v = -v
		}
		sum += v
		fmt.Printf("%-24s %-6s %8d\n", e.Account, e.Direction.String()[len("ENTRY_DIRECTION_"):], e.Amount.AmountMinor)
	}
	fmt.Printf("%d entries, signed sum (debits - credits) = %d\n", len(led.Entries), sum)
	if sum != 0 || len(led.Entries) != 6 {
		return fmt.Errorf("unexpected ledger: %d entries, sum %d", len(led.Entries), sum)
	}

	step("Balance")
	bal, err := c.GetBalance(ctx, &billingv1.GetBalanceRequest{CustomerId: cust.Customer.Id})
	if err != nil {
		return err
	}
	show("balance:", bal)
	// 1999 + 1999 - 500
	if bal.NetCollected[0].AmountMinor != 3498 {
		return fmt.Errorf("net collected %d, want 3498", bal.NetCollected[0].AmountMinor)
	}

	step("Delete protection")
	_, err = customers.DeleteCustomer(ctx, &billingv1.DeleteCustomerRequest{CustomerId: cust.Customer.Id})
	if status.Code(err) != codes.FailedPrecondition {
		return fmt.Errorf("deleting a customer with subscriptions and ledger entries returned %w, want FAILED_PRECONDITION", err)
	}
	fmt.Printf("delete of the billed customer: code=%s message=%q\n", status.Code(err), status.Convert(err).Message())
	spare, err := customers.CreateCustomer(ctx, &billingv1.CreateCustomerRequest{Email: "spare-" + run + "@example.com"})
	if err != nil {
		return err
	}
	if _, err := customers.DeleteCustomer(ctx, &billingv1.DeleteCustomerRequest{CustomerId: spare.Customer.Id}); err != nil {
		return err
	}
	_, err = customers.GetCustomer(ctx, &billingv1.GetCustomerRequest{CustomerId: spare.Customer.Id})
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("deleted customer still readable: %w", err)
	}
	fmt.Println("delete of an unused customer: removed, and a get now returns NOT_FOUND")

	fmt.Println("\ndemo complete: customer and subscription CRUD worked; 2 charges and 1 refund were applied exactly once")
	return nil
}
