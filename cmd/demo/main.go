// Command demo walks through the billing API against a running server:
// create a customer, subscribe, charge, retry with the same key, race
// duplicates, refund, and inspect the ledger.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

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

func step(n int, title string) { fmt.Printf("\n== %d. %s\n", n, title) }

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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiKey)
	}
	run := fmt.Sprint(time.Now().UnixNano()) // makes keys and emails unique per demo run

	step(1, "Create a customer")
	cust, err := c.CreateCustomer(ctx, &billingv1.CreateCustomerRequest{Email: "demo-" + run + "@example.com", Name: "Demo Customer"})
	if err != nil {
		return err
	}
	show("customer:", cust.Customer)

	step(2, "Subscribe to a $19.99/month plan")
	sub, err := c.CreateSubscription(ctx, &billingv1.CreateSubscriptionRequest{
		CustomerId: cust.Customer.Id, PlanId: "pro-monthly", Price: usd(1999), Interval: billingv1.BillingInterval_BILLING_INTERVAL_MONTHLY})
	if err != nil {
		return err
	}
	fmt.Println("subscription state:", sub.Subscription.State)

	charge := &billingv1.ChargeInvoiceRequest{
		IdempotencyKey: "demo-" + run + "-invoice-1", CustomerId: cust.Customer.Id, SubscriptionId: sub.Subscription.Id,
		InvoiceId: "inv-1001", Amount: usd(1999), PaymentMethodId: "pm_card_visa"}

	step(3, "Charge the first invoice")
	first, err := c.ChargeInvoice(ctx, charge)
	if err != nil {
		return err
	}
	fmt.Printf("charge %s status=%s idempotent_replay=%v\n", first.Charge.Id, first.Charge.Status, first.IdempotentReplay)

	step(4, "Client times out and retries with the SAME key")
	retry, err := c.ChargeInvoice(ctx, charge)
	if err != nil {
		return err
	}
	fmt.Printf("charge %s status=%s idempotent_replay=%v\n", retry.Charge.Id, retry.Charge.Status, retry.IdempotentReplay)
	if retry.Charge.Id != first.Charge.Id {
		return fmt.Errorf("retry produced a different charge")
	}
	fmt.Println("same charge id returned: no second charge")

	step(5, "Same key, different amount is rejected")
	conflict := proto.Clone(charge).(*billingv1.ChargeInvoiceRequest)
	conflict.Amount = usd(9999)
	_, err = c.ChargeInvoice(ctx, conflict)
	fmt.Printf("code=%s message=%q\n", status.Code(err), status.Convert(err).Message())

	step(6, "50 concurrent requests with one new key")
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

	step(7, "Partially refund the second charge ($5.00), then retry the refund")
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

	step(8, "Ledger")
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

	step(9, "Balance")
	bal, err := c.GetBalance(ctx, &billingv1.GetBalanceRequest{CustomerId: cust.Customer.Id})
	if err != nil {
		return err
	}
	show("balance:", bal)
	// 1999 + 1999 - 500
	if bal.NetCollected[0].AmountMinor != 3498 {
		return fmt.Errorf("net collected %d, want 3498", bal.NetCollected[0].AmountMinor)
	}
	fmt.Println("\ndemo complete: 2 charges and 1 refund were applied exactly once")
	return nil
}
