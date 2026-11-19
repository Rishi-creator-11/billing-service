// Command loadtest drives ChargeInvoice against a running server and reports
// throughput and latency percentiles.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	billingv1 "github.com/Rishi-creator-11/billing-service/gen/billing/v1"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "gRPC address")
	apiKey := flag.String("api-key", os.Getenv("BILLING_API_KEY"), "API key")
	conc := flag.Int("c", 16, "concurrent workers")
	dur := flag.Duration("d", 10*time.Second, "test duration")
	dupes := flag.Int("dupes", 1, "send each idempotency key this many times (1 = all unique)")
	customers := flag.Int("customers", 16, "number of customers to spread load over")
	flag.Parse()
	if err := run(*addr, *apiKey, *conc, *dur, *dupes, *customers); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(addr, apiKey string, conc int, dur time.Duration, dupes, nCust int) error {
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer cc.Close()
	c := billingv1.NewBillingServiceClient(cc)
	ctx := context.Background()
	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiKey)
	}
	tag := fmt.Sprint(time.Now().UnixNano())
	usd := func(n int64) *billingv1.Money { return &billingv1.Money{AmountMinor: n, CurrencyCode: "USD"} }

	type target struct{ cust, sub string }
	targets := make([]target, nCust)
	for i := range targets {
		cu, err := c.CreateCustomer(ctx, &billingv1.CreateCustomerRequest{Email: fmt.Sprintf("load-%s-%d@example.com", tag, i)})
		if err != nil {
			return err
		}
		su, err := c.CreateSubscription(ctx, &billingv1.CreateSubscriptionRequest{CustomerId: cu.Customer.Id, PlanId: "load", Price: usd(100), Interval: billingv1.BillingInterval_BILLING_INTERVAL_MONTHLY})
		if err != nil {
			return err
		}
		targets[i] = target{cu.Customer.Id, su.Subscription.Id}
	}

	var seq, errCount, replays atomic.Int64
	var mu sync.Mutex
	var lat []time.Duration
	deadline := time.Now().Add(dur)
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var mine []time.Duration
			for time.Now().Before(deadline) {
				n := seq.Add(1)
				keyN := (n - 1) / int64(dupes) // consecutive requests share a key when dupes > 1
				t := targets[int(keyN)%len(targets)]
				req := &billingv1.ChargeInvoiceRequest{
					IdempotencyKey: fmt.Sprintf("load-%s-%d", tag, keyN), CustomerId: t.cust, SubscriptionId: t.sub,
					InvoiceId: fmt.Sprintf("inv-%d", keyN), Amount: usd(100), PaymentMethodId: "pm_load"}
				rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				t0 := time.Now()
				r, err := c.ChargeInvoice(rctx, req)
				d := time.Since(t0)
				cancel()
				if err != nil {
					errCount.Add(1)
					continue
				}
				if r.IdempotentReplay {
					replays.Add(1)
				}
				mine = append(mine, d)
			}
			mu.Lock()
			lat = append(lat, mine...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		return lat[int(float64(len(lat)-1)*p)]
	}
	fmt.Printf("workers=%d duration=%s dupes_per_key=%d customers=%d\n", conc, elapsed.Round(time.Millisecond), dupes, nCust)
	fmt.Printf("requests=%d ok=%d errors=%d replays=%d\n", len(lat)+int(errCount.Load()), len(lat), errCount.Load(), replays.Load())
	fmt.Printf("throughput=%.0f req/s\n", float64(len(lat))/elapsed.Seconds())
	fmt.Printf("latency p50=%s p95=%s p99=%s max=%s\n", pct(.50).Round(10*time.Microsecond), pct(.95).Round(10*time.Microsecond), pct(.99).Round(10*time.Microsecond), pct(1).Round(10*time.Microsecond))
	return nil
}
