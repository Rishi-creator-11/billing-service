package billing

import (
	"errors"
	"testing"
	"time"
)

func TestNext(t *testing.T) {
	tests := []struct {
		name    string
		from    SubscriptionState
		ev      Event
		want    SubscriptionState
		wantErr bool
	}{
		{"pending pays", StatePending, EventPaymentSucceeded, StateActive, false},
		{"pending declined stays pending", StatePending, EventPaymentDeclined, StatePending, false},
		{"pending cancel", StatePending, EventCancel, StateCanceled, false},
		{"active pays", StateActive, EventPaymentSucceeded, StateActive, false},
		{"active declined", StateActive, EventPaymentDeclined, StatePastDue, false},
		{"active cancel", StateActive, EventCancel, StateCanceled, false},
		{"past due recovers", StatePastDue, EventPaymentSucceeded, StateActive, false},
		{"past due declined", StatePastDue, EventPaymentDeclined, StatePastDue, false},
		{"past due cancel", StatePastDue, EventCancel, StateCanceled, false},
		{"canceled cancel is no-op", StateCanceled, EventCancel, StateCanceled, false},
		{"canceled cannot be paid", StateCanceled, EventPaymentSucceeded, "", true},
		{"canceled cannot be declined", StateCanceled, EventPaymentDeclined, "", true},
		{"unknown state", SubscriptionState("BOGUS"), EventCancel, "", true},
		{"unknown event", StateActive, Event("BOGUS"), "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Next(tt.from, tt.ev)
			if tt.wantErr {
				var te *TransitionError
				if !errors.As(err, &te) {
					t.Fatalf("want TransitionError, got %v", err)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("got (%q, %v), want %q", got, err, tt.want)
			}
		})
	}
}

func TestPostingsBalance(t *testing.T) {
	n := 0
	id := func() string { n++; return string(rune('a' + n)) }
	m := Money{AmountMinor: 1299, Currency: "USD"}
	entries := append(ChargePostings("t1", "c1", "ch1", m, time.Now(), id), RefundPostings("t2", "c1", "r1", Money{500, "USD"}, time.Now(), id)...)
	for cur, sum := range SumByCurrency(entries) {
		if sum != 0 {
			t.Fatalf("%s sums to %d", cur, sum)
		}
	}
	if len(entries) != 4 {
		t.Fatalf("want 4 entries, got %d", len(entries))
	}
}

func TestMoneyValidate(t *testing.T) {
	tests := []struct {
		m  Money
		ok bool
	}{
		{Money{100, "USD"}, true},
		{Money{0, "USD"}, false},
		{Money{-5, "USD"}, false},
		{Money{100, "usd"}, false},
		{Money{100, "US"}, false},
		{Money{100, ""}, false},
	}
	for _, tt := range tests {
		if got := tt.m.ValidatePositive() == nil; got != tt.ok {
			t.Errorf("%+v: got ok=%v want %v", tt.m, got, tt.ok)
		}
	}
}

func TestRefundable(t *testing.T) {
	c := Charge{Amount: Money{1000, "USD"}, Status: ChargePartiallyRefunded, RefundedMinor: 400}
	if c.Refundable() != 600 {
		t.Fatal(c.Refundable())
	}
	c.Status = ChargeDeclined
	if c.Refundable() != 0 {
		t.Fatal("declined must not be refundable")
	}
}
