// Package billing holds the domain model: entities, the subscription state
// machine and ledger posting rules. It has no I/O and no dependencies on
// storage or transport.
package billing

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

// Account names used by the ledger.
const (
	AccountCash    = "assets:cash"
	AccountRevenue = "revenue:subscriptions"
)

type Interval string

const (
	IntervalMonthly Interval = "MONTHLY"
	IntervalYearly  Interval = "YEARLY"
)

type SubscriptionState string

const (
	StatePending  SubscriptionState = "PENDING"
	StateActive   SubscriptionState = "ACTIVE"
	StatePastDue  SubscriptionState = "PAST_DUE"
	StateCanceled SubscriptionState = "CANCELED"
)

type ChargeStatus string

const (
	ChargeSucceeded         ChargeStatus = "SUCCEEDED"
	ChargeDeclined          ChargeStatus = "DECLINED"
	ChargePartiallyRefunded ChargeStatus = "PARTIALLY_REFUNDED"
	ChargeRefunded          ChargeStatus = "REFUNDED"
)

type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

// Money is an amount in minor units plus an ISO 4217 currency code.
type Money struct {
	AmountMinor int64
	Currency    string
}

var currencyRE = regexp.MustCompile(`^[A-Z]{3}$`)

// ValidateCurrency checks the shape of an ISO 4217 alphabetic code.
func ValidateCurrency(c string) error {
	if !currencyRE.MatchString(c) {
		return fmt.Errorf("currency_code %q must be three upper-case letters", c)
	}
	return nil
}

// ValidatePositive checks that m is a positive amount in a well-formed currency.
func (m Money) ValidatePositive() error {
	if err := ValidateCurrency(m.Currency); err != nil {
		return err
	}
	if m.AmountMinor <= 0 {
		return errors.New("amount_minor must be greater than zero")
	}
	return nil
}

type Customer struct {
	ID        string
	Email     string
	Name      string
	CreatedAt time.Time
}

type Subscription struct {
	ID         string
	CustomerID string
	PlanID     string
	Price      Money
	Interval   Interval
	State      SubscriptionState
	CreatedAt  time.Time
	UpdatedAt  time.Time
	CanceledAt *time.Time
	Metadata   map[string]string
}

type Charge struct {
	ID             string
	CustomerID     string
	SubscriptionID string
	InvoiceID      string
	Amount         Money
	Status         ChargeStatus
	RefundedMinor  int64
	DeclineReason  string
	CreatedAt      time.Time
}

// Refundable returns the amount that can still be refunded.
func (c Charge) Refundable() int64 {
	if c.Status != ChargeSucceeded && c.Status != ChargePartiallyRefunded {
		return 0
	}
	return c.Amount.AmountMinor - c.RefundedMinor
}

type Refund struct {
	ID        string
	ChargeID  string
	Amount    Money
	Reason    string
	CreatedAt time.Time
}

type LedgerEntry struct {
	ID            string
	TransactionID string
	CustomerID    string
	Account       string
	Direction     Direction
	Amount        Money
	ReferenceID   string
	CreatedAt     time.Time
}

// Signed returns the amount as debits-positive, credits-negative.
func (e LedgerEntry) Signed() int64 {
	if e.Direction == Debit {
		return e.Amount.AmountMinor
	}
	return -e.Amount.AmountMinor
}

// IdempotencyRecord is the stored outcome of a keyed request.
type IdempotencyRecord struct {
	Key         string
	Method      string
	Fingerprint []byte
	Response    []byte // serialized response message
	CreatedAt   time.Time
}
