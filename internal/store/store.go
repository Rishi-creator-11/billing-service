// Package store defines the persistence contract. Implementations must give
// ReadWrite serializable semantics: concurrent read-write transactions behave
// as if run one at a time. The service's idempotency guarantee depends on it.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/Rishi-creator-11/billing-service/internal/billing"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrReadOnly      = errors.New("write attempted in a read-only transaction")
)

// LedgerCursor identifies the last entry of a page; listing resumes after it.
type LedgerCursor struct {
	CreatedAt time.Time
	EntryID   string
}

// AccountBalance is the signed sum (debits positive) of one account/currency.
type AccountBalance struct {
	Account  string
	Currency string
	Balance  int64
}

// Tx is the set of operations available inside a transaction.
//
// Writes are visible to other transactions only after the transaction
// commits. Code must not rely on reading its own uncommitted writes: the
// Spanner implementation buffers mutations until commit.
type Tx interface {
	CreateCustomer(c billing.Customer) error // ErrAlreadyExists on duplicate id or email
	GetCustomer(id string) (billing.Customer, error)

	CreateSubscription(s billing.Subscription) error
	GetSubscription(id string) (billing.Subscription, error)
	UpdateSubscription(s billing.Subscription) error

	CreateCharge(c billing.Charge) error
	GetCharge(id string) (billing.Charge, error)
	UpdateCharge(c billing.Charge) error

	CreateRefund(r billing.Refund) error

	AppendLedger(entries []billing.LedgerEntry) error
	// ListLedger returns up to limit entries for the customer ordered by
	// (created_at, id), strictly after the cursor when it is non-nil.
	ListLedger(customerID string, after *LedgerCursor, limit int) ([]billing.LedgerEntry, error)
	Balances(customerID string) ([]AccountBalance, error)
	// LedgerSums returns the signed sum of every entry per currency across the
	// whole ledger. A healthy ledger returns zero for every currency.
	LedgerSums() (map[string]int64, error)

	GetIdempotency(key string) (billing.IdempotencyRecord, error) // ErrNotFound if absent
	PutIdempotency(r billing.IdempotencyRecord) error             // ErrAlreadyExists if present
}

// Store runs transactions. fn may be invoked more than once (the Spanner
// client retries aborted transactions), so it must be free of side effects
// outside the Tx.
type Store interface {
	ReadWrite(ctx context.Context, fn func(Tx) error) error
	ReadOnly(ctx context.Context, fn func(Tx) error) error
	Close()
}
