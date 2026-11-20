// Package memory is an in-process Store. Transactions take an exclusive,
// context-aware lock, so read-write transactions are serializable, and a
// failed transaction is rolled back through an undo log.
package memory

import (
	"context"
	"maps"
	"sort"
	"time"

	"github.com/Rishi-creator-11/billing-service/internal/billing"
	"github.com/Rishi-creator-11/billing-service/internal/store"
)

type Store struct {
	sem chan struct{} // capacity 1: a lock that can be abandoned when ctx ends

	customers     map[string]billing.Customer
	emails        map[string]string // email -> customer id
	subscriptions map[string]billing.Subscription
	charges       map[string]billing.Charge
	refunds       map[string]billing.Refund
	ledger        []billing.LedgerEntry
	idem          map[string]billing.IdempotencyRecord
}

var _ store.Store = (*Store)(nil)

func New() *Store {
	return &Store{
		sem:           make(chan struct{}, 1),
		customers:     map[string]billing.Customer{},
		emails:        map[string]string{},
		subscriptions: map[string]billing.Subscription{},
		charges:       map[string]billing.Charge{},
		refunds:       map[string]billing.Refund{},
		idem:          map[string]billing.IdempotencyRecord{},
	}
}

func (s *Store) Close() {}

func (s *Store) lock(ctx context.Context) error {
	select {
	case s.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Store) unlock() { <-s.sem }

func (s *Store) ReadWrite(ctx context.Context, fn func(store.Tx) error) error {
	if err := s.lock(ctx); err != nil {
		return err
	}
	defer s.unlock()
	tx := &tx{s: s}
	if err := fn(tx); err != nil {
		for i := len(tx.undo) - 1; i >= 0; i-- {
			tx.undo[i]()
		}
		return err
	}
	return ctx.Err()
}

func (s *Store) ReadOnly(ctx context.Context, fn func(store.Tx) error) error {
	if err := s.lock(ctx); err != nil {
		return err
	}
	defer s.unlock()
	return fn(&tx{s: s, readOnly: true})
}

type tx struct {
	s        *Store
	readOnly bool
	undo     []func()
}

func (t *tx) guard() error {
	if t.readOnly {
		return store.ErrReadOnly
	}
	return nil
}

func (t *tx) CreateCustomer(c billing.Customer) error {
	if err := t.guard(); err != nil {
		return err
	}
	if _, ok := t.s.customers[c.ID]; ok {
		return store.ErrAlreadyExists
	}
	if _, ok := t.s.emails[c.Email]; ok {
		return store.ErrAlreadyExists
	}
	t.s.customers[c.ID] = c
	t.s.emails[c.Email] = c.ID
	t.undo = append(t.undo, func() { delete(t.s.customers, c.ID); delete(t.s.emails, c.Email) })
	return nil
}

func (t *tx) GetCustomer(id string) (billing.Customer, error) {
	c, ok := t.s.customers[id]
	if !ok {
		return c, store.ErrNotFound
	}
	return c, nil
}

func (t *tx) UpdateCustomer(c billing.Customer) error {
	if err := t.guard(); err != nil {
		return err
	}
	old, ok := t.s.customers[c.ID]
	if !ok {
		return store.ErrNotFound
	}
	if owner, taken := t.s.emails[c.Email]; taken && owner != c.ID {
		return store.ErrAlreadyExists
	}
	delete(t.s.emails, old.Email)
	t.s.emails[c.Email] = c.ID
	t.s.customers[c.ID] = c
	t.undo = append(t.undo, func() {
		delete(t.s.emails, c.Email)
		t.s.emails[old.Email] = old.ID
		t.s.customers[old.ID] = old
	})
	return nil
}

func (t *tx) DeleteCustomer(id string) error {
	if err := t.guard(); err != nil {
		return err
	}
	old, ok := t.s.customers[id]
	if !ok {
		return store.ErrNotFound
	}
	delete(t.s.customers, id)
	delete(t.s.emails, old.Email)
	t.undo = append(t.undo, func() { t.s.customers[id] = old; t.s.emails[old.Email] = id })
	return nil
}

func (t *tx) ListCustomers(after *store.ListCursor, limit int) ([]billing.Customer, error) {
	out := make([]billing.Customer, 0, len(t.s.customers))
	for _, c := range t.s.customers {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return before(out[i].CreatedAt, out[i].ID, out[j].CreatedAt, out[j].ID) })
	if after != nil {
		i := sort.Search(len(out), func(i int) bool { return before(after.CreatedAt, after.ID, out[i].CreatedAt, out[i].ID) })
		out = out[i:]
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (t *tx) ListSubscriptions(customerID string, after *store.ListCursor, limit int) ([]billing.Subscription, error) {
	var out []billing.Subscription
	for _, sub := range t.s.subscriptions {
		if customerID == "" || sub.CustomerID == customerID {
			out = append(out, cloneSub(sub))
		}
	}
	sort.Slice(out, func(i, j int) bool { return before(out[i].CreatedAt, out[i].ID, out[j].CreatedAt, out[j].ID) })
	if after != nil {
		i := sort.Search(len(out), func(i int) bool { return before(after.CreatedAt, after.ID, out[i].CreatedAt, out[i].ID) })
		out = out[i:]
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// cloneSub copies the metadata map so callers and the store never share it.
func cloneSub(s billing.Subscription) billing.Subscription {
	s.Metadata = maps.Clone(s.Metadata)
	return s
}

// before reports whether (at, aid) sorts strictly before (bt, bid).
func before(at time.Time, aid string, bt time.Time, bid string) bool {
	if !at.Equal(bt) {
		return at.Before(bt)
	}
	return aid < bid
}

func (t *tx) CreateSubscription(sub billing.Subscription) error {
	if err := t.guard(); err != nil {
		return err
	}
	if _, ok := t.s.subscriptions[sub.ID]; ok {
		return store.ErrAlreadyExists
	}
	t.s.subscriptions[sub.ID] = cloneSub(sub)
	t.undo = append(t.undo, func() { delete(t.s.subscriptions, sub.ID) })
	return nil
}

func (t *tx) GetSubscription(id string) (billing.Subscription, error) {
	sub, ok := t.s.subscriptions[id]
	if !ok {
		return sub, store.ErrNotFound
	}
	return cloneSub(sub), nil
}

func (t *tx) UpdateSubscription(sub billing.Subscription) error {
	if err := t.guard(); err != nil {
		return err
	}
	old, ok := t.s.subscriptions[sub.ID]
	if !ok {
		return store.ErrNotFound
	}
	t.s.subscriptions[sub.ID] = cloneSub(sub)
	t.undo = append(t.undo, func() { t.s.subscriptions[sub.ID] = old })
	return nil
}

func (t *tx) CreateCharge(c billing.Charge) error {
	if err := t.guard(); err != nil {
		return err
	}
	if _, ok := t.s.charges[c.ID]; ok {
		return store.ErrAlreadyExists
	}
	t.s.charges[c.ID] = c
	t.undo = append(t.undo, func() { delete(t.s.charges, c.ID) })
	return nil
}

func (t *tx) GetCharge(id string) (billing.Charge, error) {
	c, ok := t.s.charges[id]
	if !ok {
		return c, store.ErrNotFound
	}
	return c, nil
}

func (t *tx) UpdateCharge(c billing.Charge) error {
	if err := t.guard(); err != nil {
		return err
	}
	old, ok := t.s.charges[c.ID]
	if !ok {
		return store.ErrNotFound
	}
	t.s.charges[c.ID] = c
	t.undo = append(t.undo, func() { t.s.charges[c.ID] = old })
	return nil
}

func (t *tx) CreateRefund(r billing.Refund) error {
	if err := t.guard(); err != nil {
		return err
	}
	if _, ok := t.s.refunds[r.ID]; ok {
		return store.ErrAlreadyExists
	}
	t.s.refunds[r.ID] = r
	t.undo = append(t.undo, func() { delete(t.s.refunds, r.ID) })
	return nil
}

func (t *tx) AppendLedger(entries []billing.LedgerEntry) error {
	if err := t.guard(); err != nil {
		return err
	}
	n := len(t.s.ledger)
	t.s.ledger = append(t.s.ledger, entries...)
	t.undo = append(t.undo, func() { t.s.ledger = t.s.ledger[:n] })
	return nil
}

func (t *tx) ListLedger(customerID string, after *store.LedgerCursor, limit int) ([]billing.LedgerEntry, error) {
	var out []billing.LedgerEntry
	for _, e := range t.s.ledger {
		if e.CustomerID == customerID {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return less(out[i], out[j]) })
	if after != nil {
		i := sort.Search(len(out), func(i int) bool {
			return out[i].CreatedAt.After(after.CreatedAt) ||
				(out[i].CreatedAt.Equal(after.CreatedAt) && out[i].ID > after.EntryID)
		})
		out = out[i:]
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func less(a, b billing.LedgerEntry) bool {
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.Before(b.CreatedAt)
	}
	return a.ID < b.ID
}

func (t *tx) Balances(customerID string) ([]store.AccountBalance, error) {
	type key struct{ account, currency string }
	sums := map[key]int64{}
	for _, e := range t.s.ledger {
		if e.CustomerID == customerID {
			sums[key{e.Account, e.Amount.Currency}] += e.Signed()
		}
	}
	out := make([]store.AccountBalance, 0, len(sums))
	for k, v := range sums {
		out = append(out, store.AccountBalance{Account: k.account, Currency: k.currency, Balance: v})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Account != out[j].Account {
			return out[i].Account < out[j].Account
		}
		return out[i].Currency < out[j].Currency
	})
	return out, nil
}

func (t *tx) LedgerSums() (map[string]int64, error) {
	return billing.SumByCurrency(t.s.ledger), nil
}

func (t *tx) GetIdempotency(key string) (billing.IdempotencyRecord, error) {
	r, ok := t.s.idem[key]
	if !ok {
		return r, store.ErrNotFound
	}
	return r, nil
}

func (t *tx) PutIdempotency(r billing.IdempotencyRecord) error {
	if err := t.guard(); err != nil {
		return err
	}
	if _, ok := t.s.idem[r.Key]; ok {
		return store.ErrAlreadyExists
	}
	t.s.idem[r.Key] = r
	t.undo = append(t.undo, func() { delete(t.s.idem, r.Key) })
	return nil
}
