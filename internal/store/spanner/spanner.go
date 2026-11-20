// Package spanner is the Cloud Spanner implementation of store.Store.
package spanner

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/googleapis/gax-go/v2/apierror"
	"google.golang.org/grpc/codes"

	"github.com/Rishi-creator-11/billing-service/internal/billing"
	"github.com/Rishi-creator-11/billing-service/internal/store"
)

type Store struct {
	client *spanner.Client
}

var _ store.Store = (*Store)(nil)

// Open connects to the database ("projects/P/instances/I/databases/D"). When
// SPANNER_EMULATOR_HOST is set the client talks to the emulator.
func Open(ctx context.Context, db string) (*Store, error) {
	c, err := spanner.NewClient(ctx, db)
	if err != nil {
		return nil, err
	}
	return &Store{client: c}, nil
}

func (s *Store) Close() { s.client.Close() }

// Client exposes the underlying client for tests.
func (s *Store) Client() *spanner.Client { return s.client }

type reader interface {
	ReadRow(ctx context.Context, table string, key spanner.Key, columns []string) (*spanner.Row, error)
	Query(ctx context.Context, stmt spanner.Statement) *spanner.RowIterator
}

func (s *Store) ReadWrite(ctx context.Context, fn func(store.Tx) error) error {
	_, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, rw *spanner.ReadWriteTransaction) error {
		return fn(&tx{ctx: ctx, r: rw, rw: rw})
	})
	return mapErr(err)
}

func (s *Store) ReadOnly(ctx context.Context, fn func(store.Tx) error) error {
	ro := s.client.ReadOnlyTransaction()
	defer ro.Close()
	return mapErr(fn(&tx{ctx: ctx, r: ro}))
}

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	// Errors returned by the transaction callback (already gRPC statuses from
	// the service) pass through; only errors raised by Spanner are translated.
	var apiErr *apierror.APIError
	if !errors.As(err, &apiErr) {
		return err
	}
	switch spanner.ErrCode(err) {
	case codes.AlreadyExists:
		return errors.Join(store.ErrAlreadyExists, err)
	case codes.NotFound:
		return errors.Join(store.ErrNotFound, err)
	}
	return err
}

// tx holds the transaction context because the store.Tx methods do not take
// one; it lives exactly as long as the transaction callback.
type tx struct {
	ctx context.Context //nolint:containedctx
	r   reader
	rw  *spanner.ReadWriteTransaction // nil for read-only
}

func (t *tx) write(m *spanner.Mutation) error {
	if t.rw == nil {
		return store.ErrReadOnly
	}
	return t.rw.BufferWrite([]*spanner.Mutation{m})
}

func (t *tx) row(table string, id string, cols []string) (*spanner.Row, error) {
	row, err := t.r.ReadRow(t.ctx, table, spanner.Key{id}, cols)
	if spanner.ErrCode(err) == codes.NotFound {
		return nil, store.ErrNotFound
	}
	return row, err
}

// ---- customers

var customerCols = []string{"customer_id", "email", "name", "create_time"}

func (t *tx) CreateCustomer(c billing.Customer) error {
	return t.write(spanner.Insert("Customers", customerCols, []any{c.ID, c.Email, c.Name, c.CreatedAt}))
}

func (t *tx) GetCustomer(id string) (billing.Customer, error) {
	var c billing.Customer
	row, err := t.row("Customers", id, customerCols)
	if err != nil {
		return c, err
	}
	err = row.Columns(&c.ID, &c.Email, &c.Name, &c.CreatedAt)
	return c, err
}

func (t *tx) UpdateCustomer(c billing.Customer) error {
	return t.write(spanner.Update("Customers", customerCols, []any{c.ID, c.Email, c.Name, c.CreatedAt}))
}

// DeleteCustomer reads the row first because a Spanner delete of a missing key
// succeeds silently.
func (t *tx) DeleteCustomer(id string) error {
	if _, err := t.row("Customers", id, []string{"customer_id"}); err != nil {
		return err
	}
	return t.write(spanner.Delete("Customers", spanner.Key{id}))
}

func (t *tx) ListCustomers(after *store.ListCursor, limit int) ([]billing.Customer, error) {
	sql := "SELECT customer_id, email, name, create_time FROM Customers"
	params := map[string]any{}
	if after != nil {
		sql += " WHERE (create_time > @t OR (create_time = @t AND customer_id > @id))"
		params["t"], params["id"] = after.CreatedAt, after.ID
	}
	sql += " ORDER BY create_time, customer_id"
	if limit > 0 {
		sql += " LIMIT @n"
		params["n"] = int64(limit)
	}
	var out []billing.Customer
	err := t.r.Query(t.ctx, spanner.Statement{SQL: sql, Params: params}).Do(func(row *spanner.Row) error {
		var c billing.Customer
		if err := row.Columns(&c.ID, &c.Email, &c.Name, &c.CreatedAt); err != nil {
			return err
		}
		out = append(out, c)
		return nil
	})
	return out, err
}

// ---- subscriptions

var subCols = []string{"subscription_id", "customer_id", "plan_id", "price_minor", "currency", "billing_interval", "state", "create_time", "update_time", "cancel_time", "metadata"}

const subSelect = "SELECT subscription_id, customer_id, plan_id, price_minor, currency, billing_interval, state, create_time, update_time, cancel_time, metadata FROM Subscriptions"

func subVals(s billing.Subscription) ([]any, error) {
	var cancel spanner.NullTime
	if s.CanceledAt != nil {
		cancel = spanner.NullTime{Time: *s.CanceledAt, Valid: true}
	}
	var meta spanner.NullString
	if len(s.Metadata) > 0 {
		b, err := json.Marshal(s.Metadata)
		if err != nil {
			return nil, err
		}
		meta = spanner.NullString{StringVal: string(b), Valid: true}
	}
	return []any{s.ID, s.CustomerID, s.PlanID, s.Price.AmountMinor, s.Price.Currency, string(s.Interval), string(s.State), s.CreatedAt, s.UpdatedAt, cancel, meta}, nil
}

// scanSub reads the columns of subCols in order.
func scanSub(row *spanner.Row) (billing.Subscription, error) {
	var s billing.Subscription
	var interval, state string
	var cancel spanner.NullTime
	var meta spanner.NullString
	if err := row.Columns(&s.ID, &s.CustomerID, &s.PlanID, &s.Price.AmountMinor, &s.Price.Currency, &interval, &state, &s.CreatedAt, &s.UpdatedAt, &cancel, &meta); err != nil {
		return s, err
	}
	s.Interval, s.State = billing.Interval(interval), billing.SubscriptionState(state)
	if cancel.Valid {
		ct := cancel.Time
		s.CanceledAt = &ct
	}
	if meta.Valid {
		if err := json.Unmarshal([]byte(meta.StringVal), &s.Metadata); err != nil {
			return s, err
		}
	}
	return s, nil
}

func (t *tx) CreateSubscription(s billing.Subscription) error {
	vals, err := subVals(s)
	if err != nil {
		return err
	}
	return t.write(spanner.Insert("Subscriptions", subCols, vals))
}

func (t *tx) UpdateSubscription(s billing.Subscription) error {
	vals, err := subVals(s)
	if err != nil {
		return err
	}
	return t.write(spanner.Update("Subscriptions", subCols, vals))
}

func (t *tx) GetSubscription(id string) (billing.Subscription, error) {
	row, err := t.row("Subscriptions", id, subCols)
	if err != nil {
		return billing.Subscription{}, err
	}
	return scanSub(row)
}

func (t *tx) ListSubscriptions(customerID string, after *store.ListCursor, limit int) ([]billing.Subscription, error) {
	sql := subSelect
	params := map[string]any{}
	var where []string
	if customerID != "" {
		sql = strings.Replace(sql, "FROM Subscriptions", "FROM Subscriptions@{FORCE_INDEX=SubscriptionsByCustomer}", 1)
		where = append(where, "customer_id = @c")
		params["c"] = customerID
	}
	if after != nil {
		where = append(where, "(create_time > @t OR (create_time = @t AND subscription_id > @id))")
		params["t"], params["id"] = after.CreatedAt, after.ID
	}
	if len(where) > 0 {
		sql += " WHERE " + strings.Join(where, " AND ")
	}
	sql += " ORDER BY create_time, subscription_id"
	if limit > 0 {
		sql += " LIMIT @n"
		params["n"] = int64(limit)
	}
	var out []billing.Subscription
	err := t.r.Query(t.ctx, spanner.Statement{SQL: sql, Params: params}).Do(func(row *spanner.Row) error {
		s, err := scanSub(row)
		if err != nil {
			return err
		}
		out = append(out, s)
		return nil
	})
	return out, err
}

// ---- charges

var chargeCols = []string{"charge_id", "customer_id", "subscription_id", "invoice_id", "amount_minor", "currency", "status", "refunded_minor", "decline_reason", "create_time"}

func chargeVals(c billing.Charge) []any {
	return []any{c.ID, c.CustomerID, c.SubscriptionID, c.InvoiceID, c.Amount.AmountMinor, c.Amount.Currency, string(c.Status), c.RefundedMinor, c.DeclineReason, c.CreatedAt}
}

func (t *tx) CreateCharge(c billing.Charge) error {
	return t.write(spanner.Insert("Charges", chargeCols, chargeVals(c)))
}

func (t *tx) UpdateCharge(c billing.Charge) error {
	return t.write(spanner.Update("Charges", chargeCols, chargeVals(c)))
}

func (t *tx) GetCharge(id string) (billing.Charge, error) {
	var c billing.Charge
	row, err := t.row("Charges", id, chargeCols)
	if err != nil {
		return c, err
	}
	var status string
	if err := row.Columns(&c.ID, &c.CustomerID, &c.SubscriptionID, &c.InvoiceID, &c.Amount.AmountMinor, &c.Amount.Currency, &status, &c.RefundedMinor, &c.DeclineReason, &c.CreatedAt); err != nil {
		return c, err
	}
	c.Status = billing.ChargeStatus(status)
	return c, nil
}

// ---- refunds

func (t *tx) CreateRefund(r billing.Refund) error {
	return t.write(spanner.Insert("Refunds",
		[]string{"refund_id", "charge_id", "amount_minor", "currency", "reason", "create_time"},
		[]any{r.ID, r.ChargeID, r.Amount.AmountMinor, r.Amount.Currency, r.Reason, r.CreatedAt}))
}

// ---- ledger

var ledgerCols = []string{"customer_id", "create_time", "entry_id", "transaction_id", "account", "direction", "amount_minor", "currency", "reference_id"}

func (t *tx) AppendLedger(entries []billing.LedgerEntry) error {
	for _, e := range entries {
		m := spanner.Insert("LedgerEntries", ledgerCols,
			[]any{e.CustomerID, e.CreatedAt, e.ID, e.TransactionID, e.Account, string(e.Direction), e.Amount.AmountMinor, e.Amount.Currency, e.ReferenceID})
		if err := t.write(m); err != nil {
			return err
		}
	}
	return nil
}

func (t *tx) ListLedger(customerID string, after *store.LedgerCursor, limit int) ([]billing.LedgerEntry, error) {
	sql := "SELECT customer_id, create_time, entry_id, transaction_id, account, direction, amount_minor, currency, reference_id FROM LedgerEntries WHERE customer_id = @c"
	params := map[string]any{"c": customerID}
	if after != nil {
		sql += " AND (create_time > @t OR (create_time = @t AND entry_id > @id))"
		params["t"], params["id"] = after.CreatedAt, after.EntryID
	}
	sql += " ORDER BY create_time, entry_id"
	if limit > 0 {
		sql += " LIMIT @n"
		params["n"] = int64(limit)
	}
	it := t.r.Query(t.ctx, spanner.Statement{SQL: sql, Params: params})
	defer it.Stop()
	var out []billing.LedgerEntry
	err := it.Do(func(row *spanner.Row) error {
		var e billing.LedgerEntry
		var dir string
		if err := row.Columns(&e.CustomerID, &e.CreatedAt, &e.ID, &e.TransactionID, &e.Account, &dir, &e.Amount.AmountMinor, &e.Amount.Currency, &e.ReferenceID); err != nil {
			return err
		}
		e.Direction = billing.Direction(dir)
		out = append(out, e)
		return nil
	})
	return out, err
}

func (t *tx) Balances(customerID string) ([]store.AccountBalance, error) {
	it := t.r.Query(t.ctx, spanner.Statement{
		SQL: `SELECT account, currency, SUM(IF(direction = 'DEBIT', amount_minor, -amount_minor))
		      FROM LedgerEntries WHERE customer_id = @c GROUP BY account, currency ORDER BY account, currency`,
		Params: map[string]any{"c": customerID},
	})
	defer it.Stop()
	var out []store.AccountBalance
	err := it.Do(func(row *spanner.Row) error {
		var b store.AccountBalance
		if err := row.Columns(&b.Account, &b.Currency, &b.Balance); err != nil {
			return err
		}
		out = append(out, b)
		return nil
	})
	return out, err
}

func (t *tx) LedgerSums() (map[string]int64, error) {
	it := t.r.Query(t.ctx, spanner.Statement{
		SQL: `SELECT currency, SUM(IF(direction = 'DEBIT', amount_minor, -amount_minor)) FROM LedgerEntries GROUP BY currency`,
	})
	defer it.Stop()
	out := map[string]int64{}
	err := it.Do(func(row *spanner.Row) error {
		var cur string
		var sum int64
		if err := row.Columns(&cur, &sum); err != nil {
			return err
		}
		out[cur] = sum
		return nil
	})
	return out, err
}

// ---- idempotency

func (t *tx) GetIdempotency(key string) (billing.IdempotencyRecord, error) {
	var r billing.IdempotencyRecord
	row, err := t.row("IdempotencyKeys", key, []string{"idempotency_key", "method", "fingerprint", "response", "create_time"})
	if err != nil {
		return r, err
	}
	err = row.Columns(&r.Key, &r.Method, &r.Fingerprint, &r.Response, &r.CreatedAt)
	return r, err
}

func (t *tx) PutIdempotency(r billing.IdempotencyRecord) error {
	return t.write(spanner.Insert("IdempotencyKeys",
		[]string{"idempotency_key", "method", "fingerprint", "response", "create_time"},
		[]any{r.Key, r.Method, r.Fingerprint, r.Response, r.CreatedAt}))
}
