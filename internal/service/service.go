// Package service implements billing.v1.BillingService.
package service

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	billingv1 "github.com/Rishi-creator-11/billing-service/gen/billing/v1"
	"github.com/Rishi-creator-11/billing-service/internal/billing"
	"github.com/Rishi-creator-11/billing-service/internal/metrics"
	"github.com/Rishi-creator-11/billing-service/internal/store"
)

const (
	defaultPageSize = 50
	maxPageSize     = 500
)

type Service struct {
	billingv1.UnimplementedBillingServiceServer
	billingv1.UnimplementedCustomerServiceServer
	billingv1.UnimplementedSubscriptionServiceServer

	store   store.Store
	proc    Processor
	metrics *metrics.Metrics
	log     *slog.Logger
	clock   func() time.Time
}

type Option func(*Service)

func WithProcessor(p Processor) Option      { return func(s *Service) { s.proc = p } }
func WithMetrics(m *metrics.Metrics) Option { return func(s *Service) { s.metrics = m } }
func WithLogger(l *slog.Logger) Option      { return func(s *Service) { s.log = l } }
func WithClock(c func() time.Time) Option   { return func(s *Service) { s.clock = c } }

func New(st store.Store, opts ...Option) *Service {
	s := &Service{store: st, proc: SimulatedProcessor{}, metrics: metrics.New(), log: slog.Default(), clock: time.Now}
	for _, o := range opts {
		o(s)
	}
	return s
}

func newID() string { return uuid.NewString() }

// rw / ro wrap the store so that every error leaves as a gRPC status.
func (s *Service) rw(ctx context.Context, fn func(store.Tx) error) error {
	return toStatus(ctx, s.log, s.store.ReadWrite(ctx, fn))
}

func (s *Service) ro(ctx context.Context, fn func(store.Tx) error) error {
	return toStatus(ctx, s.log, s.store.ReadOnly(ctx, fn))
}

// pageSize applies the default and maximum to a requested page size.
func pageSize(n int32) (int, error) {
	switch {
	case n < 0 || n > maxPageSize:
		return 0, invalid(fmt.Sprintf("page_size must be between 0 and %d", maxPageSize))
	case n == 0:
		return defaultPageSize, nil
	}
	return int(n), nil
}

// A page token is the (create time, id) of the last row returned.
func encodeToken(at time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(at.UTC().Format(time.RFC3339Nano) + "|" + id))
}

func decodeToken(tok string) (at time.Time, id string, err error) {
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return at, "", err
	}
	atStr, id, ok := strings.Cut(string(raw), "|")
	if !ok || id == "" {
		return at, "", errors.New("malformed token")
	}
	at, err = time.Parse(time.RFC3339Nano, atStr)
	return at, id, err
}

func decodeCursor(tok string) (*store.LedgerCursor, error) {
	if tok == "" {
		return nil, nil
	}
	at, id, err := decodeToken(tok)
	if err != nil {
		return nil, err
	}
	return &store.LedgerCursor{CreatedAt: at, EntryID: id}, nil
}

func decodeListCursor(tok string) (*store.ListCursor, error) {
	if tok == "" {
		return nil, nil
	}
	at, id, err := decodeToken(tok)
	if err != nil {
		return nil, err
	}
	return &store.ListCursor{CreatedAt: at, ID: id}, nil
}

func notFound(what, id string) error {
	return status.Errorf(codes.NotFound, "%s %q not found", what, id)
}

// ---- charges

func (s *Service) ChargeInvoice(ctx context.Context, req *billingv1.ChargeInvoiceRequest) (*billingv1.ChargeInvoiceResponse, error) {
	if err := validateKey(req.GetIdempotencyKey()); err != nil {
		return nil, invalid(err.Error())
	}
	amount := moneyFromPB(req.GetAmount())
	switch {
	case req.GetCustomerId() == "":
		return nil, invalid("customer_id is required")
	case req.GetSubscriptionId() == "":
		return nil, invalid("subscription_id is required")
	case req.GetInvoiceId() == "" || len(req.GetInvoiceId()) > 128:
		return nil, invalid("invoice_id is required and must be at most 128 characters")
	case req.GetPaymentMethodId() == "":
		return nil, invalid("payment_method_id is required")
	}
	if err := amount.ValidatePositive(); err != nil {
		return nil, invalid("amount: " + err.Error())
	}

	const method = "ChargeInvoice"
	fp := fingerprint(method, req.GetCustomerId(), req.GetSubscriptionId(), req.GetInvoiceId(),
		strconv.FormatInt(amount.AmountMinor, 10), amount.Currency, req.GetPaymentMethodId())

	resp, replayed, err := runIdempotent(ctx, s, method, req.GetIdempotencyKey(), fp,
		func() *billingv1.ChargeInvoiceResponse { return &billingv1.ChargeInvoiceResponse{} },
		func(tx store.Tx) (*billingv1.ChargeInvoiceResponse, error) {
			ch, err := s.charge(tx, req, amount)
			if err != nil {
				return nil, err
			}
			return &billingv1.ChargeInvoiceResponse{Charge: chargePB(ch)}, nil
		})
	err = toStatus(ctx, s.log, err)
	switch {
	case err != nil:
		if status.Code(err) != codes.FailedPrecondition || !isKeyReuse(err) {
			s.metrics.ChargeOutcomes.WithLabelValues(metrics.OutcomeRejected).Inc()
		} else {
			s.metrics.ChargeOutcomes.WithLabelValues(metrics.OutcomeConflict).Inc()
		}
		return nil, err
	case replayed:
		s.metrics.ChargeOutcomes.WithLabelValues(metrics.OutcomeReplayed).Inc()
	case resp.GetCharge().GetStatus() == billingv1.ChargeStatus_CHARGE_STATUS_DECLINED:
		s.metrics.ChargeOutcomes.WithLabelValues(metrics.OutcomeDeclined).Inc()
	default:
		s.metrics.ChargeOutcomes.WithLabelValues(metrics.OutcomeSucceeded).Inc()
	}
	resp.IdempotentReplay = replayed
	return resp, nil
}

func isKeyReuse(err error) bool {
	for _, d := range status.Convert(err).Details() {
		if info, ok := d.(interface{ GetReason() string }); ok && info.GetReason() == "IDEMPOTENCY_KEY_REUSED" {
			return true
		}
	}
	return false
}

// charge runs inside the idempotent transaction: it validates against the
// subscription, applies the processor decision, moves the subscription state,
// stores the charge and, if approved, posts the balanced ledger entries.
func (s *Service) charge(tx store.Tx, req *billingv1.ChargeInvoiceRequest, amount billing.Money) (billing.Charge, error) {
	sub, err := tx.GetSubscription(req.GetSubscriptionId())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return billing.Charge{}, notFound("subscription", req.GetSubscriptionId())
		}
		return billing.Charge{}, err
	}
	if sub.CustomerID != req.GetCustomerId() {
		return billing.Charge{}, invalid("subscription does not belong to customer")
	}
	if sub.Price.Currency != amount.Currency {
		return billing.Charge{}, withReason(codes.FailedPrecondition, "CURRENCY_MISMATCH",
			"charge currency %s does not match subscription currency %s", amount.Currency, sub.Price.Currency)
	}
	if !billing.CanCharge(sub.State) {
		return billing.Charge{}, withReason(codes.FailedPrecondition, "SUBSCRIPTION_CANCELED", "subscription is %s and cannot be charged", sub.State)
	}

	now := s.clock().UTC()
	ch := billing.Charge{ID: newID(), CustomerID: sub.CustomerID, SubscriptionID: sub.ID, InvoiceID: req.GetInvoiceId(),
		Amount: amount, Status: billing.ChargeSucceeded, CreatedAt: now}
	ev := billing.EventPaymentSucceeded
	if reason := s.proc.Authorize(req.GetPaymentMethodId(), amount); reason != "" {
		ch.Status, ch.DeclineReason, ev = billing.ChargeDeclined, reason, billing.EventPaymentDeclined
	}

	next, err := billing.Next(sub.State, ev)
	if err != nil {
		return ch, err
	}
	if next != sub.State {
		sub.State, sub.UpdatedAt = next, now
		if err := tx.UpdateSubscription(sub); err != nil {
			return ch, err
		}
	}
	if err := tx.CreateCharge(ch); err != nil {
		return ch, err
	}
	if ch.Status == billing.ChargeSucceeded {
		entries := billing.ChargePostings(newID(), ch.CustomerID, ch.ID, amount, now, newID)
		if err := tx.AppendLedger(entries); err != nil {
			return ch, err
		}
	}
	return ch, nil
}

// ---- refunds

func (s *Service) RefundCharge(ctx context.Context, req *billingv1.RefundChargeRequest) (*billingv1.RefundChargeResponse, error) {
	if err := validateKey(req.GetIdempotencyKey()); err != nil {
		return nil, invalid(err.Error())
	}
	if req.GetChargeId() == "" {
		return nil, invalid("charge_id is required")
	}
	if len(req.GetReason()) > 256 {
		return nil, invalid("reason must be at most 256 characters")
	}
	amount := moneyFromPB(req.GetAmount())
	if err := amount.ValidatePositive(); err != nil {
		return nil, invalid("amount: " + err.Error())
	}

	const method = "RefundCharge"
	fp := fingerprint(method, req.GetChargeId(), strconv.FormatInt(amount.AmountMinor, 10), amount.Currency, req.GetReason())

	resp, replayed, err := runIdempotent(ctx, s, method, req.GetIdempotencyKey(), fp,
		func() *billingv1.RefundChargeResponse { return &billingv1.RefundChargeResponse{} },
		func(tx store.Tx) (*billingv1.RefundChargeResponse, error) {
			ch, err := tx.GetCharge(req.GetChargeId())
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					return nil, notFound("charge", req.GetChargeId())
				}
				return nil, err
			}
			if ch.Amount.Currency != amount.Currency {
				return nil, withReason(codes.FailedPrecondition, "CURRENCY_MISMATCH",
					"refund currency %s does not match charge currency %s", amount.Currency, ch.Amount.Currency)
			}
			if left := ch.Refundable(); amount.AmountMinor > left {
				return nil, withReason(codes.FailedPrecondition, "REFUND_EXCEEDS_CHARGE",
					"refund of %d exceeds the %d still refundable on this charge", amount.AmountMinor, left)
			}
			now := s.clock().UTC()
			rf := billing.Refund{ID: newID(), ChargeID: ch.ID, Amount: amount, Reason: req.GetReason(), CreatedAt: now}
			ch.RefundedMinor += amount.AmountMinor
			ch.Status = billing.ChargePartiallyRefunded
			if ch.RefundedMinor == ch.Amount.AmountMinor {
				ch.Status = billing.ChargeRefunded
			}
			if err := tx.CreateRefund(rf); err != nil {
				return nil, err
			}
			if err := tx.UpdateCharge(ch); err != nil {
				return nil, err
			}
			if err := tx.AppendLedger(billing.RefundPostings(newID(), ch.CustomerID, rf.ID, amount, now, newID)); err != nil {
				return nil, err
			}
			return &billingv1.RefundChargeResponse{Refund: refundPB(rf), Charge: chargePB(ch)}, nil
		})
	if err = toStatus(ctx, s.log, err); err != nil {
		return nil, err
	}
	resp.IdempotentReplay = replayed
	return resp, nil
}

// ---- ledger

func (s *Service) GetLedger(ctx context.Context, req *billingv1.GetLedgerRequest) (*billingv1.GetLedgerResponse, error) {
	size, err := pageSize(req.GetPageSize())
	if err != nil {
		return nil, err
	}
	cursor, err := decodeCursor(req.GetPageToken())
	if err != nil {
		return nil, invalid("page_token is invalid")
	}
	var entries []billing.LedgerEntry
	err = s.ro(ctx, func(tx store.Tx) error {
		if _, err := tx.GetCustomer(req.GetCustomerId()); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return notFound("customer", req.GetCustomerId())
			}
			return err
		}
		entries, err = tx.ListLedger(req.GetCustomerId(), cursor, size+1)
		return err
	})
	if err != nil {
		return nil, err
	}
	resp := &billingv1.GetLedgerResponse{}
	if len(entries) > size {
		entries = entries[:size]
		last := entries[len(entries)-1]
		resp.NextPageToken = encodeToken(last.CreatedAt, last.ID)
	}
	for _, e := range entries {
		resp.Entries = append(resp.Entries, entryPB(e))
	}
	return resp, nil
}

func (s *Service) GetBalance(ctx context.Context, req *billingv1.GetBalanceRequest) (*billingv1.GetBalanceResponse, error) {
	var bals []store.AccountBalance
	err := s.ro(ctx, func(tx store.Tx) error {
		if _, err := tx.GetCustomer(req.GetCustomerId()); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return notFound("customer", req.GetCustomerId())
			}
			return err
		}
		var err error
		bals, err = tx.Balances(req.GetCustomerId())
		return err
	})
	if err != nil {
		return nil, err
	}
	resp := &billingv1.GetBalanceResponse{CustomerId: req.GetCustomerId()}
	for _, b := range bals {
		m := billing.Money{AmountMinor: b.Balance, Currency: b.Currency}
		resp.Balances = append(resp.Balances, &billingv1.AccountBalance{Account: b.Account, Balance: moneyPB(m)})
		if b.Account == billing.AccountRevenue {
			// Revenue is a credit account: money kept is the negated balance.
			resp.NetCollected = append(resp.NetCollected, moneyPB(billing.Money{AmountMinor: -b.Balance, Currency: b.Currency}))
		}
	}
	return resp, nil
}
