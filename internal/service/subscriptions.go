package service

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	billingv1 "github.com/Rishi-creator-11/billing-service/gen/billing/v1"
	"github.com/Rishi-creator-11/billing-service/internal/billing"
	"github.com/Rishi-creator-11/billing-service/internal/store"
)

const (
	maxMetadataEntries = 20
	maxMetadataKey     = 64
	maxMetadataValue   = 256
)

// maskPaths validates an update mask against the allowed paths and returns the
// set of paths it names. An empty mask is rejected so that a request can never
// silently update nothing or everything.
func maskPaths(m *fieldmaskpb.FieldMask, allowed ...string) (map[string]bool, error) {
	if len(m.GetPaths()) == 0 {
		return nil, invalid(fmt.Sprintf("update_mask is required; allowed paths: %v", allowed))
	}
	ok := map[string]bool{}
	for _, a := range allowed {
		ok[a] = true
	}
	out := map[string]bool{}
	for _, p := range m.GetPaths() {
		if !ok[p] {
			return nil, invalid(fmt.Sprintf("update_mask path %q is not updatable; allowed paths: %v", p, allowed))
		}
		out[p] = true
	}
	return out, nil
}

func validatePlanID(plan string) error {
	if plan == "" || len(plan) > 128 {
		return invalid("plan_id is required and must be at most 128 characters")
	}
	return nil
}

func validateMetadata(md map[string]string) error {
	if len(md) > maxMetadataEntries {
		return invalid(fmt.Sprintf("metadata may have at most %d entries", maxMetadataEntries))
	}
	for k, v := range md {
		if k == "" || len(k) > maxMetadataKey {
			return invalid(fmt.Sprintf("metadata keys must be 1-%d characters", maxMetadataKey))
		}
		if len(v) > maxMetadataValue {
			return invalid(fmt.Sprintf("metadata values must be at most %d characters", maxMetadataValue))
		}
	}
	return nil
}

func (s *Service) CreateSubscription(ctx context.Context, req *billingv1.CreateSubscriptionRequest) (*billingv1.CreateSubscriptionResponse, error) {
	if req.GetCustomerId() == "" {
		return nil, invalid("customer_id is required")
	}
	if err := validatePlanID(req.GetPlanId()); err != nil {
		return nil, err
	}
	price := moneyFromPB(req.GetPrice())
	if err := price.ValidatePositive(); err != nil {
		return nil, invalid("price: " + err.Error())
	}
	var interval billing.Interval
	switch req.GetInterval() {
	case billingv1.BillingInterval_BILLING_INTERVAL_MONTHLY:
		interval = billing.IntervalMonthly
	case billingv1.BillingInterval_BILLING_INTERVAL_YEARLY:
		interval = billing.IntervalYearly
	default:
		return nil, invalid("interval must be MONTHLY or YEARLY")
	}
	now := s.clock().UTC()
	sub := billing.Subscription{ID: newID(), CustomerID: req.GetCustomerId(), PlanID: req.GetPlanId(), Price: price,
		Interval: interval, State: billing.StatePending, CreatedAt: now, UpdatedAt: now}
	err := s.rw(ctx, func(tx store.Tx) error {
		if _, err := getCustomer(tx, sub.CustomerID); err != nil {
			return err
		}
		return tx.CreateSubscription(sub)
	})
	if err != nil {
		return nil, err
	}
	return &billingv1.CreateSubscriptionResponse{Subscription: subscriptionPB(sub)}, nil
}

func getSubscription(tx store.Tx, id string) (billing.Subscription, error) {
	sub, err := tx.GetSubscription(id)
	if errors.Is(err, store.ErrNotFound) {
		return sub, notFound("subscription", id)
	}
	return sub, err
}

func (s *Service) GetSubscription(ctx context.Context, req *billingv1.GetSubscriptionRequest) (*billingv1.GetSubscriptionResponse, error) {
	var sub billing.Subscription
	err := s.ro(ctx, func(tx store.Tx) (err error) {
		sub, err = getSubscription(tx, req.GetSubscriptionId())
		return err
	})
	if err != nil {
		return nil, err
	}
	return &billingv1.GetSubscriptionResponse{Subscription: subscriptionPB(sub)}, nil
}

// UpdateSubscription edits descriptive fields only. Price, interval and state
// are not editable: charges are validated against the price and currency, and
// state moves only through payments and CancelSubscription, so the state machine
// stays the single authority on transitions.
func (s *Service) UpdateSubscription(ctx context.Context, req *billingv1.UpdateSubscriptionRequest) (*billingv1.UpdateSubscriptionResponse, error) {
	paths, err := maskPaths(req.GetUpdateMask(), "plan_id", "metadata")
	if err != nil {
		return nil, err
	}
	if paths["plan_id"] {
		if err := validatePlanID(req.GetPlanId()); err != nil {
			return nil, err
		}
	}
	if paths["metadata"] {
		if err := validateMetadata(req.GetMetadata()); err != nil {
			return nil, err
		}
	}
	var sub billing.Subscription
	err = s.rw(ctx, func(tx store.Tx) (err error) {
		if sub, err = getSubscription(tx, req.GetSubscriptionId()); err != nil {
			return err
		}
		if sub.State == billing.StateCanceled {
			return withReason(codes.FailedPrecondition, "SUBSCRIPTION_CANCELED", "subscription is %s and cannot be updated", sub.State)
		}
		if paths["plan_id"] {
			sub.PlanID = req.GetPlanId()
		}
		if paths["metadata"] {
			sub.Metadata = maps.Clone(req.GetMetadata())
		}
		sub.UpdatedAt = s.clock().UTC()
		return tx.UpdateSubscription(sub)
	})
	if err != nil {
		return nil, err
	}
	return &billingv1.UpdateSubscriptionResponse{Subscription: subscriptionPB(sub)}, nil
}

func (s *Service) ListSubscriptions(ctx context.Context, req *billingv1.ListSubscriptionsRequest) (*billingv1.ListSubscriptionsResponse, error) {
	size, err := pageSize(req.GetPageSize())
	if err != nil {
		return nil, err
	}
	cursor, err := decodeListCursor(req.GetPageToken())
	if err != nil {
		return nil, invalid("page_token is invalid")
	}
	var rows []billing.Subscription
	err = s.ro(ctx, func(tx store.Tx) (err error) {
		if req.GetCustomerId() != "" {
			if _, err := getCustomer(tx, req.GetCustomerId()); err != nil {
				return err
			}
		}
		rows, err = tx.ListSubscriptions(req.GetCustomerId(), cursor, size+1)
		return err
	})
	if err != nil {
		return nil, err
	}
	resp := &billingv1.ListSubscriptionsResponse{}
	if len(rows) > size {
		rows = rows[:size]
		last := rows[size-1]
		resp.NextPageToken = encodeToken(last.CreatedAt, last.ID)
	}
	for _, sub := range rows {
		resp.Subscriptions = append(resp.Subscriptions, subscriptionPB(sub))
	}
	return resp, nil
}

func (s *Service) CancelSubscription(ctx context.Context, req *billingv1.CancelSubscriptionRequest) (*billingv1.CancelSubscriptionResponse, error) {
	var sub billing.Subscription
	err := s.rw(ctx, func(tx store.Tx) (err error) {
		if sub, err = getSubscription(tx, req.GetSubscriptionId()); err != nil {
			return err
		}
		if sub.State == billing.StateCanceled {
			return nil // already cancelled: retrying is a no-op
		}
		next, err := billing.Next(sub.State, billing.EventCancel)
		if err != nil {
			return err
		}
		now := s.clock().UTC()
		sub.State, sub.UpdatedAt, sub.CanceledAt = next, now, &now
		return tx.UpdateSubscription(sub)
	})
	if err != nil {
		return nil, err
	}
	return &billingv1.CancelSubscriptionResponse{Subscription: subscriptionPB(sub)}, nil
}
