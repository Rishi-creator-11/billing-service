package service

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	billingv1 "github.com/Rishi-creator-11/billing-service/gen/billing/v1"
	"github.com/Rishi-creator-11/billing-service/internal/billing"
)

func ts(t time.Time) *timestamppb.Timestamp { return timestamppb.New(t) }

func moneyPB(m billing.Money) *billingv1.Money {
	return &billingv1.Money{AmountMinor: m.AmountMinor, CurrencyCode: m.Currency}
}

func moneyFromPB(m *billingv1.Money) billing.Money {
	return billing.Money{AmountMinor: m.GetAmountMinor(), Currency: m.GetCurrencyCode()}
}

var intervalToPB = map[billing.Interval]billingv1.BillingInterval{
	billing.IntervalMonthly: billingv1.BillingInterval_BILLING_INTERVAL_MONTHLY,
	billing.IntervalYearly:  billingv1.BillingInterval_BILLING_INTERVAL_YEARLY,
}

var stateToPB = map[billing.SubscriptionState]billingv1.SubscriptionState{
	billing.StatePending:  billingv1.SubscriptionState_SUBSCRIPTION_STATE_PENDING,
	billing.StateActive:   billingv1.SubscriptionState_SUBSCRIPTION_STATE_ACTIVE,
	billing.StatePastDue:  billingv1.SubscriptionState_SUBSCRIPTION_STATE_PAST_DUE,
	billing.StateCanceled: billingv1.SubscriptionState_SUBSCRIPTION_STATE_CANCELED,
}

var chargeStatusToPB = map[billing.ChargeStatus]billingv1.ChargeStatus{
	billing.ChargeSucceeded:         billingv1.ChargeStatus_CHARGE_STATUS_SUCCEEDED,
	billing.ChargeDeclined:          billingv1.ChargeStatus_CHARGE_STATUS_DECLINED,
	billing.ChargePartiallyRefunded: billingv1.ChargeStatus_CHARGE_STATUS_PARTIALLY_REFUNDED,
	billing.ChargeRefunded:          billingv1.ChargeStatus_CHARGE_STATUS_REFUNDED,
}

func customerPB(c billing.Customer) *billingv1.Customer {
	return &billingv1.Customer{Id: c.ID, Email: c.Email, Name: c.Name, CreateTime: ts(c.CreatedAt)}
}

func subscriptionPB(s billing.Subscription) *billingv1.Subscription {
	out := &billingv1.Subscription{
		Id: s.ID, CustomerId: s.CustomerID, PlanId: s.PlanID, Price: moneyPB(s.Price),
		Interval: intervalToPB[s.Interval], State: stateToPB[s.State],
		CreateTime: ts(s.CreatedAt), UpdateTime: ts(s.UpdatedAt),
	}
	if s.CanceledAt != nil {
		out.CancelTime = ts(*s.CanceledAt)
	}
	return out
}

func chargePB(c billing.Charge) *billingv1.Charge {
	return &billingv1.Charge{
		Id: c.ID, CustomerId: c.CustomerID, SubscriptionId: c.SubscriptionID, InvoiceId: c.InvoiceID,
		Amount: moneyPB(c.Amount), Status: chargeStatusToPB[c.Status],
		Refunded:      moneyPB(billing.Money{AmountMinor: c.RefundedMinor, Currency: c.Amount.Currency}),
		DeclineReason: c.DeclineReason, CreateTime: ts(c.CreatedAt),
	}
}

func refundPB(r billing.Refund) *billingv1.Refund {
	return &billingv1.Refund{Id: r.ID, ChargeId: r.ChargeID, Amount: moneyPB(r.Amount), Reason: r.Reason, CreateTime: ts(r.CreatedAt)}
}

func entryPB(e billing.LedgerEntry) *billingv1.LedgerEntry {
	dir := billingv1.EntryDirection_ENTRY_DIRECTION_DEBIT
	if e.Direction == billing.Credit {
		dir = billingv1.EntryDirection_ENTRY_DIRECTION_CREDIT
	}
	return &billingv1.LedgerEntry{
		Id: e.ID, TransactionId: e.TransactionID, CustomerId: e.CustomerID, Account: e.Account,
		Direction: dir, Amount: moneyPB(e.Amount), ReferenceId: e.ReferenceID, CreateTime: ts(e.CreatedAt),
	}
}
