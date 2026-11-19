package service

import "github.com/Rishi-creator-11/billing-service/internal/billing"

// Processor decides whether a payment method can be charged. It stands in for
// a payment gateway. Implementations used inside a store transaction must be
// side-effect free, because the transaction function can be retried.
type Processor interface {
	// Authorize returns an empty string to approve, or a decline reason.
	Authorize(paymentMethodID string, amount billing.Money) string
}

// DeclineMethod is the payment method value the simulated processor declines.
const DeclineMethod = "pm_decline"

// SimulatedProcessor approves everything except DeclineMethod.
type SimulatedProcessor struct{}

func (SimulatedProcessor) Authorize(paymentMethodID string, _ billing.Money) string {
	if paymentMethodID == DeclineMethod {
		return "card_declined"
	}
	return ""
}
