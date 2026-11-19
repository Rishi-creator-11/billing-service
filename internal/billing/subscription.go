package billing

import "fmt"

// Event drives subscription state transitions.
type Event string

const (
	EventPaymentSucceeded Event = "PAYMENT_SUCCEEDED"
	EventPaymentDeclined  Event = "PAYMENT_DECLINED"
	EventCancel           Event = "CANCEL"
)

// TransitionError is returned for a transition the state machine forbids.
type TransitionError struct {
	From  SubscriptionState
	Event Event
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("subscription in state %s cannot handle event %s", e.From, e.Event)
}

// Next returns the state reached from `from` on event ev, or a
// *TransitionError if the transition is not allowed.
//
//	PENDING  --payment succeeded--> ACTIVE
//	PENDING  --payment declined-->  PENDING   (stays; nothing was ever paid)
//	ACTIVE   --payment succeeded--> ACTIVE
//	ACTIVE   --payment declined-->  PAST_DUE
//	PAST_DUE --payment succeeded--> ACTIVE
//	PAST_DUE --payment declined-->  PAST_DUE
//	any non-terminal --cancel-->    CANCELED
//	CANCELED                        terminal: only cancel (a no-op) is accepted
func Next(from SubscriptionState, ev Event) (SubscriptionState, error) {
	switch from {
	case StatePending:
		switch ev {
		case EventPaymentSucceeded:
			return StateActive, nil
		case EventPaymentDeclined:
			return StatePending, nil
		case EventCancel:
			return StateCanceled, nil
		}
	case StateActive:
		switch ev {
		case EventPaymentSucceeded:
			return StateActive, nil
		case EventPaymentDeclined:
			return StatePastDue, nil
		case EventCancel:
			return StateCanceled, nil
		}
	case StatePastDue:
		switch ev {
		case EventPaymentSucceeded:
			return StateActive, nil
		case EventPaymentDeclined:
			return StatePastDue, nil
		case EventCancel:
			return StateCanceled, nil
		}
	case StateCanceled:
		if ev == EventCancel {
			return StateCanceled, nil
		}
	}
	return "", &TransitionError{From: from, Event: ev}
}

// CanCharge reports whether a subscription in state s accepts new charges.
func CanCharge(s SubscriptionState) bool { return s != StateCanceled }
