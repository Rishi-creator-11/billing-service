package billing

import "time"

// Post builds the balanced pair of entries for moving `amount` between the
// debit and credit accounts. Both entries share transactionID and sum to zero.
func Post(transactionID, customerID, referenceID string, amount Money, debitAcct, creditAcct string, at time.Time, newID func() string) []LedgerEntry {
	// Entries of one transaction share a timestamp and the ledger is ordered by
	// (time, entry ID), so the debit gets the smaller ID and always lists first.
	debitID, creditID := newID(), newID()
	if debitID > creditID {
		debitID, creditID = creditID, debitID
	}
	return []LedgerEntry{
		{ID: debitID, TransactionID: transactionID, CustomerID: customerID, Account: debitAcct, Direction: Debit, Amount: amount, ReferenceID: referenceID, CreatedAt: at},
		{ID: creditID, TransactionID: transactionID, CustomerID: customerID, Account: creditAcct, Direction: Credit, Amount: amount, ReferenceID: referenceID, CreatedAt: at},
	}
}

// ChargePostings: cash comes in (debit), revenue is recognised (credit).
func ChargePostings(txnID, customerID, chargeID string, amount Money, at time.Time, newID func() string) []LedgerEntry {
	return Post(txnID, customerID, chargeID, amount, AccountCash, AccountRevenue, at, newID)
}

// RefundPostings reverse a charge: revenue is reduced (debit), cash goes out (credit).
func RefundPostings(txnID, customerID, refundID string, amount Money, at time.Time, newID func() string) []LedgerEntry {
	return Post(txnID, customerID, refundID, amount, AccountRevenue, AccountCash, at, newID)
}

// SumByCurrency returns the signed sum of entries per currency. For a
// consistent ledger every value is zero.
func SumByCurrency(entries []LedgerEntry) map[string]int64 {
	out := map[string]int64{}
	for _, e := range entries {
		out[e.Amount.Currency] += e.Signed()
	}
	return out
}
