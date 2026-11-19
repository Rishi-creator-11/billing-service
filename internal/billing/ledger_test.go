package billing

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

// A transaction's entries share a timestamp, and the ledger sorts by
// (time, entry ID). The debit must therefore always sort first.
func TestPostListsDebitBeforeCredit(t *testing.T) {
	n := 0
	ids := []string{"z-late", "a-early", "m-mid", "b-next"} // deliberately out of order
	newID := func() string { id := ids[n%len(ids)]; n++; return id }
	for i := 0; i < 4; i++ {
		es := ChargePostings("t", "c", fmt.Sprint("ch", i), Money{AmountMinor: 100, Currency: "USD"}, time.Unix(0, 0), newID)
		sort.Slice(es, func(a, b int) bool { return es[a].ID < es[b].ID })
		if es[0].Direction != Debit || es[1].Direction != Credit {
			t.Fatalf("run %d: expected debit then credit, got %v then %v", i, es[0].Direction, es[1].Direction)
		}
	}
}
