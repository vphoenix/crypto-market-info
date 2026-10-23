package justlendkeeper

import (
	"strings"
	"time"
)

// Exhaust the source's available history with the existing paginated adapter.
// This discovers old still-open orders without querying every order's state.
func (c *Collector) ensureDynamicHistory() {
	w := c.State.Dynamic
	if !c.DynamicSelection || w == nil || !w.Ready {
		return
	}
	if w.HistoryTo.IsZero() {
		w.HistoryTo = c.dynamicCoverage()
		if w.HistoryTo.IsZero() {
			return
		}
	}
	if w.HistoryDone == nil {
		w.HistoryDone = map[string]bool{}
	}
	for _, kind := range []string{"RentResource", "ReturnResource", "Liquidate"} {
		if w.HistoryDone[kind] {
			continue
		}
		id := "dynamic-history:" + kind + ":" + w.HistoryTo.Format(time.RFC3339Nano)
		exists := false
		for _, sc := range c.State.Scans {
			if sc.ID == id {
				exists = true
				break
			}
		}
		if !exists {
			c.State.Scans = append(c.State.Scans, Scan{ID: id, Kind: kind, Mode: "catchup", From: time.Unix(0, 0).UTC(), To: w.HistoryTo})
		}
	}
}
func (c *Collector) completeDynamicHistory(sc Scan) {
	w := c.State.Dynamic
	if w == nil || !strings.HasPrefix(sc.ID, "dynamic-history:") || !sc.Done || sc.Failed {
		return
	}
	if w.HistoryDone == nil {
		w.HistoryDone = map[string]bool{}
	}
	w.HistoryDone[sc.Kind] = true
	if w.HistoryDone["RentResource"] && w.HistoryDone["ReturnResource"] && w.HistoryDone["Liquidate"] {
		// Pagination exhaustion is a provider claim, not independent proof that
		// the provider retained every event since the contract was deployed.
		w.CoverageScope = "all_available_contract_history_provider_indexed"
	}
}

func (c *Collector) allowDynamicHistory(sc Scan) bool {
	w := c.State.Dynamic
	if w == nil {
		return false
	}
	now := c.Now()
	coverage := c.dynamicCoverage()
	if now.Before(w.HistoryNext) || coverage.IsZero() || now.Sub(coverage) > 5*time.Minute || c.State.Sources["trongrid"].Blocked {
		return false
	}
	kinds := []string{"RentResource", "ReturnResource", "Liquidate"}
	for i := 0; i < 3; i++ {
		index := (int(w.HistoryTurn) + i) % 3
		if w.HistoryDone[kinds[index]] {
			continue
		}
		if sc.Kind != kinds[index] {
			return false
		}
		w.HistoryTurn = uint8((index + 1) % 3)
		w.HistoryNext = UTC(now.Add(time.Minute))
		return true
	}
	return false
}
