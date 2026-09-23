package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/options"
)

// Check the actual persisted catalog evidence, not just caller supplied flags.
func (c *Client) validateLiveMetadata(ctx context.Context, r options.LiveRun, e options.LiveEnvelope) error {
	rules := map[string]bool{}
	for _, b := range e.Books {
		for _, q := range b.Quality {
			if q.MarketKnown {
				if q.TradingRuleID == "" || q.MarketStateBasis != 2 || !q.MarketStateAt.Equal(q.RulePublishedAt) {
					return fmt.Errorf("live market state lacks catalog reference")
				}
				rules[q.TradingRuleID] = true
			}
		}
	}
	if len(rules) == 0 {
		return nil
	}
	ids := make([]uint32, len(r.Members))
	for n, m := range r.Members {
		ids[n] = m.InstrumentID
	}
	specs, err := c.LoadDerivativeSpecs(ctx, ids)
	if err != nil {
		return err
	}
	var ruleIDs []string
	for id := range rules {
		ruleIDs = append(ruleIDs, id)
	}
	rows, err := c.conn.Query(ctx, `SELECT `+metadataColumns+` FROM `+c.table("options_metadata_observation")+` FINAL WHERE run_id=? AND trading_rule_id IN (?) AND status='complete'`, r.ID, ruleIDs)
	if err != nil {
		return err
	}
	defer rows.Close()
	byRule := map[string]options.MetadataObservation{}
	for rows.Next() {
		var o options.MetadataObservation
		var hash string
		if err = rows.Scan(&o.RunID, &o.AttemptID, &o.InstrumentID, &o.Symbol, &o.Scope, &o.SourceURL, &o.RequestedAt, &o.ObservedAt, &o.PayloadHash, &o.Status, &o.DefinitionHash, &o.TradingRuleID, &o.State, &o.Active, &o.ScopeComplete, &o.ScopeRawCount, &o.ScopeAcceptedCount, &o.ScopeExcludedCount, &hash); err != nil {
			return err
		}
		o.RequestedAt = o.RequestedAt.UTC()
		o.ObservedAt = o.ObservedAt.UTC()
		if err = o.Validate(); err != nil {
			return err
		}
		if hash != o.Hash() {
			return fmt.Errorf("metadata evidence hash mismatch")
		}
		if _, ok := byRule[o.TradingRuleID]; ok {
			return fmt.Errorf("ambiguous live metadata evidence")
		}
		byRule[o.TradingRuleID] = o
	}
	if err = rows.Err(); err != nil {
		return err
	}
	for n, b := range e.Books {
		for sec, q := range b.Quality {
			if !q.MarketKnown {
				continue
			}
			o, ok := byRule[q.TradingRuleID]
			if !ok || o.InstrumentID != b.InstrumentID || o.Symbol != r.Members[n].Symbol || o.DefinitionHash != r.Members[n].DefinitionHash || q.RulePublishedAt.Before(o.ObservedAt) {
				return fmt.Errorf("missing/future/wrong live metadata evidence")
			}
			open := o.Active && o.State == "open" && e.MinuteTime.Add(time.Duration(sec)*time.Second).Before(*specs[n].Instrument.ExpiryTime)
			if q.MarketOpen != open {
				return fmt.Errorf("live market state disagrees with catalog/expiry")
			}
		}
	}
	return nil
}
