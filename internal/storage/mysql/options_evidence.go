package mysql

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"slices"
	"time"
)

func (c *Client) validateCatalogEvidence(ctx context.Context, r options.LiveRun, e options.CatalogEnvelope) error {
	var catalogIDs, lifeIDs []uuid.UUID
	for _, q := range e.Evidence {
		for sec := 0; sec < 60; sec++ {
			if q.RuleObservationIDs[sec] != nil {
				catalogIDs = append(catalogIDs, *q.RuleObservationIDs[sec])
			}
			if q.StateIDs[sec] != nil {
				if q.StateKinds[sec] == 2 {
					catalogIDs = append(catalogIDs, *q.StateIDs[sec])
				} else {
					lifeIDs = append(lifeIDs, *q.StateIDs[sec])
				}
			}
			for _, id := range []*uuid.UUID{q.PlatformIDs[sec], q.MaintenanceIDs[sec], q.LockIDs[sec]} {
				if id != nil {
					lifeIDs = append(lifeIDs, *id)
				}
			}
		}
	}
	unique := func(ids []uuid.UUID) []uuid.UUID {
		slices.SortFunc(ids, func(a, b uuid.UUID) int { return slices.Compare(a[:], b[:]) })
		return slices.Compact(ids)
	}
	catalog, err := loadCatalogRows[options.CatalogObservation](ctx, c, "options_catalog_scope_observation", unique(catalogIDs))
	if err != nil {
		return err
	}
	lifecycle, err := loadCatalogRows[options.LifecycleObservation](ctx, c, "options_lifecycle_observation", unique(lifeIDs))
	if err != nil {
		return err
	}
	ids := make([]uint32, len(r.Members))
	for n, m := range r.Members {
		ids[n] = m.InstrumentID
	}
	specs, err := c.LoadDerivativeSpecs(ctx, ids)
	if err != nil {
		return err
	}
	for n, b := range e.Live.Books {
		proof := e.Evidence[n]
		for sec, q := range b.Quality {
			if !q.MarketKnown {
				if q.StreamValid {
					return fmt.Errorf("stream valid with unknown market")
				}
				continue
			}
			at := e.Live.MinuteTime.Add(time.Duration(sec) * time.Second)
			o, ok := catalog[*proof.RuleObservationIDs[sec]]
			if !ok || o.Status != "complete" || q.RulePublishedAt.Before(o.ObservedAt) || at.Sub(o.ObservedAt) > options.CatalogEvidenceMaxAge {
				return fmt.Errorf("missing/future/stale rule observation")
			}
			i := slices.Index(o.InstrumentIDs, b.InstrumentID)
			if i < 0 || o.Definitions[i] != r.Members[n].DefinitionHash || o.Rules[i] != q.TradingRuleID {
				return fmt.Errorf("wrong economic rule evidence")
			}
			open := false
			if proof.StateKinds[sec] == 2 {
				state := catalog[*proof.StateIDs[sec]]
				j := slices.Index(state.InstrumentIDs, b.InstrumentID)
				if state.Status != "complete" || j < 0 || state.Definitions[j] != r.Members[n].DefinitionHash || q.MarketStateAt.Before(state.ObservedAt) || at.Sub(state.ObservedAt) > options.CatalogEvidenceMaxAge {
					return fmt.Errorf("wrong catalog state evidence")
				}
				open = state.Active[j] && state.States[j] == "open"
			} else {
				state := lifecycle[*proof.StateIDs[sec]]
				if state.Kind != "state" || state.Symbol != r.Members[n].Symbol || q.MarketStateAt.Before(state.ReceivedAt) || state.Epoch != proof.LifecycleEpochs[sec] {
					return fmt.Errorf("wrong lifecycle state evidence")
				}
				open = state.State == "open"
			}
			platform := lifecycle[*proof.PlatformIDs[sec]]
			confirmed := proof.LifecycleConfirmed[sec]
			if platform.Kind != "status" || platform.Epoch != proof.LifecycleEpochs[sec] || platform.ReceivedAt.After(at) || confirmed == nil || confirmed.After(at) || at.Sub(*confirmed) > 30*time.Second {
				return fmt.Errorf("unknown/stale platform baseline")
			}
			if platform.LockMode == "true" || (platform.LockMode == "partial" && slices.Contains(platform.LockedIndexes, r.Members[n].IndexID)) {
				open = false
			}
			for _, id := range []*uuid.UUID{proof.MaintenanceIDs[sec], proof.LockIDs[sec]} {
				if id == nil {
					continue
				}
				v := lifecycle[*id]
				if v.Kind != "platform" || v.Epoch != platform.Epoch || v.ReceivedAt.After(at) {
					return fmt.Errorf("invalid platform event reference")
				}
				if v.Maintenance != nil && *v.Maintenance {
					open = false
				}
				if v.Locked != nil && v.IndexID == r.Members[n].IndexID && *v.Locked {
					open = false
				}
			}
			open = open && at.Before(*specs[n].Instrument.ExpiryTime)
			if q.MarketOpen != open {
				return fmt.Errorf("market state disagrees with persisted evidence")
			}
		}
	}
	return nil
}
