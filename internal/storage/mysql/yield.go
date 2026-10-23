package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

const yieldRouteColumns = "yield_route_id,provider_type,provider,product_code,product_name,yield_type,deposit_asset_key,position_asset_key,redeem_asset_key,network,contract_address,price_exposure_asset,income_source,source_url,collection_enabled"
const yieldObservationColumns = "yield_route_id,observation_time,collected_at,tier_no,tier_min_amount,tier_max_amount,tier_mode,rate,rate_kind,rate_origin,rate_mode,reward_asset_keys,reward_component_rates,entry_fee_rate,exit_fee_rate,fixed_penalty_rate,performance_fee_rate,entry_fee_amount,exit_fee_amount,fixed_fee_asset_key,lock_seconds,unbonding_seconds,redemption_window_seconds,rule_principal_loss_mode,fixed_principal_loss_rate,rule_eligibility,eligibility_reason,exposure_ratio,capacity,remaining_capacity,tvl,pool_cash,availability,block_height,block_hash,finality,source_payload_hash"

func routeValues(r marketyield.YieldRouteDefinition) []any {
	return []any{r.ID, r.ProviderType, r.Provider, r.ProductCode, r.ProductName, r.YieldType, r.DepositAssetKey, r.PositionAssetKey, r.RedeemAssetKey, r.Network, r.ContractAddress, r.PriceExposureAsset, r.IncomeSource, r.SourceURL, r.CollectionEnabled}
}
func observationValues(id uint32, o marketyield.YieldObservation) []any {
	return []any{id, o.ObservationTime.UTC(), o.CollectedAt.UTC(), o.TierNo, o.TierMinAmount, o.TierMaxAmount, o.TierMode, o.Rate, o.RateKind, o.RateOrigin, o.RateMode, o.RewardAssetKeys, o.RewardComponentRates, o.EntryFeeRate, o.ExitFeeRate, o.FixedPenaltyRate, o.PerformanceFeeRate, o.EntryFeeAmount, o.ExitFeeAmount, o.FixedFeeAssetKey, o.LockSeconds, o.UnbondingSeconds, o.RedemptionWindowSeconds, o.RulePrincipalLossMode, o.FixedPrincipalLossRate, o.RuleEligibility, o.EligibilityReason, o.ExposureRatio, o.Capacity, o.RemainingCapacity, o.TVL, o.PoolCash, o.Availability, o.BlockHeight, o.BlockHash, o.Finality, o.SourcePayloadHash}
}
func routeModel(r record) (marketyield.YieldRouteDefinition, error) {
	v := marketyield.YieldRouteDefinition{}
	fields := []struct {
		name   string
		target any
	}{{"yield_route_id", &v.ID}, {"provider_type", &v.ProviderType}, {"provider", &v.Provider}, {"product_code", &v.ProductCode}, {"product_name", &v.ProductName}, {"yield_type", &v.YieldType}, {"deposit_asset_key", &v.DepositAssetKey}, {"position_asset_key", &v.PositionAssetKey}, {"redeem_asset_key", &v.RedeemAssetKey}, {"network", &v.Network}, {"contract_address", &v.ContractAddress}, {"price_exposure_asset", &v.PriceExposureAsset}, {"income_source", &v.IncomeSource}, {"source_url", &v.SourceURL}, {"collection_enabled", &v.CollectionEnabled}}
	for _, f := range fields {
		if e := codec.Assign(reflect.ValueOf(f.target).Elem(), r[f.name]); e != nil {
			return v, e
		}
	}
	definition := v
	definition.ID = 0
	return v, definition.ValidateDefinition()
}
func (c *Client) yieldRoutes(ctx context.Context, q executor) ([]marketyield.YieldRouteDefinition, error) {
	t, e := c.mapping("yield_route")
	if e != nil {
		return nil, e
	}
	rs, e := readRecords(ctx, q, c.table(t.Name), t, "ORDER BY yield_route_id")
	if e != nil {
		return nil, e
	}
	out := make([]marketyield.YieldRouteDefinition, len(rs))
	seen := map[string]uint32{}
	for i, r := range rs {
		out[i], e = routeModel(r)
		if e != nil {
			return nil, e
		}
		identity := out[i].Identity()
		if id, ok := seen[identity]; ok && id != out[i].ID {
			return nil, fmt.Errorf("ambiguous stored yield identity")
		}
		seen[identity] = out[i].ID
	}
	return out, nil
}
func (c *Client) InitYieldRegistry(ctx context.Context) error {
	_, e := c.yieldRoutes(ctx, c.db)
	return e
}
func (c *Client) WriteYieldBatch(ctx context.Context, b marketyield.Batch) error {
	if e := b.NormalizeAndValidate(); e != nil {
		return e
	}
	return c.transaction(ctx, func(tx *sql.Tx) error {
		if _, e := tx.ExecContext(ctx, "INSERT IGNORE INTO cmi_storage.identity_allocator(database_name,kind,next_id) VALUES(?,'yield',1)", c.database); e != nil {
			return e
		}
		var next uint64
		if e := tx.QueryRowContext(ctx, "SELECT next_id FROM cmi_storage.identity_allocator WHERE database_name=? AND kind='yield' FOR UPDATE", c.database).Scan(&next); e != nil {
			return e
		}
		routes, e := c.yieldRoutes(ctx, tx)
		if e != nil {
			return e
		}
		byIdentity := map[string]marketyield.YieldRouteDefinition{}
		for _, r := range routes {
			byIdentity[r.Identity()] = r
			if next <= uint64(r.ID) {
				next = uint64(r.ID) + 1
			}
		}
		for _, item := range b.Items {
			route := item.Route
			route.ID = 0
			if old, ok := byIdentity[route.Identity()]; ok {
				if !route.SameDefinition(old) {
					return fmt.Errorf("yield route immutable definition conflict")
				}
				route.ID = old.ID
				if !route.SameMetadata(old) {
					t, e := c.mapping("yield_route")
					if e != nil {
						return e
					}
					r, e := namedRecord(yieldRouteColumns, routeValues(route))
					if e != nil {
						return e
					}
					if _, e = tx.ExecContext(ctx, "DELETE FROM "+c.table(t.Name)+" WHERE yield_route_id=?", route.ID); e != nil {
						return e
					}
					if e = insertRecord(ctx, tx, c.table(t.Name), t, r); e != nil {
						return e
					}
				}
			} else {
				if next > math.MaxUint32 {
					return fmt.Errorf("yield route ID exhausted")
				}
				route.ID = uint32(next)
				next++
				if e = c.putValues(ctx, tx, "yield_route", yieldRouteColumns, routeValues(route)); e != nil {
					return e
				}
			}
			byIdentity[route.Identity()] = route
			if e = c.writeYieldObservation(ctx, tx, route.ID, item.Observation); e != nil {
				return e
			}
		}
		_, e = tx.ExecContext(ctx, "UPDATE cmi_storage.identity_allocator SET next_id=? WHERE database_name=? AND kind='yield'", next, c.database)
		return e
	})
}

// Source history endpoints return overlapping observation times on each poll.
// collected_at orders refreshed observations; a delivery retry keeps that time
// fixed. A stale retry never replaces a newer observation, and equal versions
// still require an exact full-row match.
func (c *Client) writeYieldObservation(ctx context.Context, tx *sql.Tx, id uint32, o marketyield.YieldObservation) error {
	t, err := c.mapping("yield_observation")
	if err != nil {
		return err
	}
	r, err := namedRecord(yieldObservationColumns, observationValues(id, o))
	if err != nil {
		return err
	}
	hash, err := rowHash(t, r)
	if err != nil {
		return err
	}
	where, args, err := rowPredicate(t, r)
	if err != nil {
		return err
	}
	old, err := readRecords(ctx, tx, c.table(t.Name), t, "WHERE "+where+" FOR UPDATE", args...)
	if err != nil {
		return err
	}
	if len(old) > 1 {
		return contracts.ErrConflict
	}
	if len(old) == 1 {
		previous := old[0]
		version := previous["collected_at"].(time.Time)
		if o.CollectedAt.Before(version) {
			return nil
		}
		if o.CollectedAt.Equal(version) {
			priorHash, e := rowHash(t, previous)
			if e != nil {
				return e
			}
			if hash != priorHash {
				return contracts.ErrConflict
			}
			return nil
		}
		// A refresh cannot change a known chain anchor or downgrade finality.
		for _, name := range []string{"block_height", "block_hash"} {
			if previous[name] != nil {
				var column *codec.Type
				for _, col := range t.Columns {
					if col.Name == name {
						column = col.Type
						break
					}
				}
				a, e := codec.Encode(column, previous[name])
				if e != nil {
					return e
				}
				b, e := codec.Encode(column, r[name])
				if e != nil {
					return e
				}
				if string(a) != string(b) {
					return contracts.ErrConflict
				}
			}
		}
		finalityRank := func(v any) int {
			if v == nil {
				return 0
			}
			var s string
			switch x := v.(type) {
			case string:
				s = x
			case *string:
				if x == nil {
					return 0
				}
				s = *x
			default:
				return -1
			}
			switch s {
			case "finalized", "finalized_anchor":
				return 3
			case "safe":
				return 2
			case "unfinalized":
				return 1
			}
			return -1
		}
		if finalityRank(r["finality"]) < finalityRank(previous["finality"]) {
			return contracts.ErrConflict
		}
		if _, err = tx.ExecContext(ctx, "DELETE FROM "+c.table(t.Name)+" WHERE "+where, args...); err != nil {
			return err
		}
	}
	return insertRecord(ctx, tx, c.table(t.Name), t, r)
}

var _ marketyield.Sink = (*Client)(nil)
