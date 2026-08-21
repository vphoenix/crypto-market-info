package clickhouse

import (
	"context"
	"fmt"
	"math"

	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

type yieldRouteEntry struct {
	ID         uint32
	Definition marketyield.YieldRouteDefinition
}

func (c *Client) InitYieldRegistry(ctx context.Context) error {
	if c == nil || c.conn == nil {
		return fmt.Errorf("ClickHouse client is nil")
	}
	c.yieldMu.Lock()
	defer c.yieldMu.Unlock()
	return c.loadYieldRoutesLocked(ctx)
}

func (c *Client) WriteYieldBatch(ctx context.Context, value marketyield.Batch) error {
	if err := value.NormalizeAndValidate(); err != nil {
		return err
	}
	ids, err := c.registerYieldRoutes(ctx, value.Items)
	if err != nil {
		return err
	}
	for index := range value.Items {
		if ids[index] == 0 || value.Items[index].Route.Identity() == "" {
			return fmt.Errorf("invalid route assignment for item %d", index)
		}
	}
	if err = c.retryWrite(ctx, func(writeCtx context.Context) error { return c.insertYieldObservations(writeCtx, value.Items, ids) }); err != nil {
		return fmt.Errorf("write yield observations: %w", err)
	}
	return nil
}

func (c *Client) registerYieldRoutes(ctx context.Context, items []marketyield.CollectedYield) ([]uint32, error) {
	c.yieldMu.Lock()
	defer c.yieldMu.Unlock()
	if !c.yieldLoaded {
		if err := c.loadYieldRoutesLocked(ctx); err != nil {
			return nil, err
		}
	}
	working := make(map[string]yieldRouteEntry, len(c.yieldByKey)+len(items))
	for key, entry := range c.yieldByKey {
		working[key] = entry
	}
	next := c.yieldMaxID
	ids := make([]uint32, len(items))
	newRoutes := make([]marketyield.YieldRouteDefinition, 0)
	for index, item := range items {
		definition := item.Route
		definition.ID = 0
		key := definition.Identity()
		if stored, ok := working[key]; ok {
			if !definition.SameDefinition(stored.Definition) {
				return nil, fmt.Errorf("yield route %s conflicts with stored definition", definition.ProductCode)
			}
			ids[index] = stored.ID
			continue
		}
		if next == math.MaxUint32 {
			return nil, fmt.Errorf("yield_route_id space exhausted")
		}
		next++
		definition.ID = next
		ids[index] = next
		entryDefinition := definition
		entryDefinition.ID = 0
		working[key] = yieldRouteEntry{ID: next, Definition: entryDefinition}
		newRoutes = append(newRoutes, definition)
	}
	if len(newRoutes) > 0 {
		if err := c.retryWrite(ctx, func(writeCtx context.Context) error { return c.insertYieldRoutes(writeCtx, newRoutes) }); err != nil {
			c.yieldLoaded = false
			return nil, fmt.Errorf("register yield routes: %w", err)
		}
	}
	c.yieldByKey, c.yieldMaxID, c.yieldLoaded = working, next, true
	return ids, nil
}

func (c *Client) loadYieldRoutesLocked(ctx context.Context) error {
	rows, err := c.conn.Query(ctx, `SELECT yield_route_id,provider_type,provider,product_code,product_name,yield_type,deposit_asset_key,position_asset_key,redeem_asset_key,network,contract_address,price_exposure_asset,income_source,source_url,collection_enabled FROM `+c.table("yield_route")+` FINAL ORDER BY yield_route_id`)
	if err != nil {
		return fmt.Errorf("load yield routes: %w", err)
	}
	defer rows.Close()
	registry := make(map[string]yieldRouteEntry)
	var maxID uint32
	for rows.Next() {
		var route marketyield.YieldRouteDefinition
		if err = rows.Scan(&route.ID, &route.ProviderType, &route.Provider, &route.ProductCode, &route.ProductName, &route.YieldType, &route.DepositAssetKey, &route.PositionAssetKey, &route.RedeemAssetKey, &route.Network, &route.ContractAddress, &route.PriceExposureAsset, &route.IncomeSource, &route.SourceURL, &route.CollectionEnabled); err != nil {
			return err
		}
		if route.ID == 0 {
			return fmt.Errorf("stored yield route has id zero")
		}
		id := route.ID
		route.ID = 0
		if err = route.ValidateDefinition(); err != nil {
			return fmt.Errorf("invalid stored yield route %d: %w", id, err)
		}
		key := route.Identity()
		if previous, ok := registry[key]; ok && (previous.ID != id || !route.SameDefinition(previous.Definition)) {
			return fmt.Errorf("conflicting stored yield route identity %s", route.ProductCode)
		}
		registry[key] = yieldRouteEntry{ID: id, Definition: route}
		if id > maxID {
			maxID = id
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	c.yieldByKey, c.yieldMaxID, c.yieldLoaded = registry, maxID, true
	return nil
}

func (c *Client) insertYieldRoutes(ctx context.Context, routes []marketyield.YieldRouteDefinition) error {
	batch, err := c.conn.PrepareBatch(ctx, `INSERT INTO `+c.table("yield_route")+` (yield_route_id,provider_type,provider,product_code,product_name,yield_type,deposit_asset_key,position_asset_key,redeem_asset_key,network,contract_address,price_exposure_asset,income_source,source_url,collection_enabled)`)
	if err != nil {
		return err
	}
	for _, route := range routes {
		if err = batch.Append(route.ID, route.ProviderType, route.Provider, route.ProductCode, route.ProductName, route.YieldType, route.DepositAssetKey, route.PositionAssetKey, route.RedeemAssetKey, route.Network, route.ContractAddress, route.PriceExposureAsset, route.IncomeSource, route.SourceURL, route.CollectionEnabled); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *Client) insertYieldObservations(ctx context.Context, items []marketyield.CollectedYield, ids []uint32) error {
	batch, err := c.conn.PrepareBatch(ctx, `INSERT INTO `+c.table("yield_observation")+` (yield_route_id,observation_time,collected_at,tier_no,tier_min_amount,tier_max_amount,tier_mode,rate,rate_kind,rate_origin,rate_mode,reward_asset_keys,reward_component_rates,entry_fee_rate,exit_fee_rate,fixed_penalty_rate,performance_fee_rate,entry_fee_amount,exit_fee_amount,fixed_fee_asset_key,lock_seconds,unbonding_seconds,rule_principal_loss_mode,fixed_principal_loss_rate,rule_eligibility,eligibility_reason,exposure_ratio,capacity,remaining_capacity,tvl,availability,block_height,block_hash,finality,source_payload_hash)`)
	if err != nil {
		return err
	}
	for index, item := range items {
		o := item.Observation
		if err = batch.Append(ids[index], o.ObservationTime.UTC(), o.CollectedAt.UTC(), o.TierNo, o.TierMinAmount, o.TierMaxAmount, o.TierMode, o.Rate, o.RateKind, o.RateOrigin, o.RateMode, o.RewardAssetKeys, o.RewardComponentRates, o.EntryFeeRate, o.ExitFeeRate, o.FixedPenaltyRate, o.PerformanceFeeRate, o.EntryFeeAmount, o.ExitFeeAmount, o.FixedFeeAssetKey, o.LockSeconds, o.UnbondingSeconds, o.RulePrincipalLossMode, o.FixedPrincipalLossRate, o.RuleEligibility, o.EligibilityReason, o.ExposureRatio, o.Capacity, o.RemainingCapacity, o.TVL, o.Availability, o.BlockHeight, o.BlockHash, o.Finality, o.SourcePayloadHash); err != nil {
			return err
		}
	}
	return batch.Send()
}
