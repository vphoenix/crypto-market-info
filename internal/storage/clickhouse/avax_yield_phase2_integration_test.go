package clickhouse

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/shopspring/decimal"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
	"github.com/vphoenix/crypto-market-info/internal/yield/ankr"
	"github.com/vphoenix/crypto-market-info/internal/yield/avalanche"
	"github.com/vphoenix/crypto-market-info/internal/yield/benqi"
)

func TestClickHouseAVAXPhase2ColumnsMigrationAndRoundTrip(t *testing.T) {
	if os.Getenv("CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKHOUSE_INTEGRATION=1 to run isolated ClickHouse integration tests")
	}
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy_%t", legacy), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			database := fmt.Sprintf("crypto_market_info_avax_phase2_it_%d", time.Now().UnixNano())
			address := envOr("CLICKHOUSE_TEST_ADDR", "127.0.0.1:9000")
			client, err := Open(ctx, Config{Addresses: []string{address}, Database: database, Username: "default", WriteTimeout: 5 * time.Second, MaxAttempts: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = client.Close()
				admin, openErr := ch.Open(&ch.Options{Addr: []string{address}, Auth: ch.Auth{Database: "default", Username: "default"}})
				if openErr != nil {
					t.Errorf("open cleanup for %s: %v", database, openErr)
					return
				}
				defer admin.Close()
				if dropErr := admin.Exec(context.Background(), "DROP DATABASE IF EXISTS `"+database+"` SYNC"); dropErr != nil {
					t.Errorf("drop isolated test database %s: %v", database, dropErr)
				}
			}()
			at := time.Date(2026, 8, 26, 16, 57, 19, 123000000, time.UTC)
			oldBatch := integrationYieldBatch("TRON", "legacy-source", 1, at.Add(-time.Hour))
			if legacy {
				// Build the actual previous schema without the new columns. Do not
				// drop columns or mutate a production database to simulate migration.
				statements, schemaErr := SchemaStatements(database)
				if schemaErr != nil {
					t.Fatal(schemaErr)
				}
				for _, statement := range statements[1:] {
					if strings.Contains(statement, "ADD COLUMN IF NOT EXISTS pool_cash") {
						continue
					}
					statement = strings.ReplaceAll(statement, "    pool_cash Nullable(Decimal(38, 18)),\n", "")
					statement = strings.ReplaceAll(statement, "    redemption_window_seconds Nullable(UInt64),\n", "")
					if err = client.conn.Exec(ctx, statement); err != nil {
						t.Fatal(err)
					}
				}
				var columnsBeforeMigration uint64
				if err = client.conn.QueryRow(ctx, `SELECT count() FROM system.columns WHERE database=? AND table='yield_observation' AND name IN ('pool_cash','redemption_window_seconds')`, database).Scan(&columnsBeforeMigration); err != nil || columnsBeforeMigration != 0 {
					t.Fatalf("legacy fixture already has %d new columns before migration: %v", columnsBeforeMigration, err)
				}
				ids, registerErr := client.registerYieldRoutes(ctx, oldBatch.Items)
				if registerErr != nil {
					t.Fatal(registerErr)
				}
				// An old observation genuinely exists before ALTER. Omitted
				// historical nullable columns remain unknown rather than zero.
				if err = client.conn.Exec(ctx, `INSERT INTO `+client.table("yield_observation")+`
					(yield_route_id,observation_time,collected_at,tier_no,tier_mode,rate_kind,rate_origin,rate_mode,rule_principal_loss_mode,rule_eligibility,availability)
					VALUES (?, fromUnixTimestamp64Milli(?), fromUnixTimestamp64Milli(?), 1, 'none', 'unknown', 'reported', 'variable', 'none', 'candidate', 'unknown')`, ids[0], oldBatch.Items[0].Observation.ObservationTime.UnixMilli(), oldBatch.CollectedAt.UnixMilli()); err != nil {
					t.Fatal(err)
				}
			} else {
				if err = client.InitSchema(ctx); err != nil {
					t.Fatal(err)
				}
				if err = client.WriteYieldBatch(ctx, oldBatch); err != nil {
					t.Fatal(err)
				}
			}
			for range 2 {
				if err = client.InitSchema(ctx); err != nil {
					t.Fatal(err)
				}
			}
			var newColumns uint64
			if err = client.conn.QueryRow(ctx, `SELECT count() FROM system.columns WHERE database=? AND table='yield_observation' AND name IN ('pool_cash','redemption_window_seconds')`, database).Scan(&newColumns); err != nil || newColumns != 2 {
				t.Fatalf("new columns=%d err=%v", newColumns, err)
			}
			var oldCashNull, oldWindowNull bool
			if err = client.conn.QueryRow(ctx, `SELECT isNull(pool_cash),isNull(redemption_window_seconds) FROM `+client.table("yield_observation")+` FINAL`).Scan(&oldCashNull, &oldWindowNull); err != nil || !oldCashNull || !oldWindowNull {
				t.Fatalf("migration invented old fields: cash_null=%t window_null=%t err=%v", oldCashNull, oldWindowNull, err)
			}
			// An unchanged old collector can still write after both ALTER runs.
			oldBatch.CollectedAt = oldBatch.CollectedAt.Add(time.Minute)
			oldBatch.Items[0].Observation.ObservationTime = oldBatch.CollectedAt
			oldBatch.Items[0].Observation.CollectedAt = oldBatch.CollectedAt
			if err = client.WriteYieldBatch(ctx, oldBatch); err != nil {
				t.Fatal(err)
			}
			batch := phase2StorageFixture(at)
			if err = batch.NormalizeAndValidateForLiveCollection(); err != nil {
				t.Fatal(err)
			}
			ids, err := client.registerYieldRoutes(ctx, batch.Items)
			if err != nil {
				t.Fatal(err)
			}
			failedCtx, stop := context.WithCancel(ctx)
			stop()
			if err = client.WriteYieldBatch(failedCtx, batch); err == nil {
				t.Fatal("cancelled write unexpectedly succeeded")
			}
			if err = client.InitYieldRegistry(ctx); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err = client.WriteYieldBatch(ctx, batch); err != nil {
					t.Fatal(err)
				}
			}
			for index, item := range batch.Items {
				var routeID uint32
				var cash, rate *decimal.Decimal
				var window *uint64
				var hash string
				var sourceTime, collectedTime time.Time
				// Positional time.Time query parameters default to second precision
				// in clickhouse-go. Bind Unix milliseconds explicitly, unlike the
				// native batch writer which already honors DateTime64(3).
				if err = client.conn.QueryRow(ctx, `SELECT o.yield_route_id,o.pool_cash,o.redemption_window_seconds,o.rate,assumeNotNull(o.source_payload_hash),o.observation_time,o.collected_at
					FROM `+client.table("yield_observation")+` AS o FINAL
					INNER JOIN (SELECT yield_route_id,product_code FROM `+client.table("yield_route")+` FINAL) AS r USING (yield_route_id)
					WHERE r.product_code=? AND o.observation_time=fromUnixTimestamp64Milli(?)`, item.Route.ProductCode, item.Observation.ObservationTime.UnixMilli()).Scan(&routeID, &cash, &window, &rate, &hash, &sourceTime, &collectedTime); err != nil {
					t.Fatal(err)
				}
				want := item.Observation
				if routeID != ids[index] || cash == nil || !cash.Equal(*want.PoolCash) || (window == nil) != (want.RedemptionWindowSeconds == nil) ||
					(window != nil && *window != *want.RedemptionWindowSeconds) || (rate == nil) != (want.Rate == nil) || (rate != nil && !rate.Equal(*want.Rate)) || hash != *want.SourcePayloadHash ||
					!sourceTime.Equal(want.ObservationTime) || !collectedTime.Equal(want.CollectedAt) {
					t.Fatalf("round-trip changed %s: route=%d cash=%v window=%v rate=%v hash=%s", item.Route.ProductCode, routeID, cash, window, rate, hash)
				}
			}
			var routes, observations, oldNullRows uint64
			if err = client.conn.QueryRow(ctx, `SELECT count() FROM `+client.table("yield_route")+` FINAL`).Scan(&routes); err != nil {
				t.Fatal(err)
			}
			if err = client.conn.QueryRow(ctx, `SELECT count(),countIf(isNull(pool_cash) AND isNull(redemption_window_seconds)) FROM `+client.table("yield_observation")+` FINAL`).Scan(&observations, &oldNullRows); err != nil {
				t.Fatal(err)
			}
			if routes != 4 || observations != 6 || oldNullRows != 2 {
				t.Fatalf("FINAL routes=%d observations=%d legacy_null_rows=%d", routes, observations, oldNullRows)
			}
		})
	}
}

func phase2StorageFixture(at time.Time) marketyield.Batch {
	batch := marketyield.Batch{Source: "avax-phase2-storage-fixture", CollectedAt: at}
	for i, product := range []struct{ provider, code, token, contract string }{
		{"BENQI", "avalanche-savax-staking", benqi.SAVAX, benqi.SAVAX},
		{"Ankr", "avalanche-ankravax-staking", ankr.Token, ankr.Pool},
		{"BENQI", "avalanche-qiavax-supply", benqi.QiAVAX, benqi.QiAVAX},
	} {
		item := integrationYieldBatch(product.provider, product.code, 1, at).Items[0]
		r := &item.Route
		r.ProviderType, r.ProductCode, r.YieldType = "protocol", product.code, "liquid_staking"
		r.DepositAssetKey, r.RedeemAssetKey, r.PositionAssetKey = avalanche.NativeAsset, avalanche.NativeAsset, "eip155:43114:erc20:"+product.token
		r.Network, r.ContractAddress, r.PriceExposureAsset = marketyield.Ptr(avalanche.Network), marketyield.Ptr(product.contract), marketyield.Ptr("AVAX")
		o := &item.Observation
		o.ObservationTime = at.Add(-time.Minute)
		o.Rate, o.RateKind, o.RateOrigin = nil, "unknown", "reported"
		o.RewardAssetKeys, o.RewardComponentRates = []string{avalanche.NativeAsset}, []*decimal.Decimal{nil}
		o.BlockHeight, o.BlockHash, o.Finality = marketyield.Ptr(uint64(93742828)), marketyield.Ptr("0x4c621d56b2a6749290b40b6e31b78cd7ecafe02bbcd723ff6c82ea6fa2063d48"), marketyield.Ptr("finalized")
		o.PoolCash = marketyield.Ptr(decimal.RequireFromString([]string{"0", "0.000000000000000001", "99999999999999999999.999999999999999999"}[i]))
		if i == 0 {
			o.RedemptionWindowSeconds, o.UnbondingSeconds = marketyield.Ptr(uint64(172800)), marketyield.Ptr(uint64(1296000))
		} else if i == 1 {
			o.RulePrincipalLossMode, o.RuleEligibility, o.Availability = "unknown", "unknown", "unknown"
		} else {
			r.YieldType, r.IncomeSource = "lending", "borrow_interest"
			o.Rate, o.RateKind, o.RateOrigin = marketyield.Ptr(decimal.RequireFromString("0.008432715236256")), "apr", "derived"
			o.RewardComponentRates = []*decimal.Decimal{o.Rate}
		}
		batch.Items = append(batch.Items, item)
	}
	zeroWindow := batch.Items[0]
	zeroWindow.Observation.ObservationTime = at
	zeroWindow.Observation.RedemptionWindowSeconds = marketyield.Ptr(uint64(0))
	zeroWindow.Observation.RuleEligibility, zeroWindow.Observation.EligibilityReason = "unknown", marketyield.Ptr("redemption_window_empty")
	batch.Items = append(batch.Items, zeroWindow)
	return batch
}
