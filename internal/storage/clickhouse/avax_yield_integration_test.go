package clickhouse

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
	"github.com/vphoenix/crypto-market-info/internal/yield/aave"
	"github.com/vphoenix/crypto-market-info/internal/yield/okxearn"
)

// Only a unique test database is created/dropped; API data is supplied by local
// fixtures. This never uses CLICKHOUSE_DATABASE or a production collector.
func TestClickHouseAVAXYieldCollectorsHistoryAndRetry(t *testing.T) {
	if os.Getenv("CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKHOUSE_INTEGRATION=1 to run ClickHouse integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	database := fmt.Sprintf("crypto_market_info_avax_yield_it_%d", time.Now().UnixNano())
	address := envOr("CLICKHOUSE_TEST_ADDR", "127.0.0.1:9000")
	client, err := Open(ctx, Config{Addresses: []string{address}, Database: database, Username: "default", WriteTimeout: 5 * time.Second, MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = client.Close()
		admin, openErr := ch.Open(&ch.Options{Addr: []string{address}, Auth: ch.Auth{Database: "default", Username: "default"}})
		if openErr != nil {
			t.Errorf("open test database cleanup: %v", openErr)
			return
		}
		defer admin.Close()
		if dropErr := admin.Exec(context.Background(), "DROP DATABASE IF EXISTS `"+database+"` SYNC"); dropErr != nil {
			t.Errorf("drop test database %s: %v", database, dropErr)
		}
	}()
	if err = client.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err = client.InitYieldRegistry(ctx); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 8, 27, 0, 0, 0, 123000000, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/okx/api/v5/finance/savings/lending-rate-history":
			if r.URL.Query().Get("after") != "" {
				fmt.Fprint(w, `{"code":"0","data":[]}`)
				return
			}
			fmt.Fprintf(w, `{"code":"0","data":[{"ccy":"AVAX","ts":"%d","lendingRate":"0.0206","rate":"99"},{"ccy":"AVAX","ts":"%d","lendingRate":"0"}]}`, at.Add(-time.Hour).UnixMilli(), at.Add(-2*time.Hour).UnixMilli())
		case "/v3":
			fmt.Fprint(w, `{"data":{"market":{"address":"0x794a61358d6845594f94dc1db02a252b5b4814ad","chain":{"chainId":43114},"reserves":[{"underlyingToken":{"address":"0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7","decimals":18},"aToken":{"address":"0x6d80113e533a2c0fe82eabd35f1875dcea89ea97","decimals":18}}]},"supplyAPYHistory":[{"date":"2026-08-26T23:00:00.123Z","avgRate":{"raw":"6668427376540457481976400","decimals":27}},{"date":"2026-08-26T22:00:00.123Z","avgRate":{"raw":"0","decimals":27}}]}}`)
		case "/v4":
			fmt.Fprint(w, `{"data":{"reserve":{"id":"NDMxMTQ6OjB4NDM1MjcyQ2VmRjkzYTFFNjU3RThBQmZkZjBBMTNlOTU5MDBBM2E1Njo6MA==","onChainId":"0","chain":{"chainId":43114},"spoke":{"address":"0x435272ceff93a1e657e8abfdf0a13e95900a3a56"},"asset":{"underlying":{"address":"0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7","info":{"decimals":18}}}},"supplyApyHistory":[{"date":"2026-08-26T23:00:00.123Z","avgRate":{"normalized":"1.36082551852043339681246800"}},{"date":"2026-08-26T22:00:00.123Z","avgRate":{"normalized":"0"}}]}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	okxCollector := okxearn.NewCollector(server.URL + "/okx")
	v3, v4 := aave.NewV3Collector(server.URL+"/v3"), aave.NewV4Collector(server.URL+"/v4")
	okxCollector.Now, v3.Now, v4.Now = func() time.Time { return at }, func() time.Time { return at }, func() time.Time { return at }
	okxCollector.Retry.Cooldown = exchange.NewRequestGate(0)
	var batches []marketyield.Batch
	for _, collector := range []marketyield.Collector{okxCollector, v3, v4} {
		batch, collectErr := collector.Collect(ctx)
		if collectErr != nil {
			t.Fatal(collectErr)
		}
		if err = batch.NormalizeAndValidateForLiveCollection(); err != nil {
			t.Fatal(err)
		}
		batches = append(batches, batch)
	}
	// Simulate registration succeeding but observations failing. Retrying must
	// reuse the registered ID, even after loading the registry from the DB.
	ids, err := client.registerYieldRoutes(ctx, batches[0].Items)
	if err != nil {
		t.Fatal(err)
	}
	originalID := ids[0]
	failedCtx, stop := context.WithCancel(ctx)
	stop()
	if err = client.WriteYieldBatch(failedCtx, batches[0]); err == nil {
		t.Fatal("cancelled observation write unexpectedly succeeded")
	}
	if err = client.InitYieldRegistry(ctx); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		for _, batch := range batches {
			if err = client.WriteYieldBatch(ctx, batch); err != nil {
				t.Fatal(err)
			}
		}
	}
	var routeCount, observationCount, missingHashes uint64
	if err = client.conn.QueryRow(ctx, `SELECT count() FROM `+client.table("yield_route")+` FINAL`).Scan(&routeCount); err != nil {
		t.Fatal(err)
	}
	if err = client.conn.QueryRow(ctx, `SELECT count(),countIf(isNull(source_payload_hash)) FROM `+client.table("yield_observation")+` FINAL`).Scan(&observationCount, &missingHashes); err != nil {
		t.Fatal(err)
	}
	if routeCount != 3 || observationCount != 6 || missingHashes != 0 {
		t.Fatalf("routes=%d observations=%d missing_hashes=%d", routeCount, observationCount, missingHashes)
	}
	for i, want := range []string{"0.0206", "0.006668427376540457", "0.013608255185204333"} {
		code := batches[i].Items[0].Route.ProductCode
		var id uint32
		var rate decimal.Decimal
		var sourceTime, collectedTime time.Time
		var nullExposure bool
		if err = client.conn.QueryRow(ctx, `SELECT r.yield_route_id,assumeNotNull(o.rate),o.observation_time,o.collected_at,isNull(o.exposure_ratio)
			FROM `+client.table("yield_observation")+` AS o FINAL
			INNER JOIN (SELECT yield_route_id,product_code FROM `+client.table("yield_route")+` FINAL) AS r USING (yield_route_id)
			WHERE r.product_code=? ORDER BY o.observation_time DESC LIMIT 1`, code).Scan(&id, &rate, &sourceTime, &collectedTime, &nullExposure); err != nil {
			t.Fatal(err)
		}
		if rate.String() != want || !sourceTime.Equal(at.Add(-time.Hour)) || !collectedTime.Equal(at) || nullExposure != (i == 2) {
			t.Fatalf("%s: rate=%s source=%s collected=%s null_exposure=%t", code, rate, sourceTime, collectedTime, nullExposure)
		}
		if i == 0 && id != originalID {
			t.Fatalf("retry changed route ID: got=%d want=%d", id, originalID)
		}
	}
}
