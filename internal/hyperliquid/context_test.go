package hyperliquid

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

var collected = time.Date(2026, 11, 2, 15, 40, 0, 0, time.UTC)

func fixture(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/xyz-20261102.json")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestParseAssetContextsKeepsListedAssetsExactly(t *testing.T) {
	raw := fixture(t)
	batch, err := ParseAssetContexts(raw, "xyz", collected)
	if err != nil {
		t.Fatal(err)
	}
	// 142 universe entries, 22 delisted with null book fields.
	if batch.UniverseCount != 142 || len(batch.Assets) != 120 {
		t.Fatalf("universe %d assets %d", batch.UniverseCount, len(batch.Assets))
	}
	digest := sha256.Sum256(raw)
	var source []json.RawMessage
	if err := json.Unmarshal(raw, &source); err != nil {
		t.Fatal(err)
	}
	var meta struct {
		Universe []struct {
			Name string `json:"name"`
		} `json:"universe"`
	}
	var contexts []map[string]any
	_ = json.Unmarshal(source[0], &meta)
	_ = json.Unmarshal(source[1], &contexts)
	byName := map[string]map[string]any{}
	for i, u := range meta.Universe {
		byName[u.Name] = contexts[i]
	}
	for _, a := range batch.Assets {
		if a.PayloadHash != string(digest[:]) || a.BatchAssetCount != 120 || a.Dex != "xyz" || !a.ObservationTime.Equal(collected) {
			t.Fatalf("%s: batch fields not shared", a.Coin)
		}
		src := byName[a.Coin]
		for key, got := range map[string]decimal.Decimal{"markPx": a.MarkPx, "oraclePx": a.OraclePx, "funding": a.FundingRateHourly, "openInterest": a.OpenInterest, "midPx": a.MidPx, "premium": a.Premium} {
			if !got.Equal(decimal.RequireFromString(src[key].(string))) {
				t.Fatalf("%s %s = %s, source %v", a.Coin, key, got, src[key])
			}
		}
		if a.BookValid != 1 {
			t.Fatalf("%s listed asset without book", a.Coin)
		}
	}
}

func TestParseAssetContextsStoresAbsentBookAsFlaggedZero(t *testing.T) {
	raw := []byte(`[{"universe":[{"name":"xyz:A","szDecimals":2,"maxLeverage":10}]},[{"funding":"-0.0000125","openInterest":"0","prevDayPx":"1.5","dayNtlVlm":"0","premium":null,"oraclePx":"1.5","markPx":"1.5","midPx":null,"impactPxs":null,"dayBaseVlm":"0"}]]`)
	batch, err := ParseAssetContexts(raw, "xyz", collected)
	if err != nil {
		t.Fatal(err)
	}
	a := batch.Assets[0]
	if a.BookValid != 0 || !a.MidPx.IsZero() || !a.Premium.IsZero() || !a.FundingRateHourly.Equal(decimal.RequireFromString("-0.0000125")) {
		t.Fatalf("absent book stored as %+v", a)
	}
}

func TestParseAssetContextsRejectsIncompleteOrMalformedResponses(t *testing.T) {
	good := `{"funding":"0.00001","openInterest":"1","prevDayPx":"2","dayNtlVlm":"3","premium":"0.0001","oraclePx":"2","markPx":"2","midPx":"2","impactPxs":["1.9","2.1"],"dayBaseVlm":"1"}`
	cases := map[string]string{
		"length mismatch":    `[{"universe":[{"name":"xyz:A","szDecimals":2,"maxLeverage":10},{"name":"xyz:B","szDecimals":2,"maxLeverage":10}]},[` + good + `]]`,
		"one element":        `[{"universe":[]}]`,
		"empty universe":     `[{"universe":[]},[]]`,
		"partial book":       `[{"universe":[{"name":"xyz:A","szDecimals":2,"maxLeverage":10}]},[` + strings.Replace(good, `"premium":"0.0001"`, `"premium":null`, 1) + `]]`,
		"missing field":      `[{"universe":[{"name":"xyz:A","szDecimals":2,"maxLeverage":10}]},[` + strings.Replace(good, `"funding":"0.00001",`, ``, 1) + `]]`,
		"number not string":  `[{"universe":[{"name":"xyz:A","szDecimals":2,"maxLeverage":10}]},[` + strings.Replace(good, `"markPx":"2"`, `"markPx":2`, 1) + `]]`,
		"exponent":           `[{"universe":[{"name":"xyz:A","szDecimals":2,"maxLeverage":10}]},[` + strings.Replace(good, `"funding":"0.00001"`, `"funding":"1e-5"`, 1) + `]]`,
		"19 digits":          `[{"universe":[{"name":"xyz:A","szDecimals":2,"maxLeverage":10}]},[` + strings.Replace(good, `"funding":"0.00001"`, `"funding":"0.0000000000000000001"`, 1) + `]]`,
		"wrong dex prefix":   `[{"universe":[{"name":"flx:A","szDecimals":2,"maxLeverage":10}]},[` + good + `]]`,
		"duplicate coin":     `[{"universe":[{"name":"xyz:A","szDecimals":2,"maxLeverage":10},{"name":"xyz:A","szDecimals":2,"maxLeverage":10}]},[` + good + `,` + good + `]]`,
		"zero mark":          `[{"universe":[{"name":"xyz:A","szDecimals":2,"maxLeverage":10}]},[` + strings.Replace(good, `"markPx":"2"`, `"markPx":"0"`, 1) + `]]`,
		"negative OI":        `[{"universe":[{"name":"xyz:A","szDecimals":2,"maxLeverage":10}]},[` + strings.Replace(good, `"openInterest":"1"`, `"openInterest":"-1"`, 1) + `]]`,
		"all delisted":       `[{"universe":[{"name":"xyz:A","szDecimals":2,"maxLeverage":10,"isDelisted":true}]},[` + good + `]]`,
		"bad delisted flag":  `[{"universe":[{"name":"xyz:A","szDecimals":2,"maxLeverage":10,"isDelisted":"no"}]},[` + good + `]]`,
		"trailing JSON data": `[{"universe":[{"name":"xyz:A","szDecimals":2,"maxLeverage":10}]},[` + good + `]] []`,
	}
	for name, body := range cases {
		if _, err := ParseAssetContexts([]byte(body), "xyz", collected); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
}

func TestContextCollectorPostsDexRequest(t *testing.T) {
	raw := fixture(t)
	var got map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &got)
		_, _ = w.Write(raw)
	}))
	defer server.Close()
	c := &ContextCollector{Client: NewClient(server.URL), Dex: "xyz", Now: func() time.Time { return collected.Add(123 * time.Microsecond) }}
	batch, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got["type"] != "metaAndAssetCtxs" || got["dex"] != "xyz" {
		t.Fatalf("request body %v", got)
	}
	if !batch.CollectedAt.Equal(collected) || batch.CollectedAt.Location() != time.UTC {
		t.Fatalf("collected_at %s not truncated to UTC milliseconds", batch.CollectedAt)
	}
}
