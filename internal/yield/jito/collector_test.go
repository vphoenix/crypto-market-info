package jito

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/yield/solana"
)

type checkingReader struct {
	calls  int
	config solana.PoolConfig
	err    error
}

func (r *checkingReader) Read(_ context.Context, config solana.PoolConfig) (solana.PoolSnapshot, error) {
	r.calls++
	r.config = config
	return solana.PoolSnapshot{}, r.err
}

func TestCollectorValidatesIdentityAndJoinsHistoryByExactDate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/stake_pool_stats":
			fmt.Fprint(w, `{"apy":[{"data":0.05,"date":"2026-08-24T00:00:00Z"},{"data":0.06,"date":"2026-08-23T00:00:00Z"}],"tvl":[{"data":2000000000,"date":"2026-08-23T00:00:00Z"},{"data":3000000000,"date":"2026-08-24T00:00:00Z"}]}`)
		case "/api/v1/jitosol_sol_ratio":
			fmt.Fprint(w, `{"ratios":[{"data":1.2,"date":"2026-08-24T00:00:00Z"},{"data":1.1,"date":"2026-08-23T00:00:00Z"}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	reader := &checkingReader{}
	collector := NewCollector(server.URL, reader)
	collector.Now = func() time.Time { return time.Date(2026, 8, 24, 1, 0, 0, 0, time.UTC) }
	batch, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if reader.calls != 1 || reader.config.Program != solana.StakePoolProgram || reader.config.Address != solana.JitoPoolAddress || reader.config.Mint != solana.JitoMintAddress {
		t.Fatalf("identity config=%+v", reader.config)
	}
	if len(batch.Items) != 2 {
		t.Fatalf("items=%d", len(batch.Items))
	}
	first, second := batch.Items[0].Observation, batch.Items[1].Observation
	if first.ObservationTime.Day() != 23 || first.Rate.String() != "0.06" || first.TVL.String() != "2" || first.ExposureRatio.String() != "1.1" {
		t.Fatalf("first=%+v", first)
	}
	if second.Rate.String() != "0.05" || second.TVL.String() != "3" || second.BlockHeight != nil || second.Finality != nil || second.SourcePayloadHash == nil {
		t.Fatalf("second=%+v", second)
	}
	if err = batch.NormalizeAndValidateForLiveCollection(); err != nil {
		t.Fatal(err)
	}
}

func TestCollectorRejectsDuplicateOrStaleJitoHistory(t *testing.T) {
	for name, fixture := range map[string]struct{ stats, ratio string }{
		"duplicate": {
			stats: `{"apy":[{"data":0.05,"date":"2026-08-24T00:00:00Z"},{"data":0.06,"date":"2026-08-24T00:00:00Z"}],"tvl":[{"data":1,"date":"2026-08-24T00:00:00Z"}]}`,
			ratio: `{"ratios":[{"data":1,"date":"2026-08-24T00:00:00Z"}]}`,
		},
		"stale": {
			stats: `{"apy":[{"data":0.05,"date":"2026-08-10T00:00:00Z"}],"tvl":[{"data":1,"date":"2026-08-10T00:00:00Z"}]}`,
			ratio: `{"ratios":[{"data":1,"date":"2026-08-10T00:00:00Z"}]}`,
		},
		"missing array": {
			stats: `{"apy":[{"data":0.05,"date":"2026-08-24T00:00:00Z"}]}`,
			ratio: `{"ratios":[{"data":1,"date":"2026-08-24T00:00:00Z"}]}`,
		},
		"future": {
			stats: `{"apy":[{"data":0.05,"date":"2026-08-24T01:10:00Z"}],"tvl":[{"data":1,"date":"2026-08-24T01:10:00Z"}]}`,
			ratio: `{"ratios":[{"data":1,"date":"2026-08-24T01:10:00Z"}]}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/v1/stake_pool_stats" {
					fmt.Fprint(w, fixture.stats)
					return
				}
				fmt.Fprint(w, fixture.ratio)
			}))
			defer server.Close()
			collector := NewCollector(server.URL, &checkingReader{})
			collector.Now = func() time.Time { return time.Date(2026, 8, 24, 1, 0, 0, 0, time.UTC) }
			if _, err := collector.Collect(context.Background()); err == nil {
				t.Fatal("invalid history accepted")
			}
		})
	}
}
