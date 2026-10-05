// options-check reads committed offline or live option batches, without writes.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/bits"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	addr := flag.String("addr", "127.0.0.1:9000", "ClickHouse native address")
	db := flag.String("database", "", "existing database (required)")
	profile := flag.String("profile", "offline", "offline or live")
	runID := flag.String("run", "", "run UUID (live defaults to complete collection plan)")
	at := flag.String("at", "", "exact UTC replay second, RFC3339 (live defaults to latest minute's last second)")
	id := flag.Uint("instrument", 0, "instrument ID (live: omit for member/quality summary)")
	user := flag.String("user", "default", "ClickHouse user; password via CLICKHOUSE_PASSWORD")
	flag.Parse()
	if *profile != "offline" && *profile != "live" {
		return fmt.Errorf("profile must be offline or live")
	}
	r, err := uuid.Parse(*runID)
	if (*profile == "offline" || *runID != "") && (err != nil || r == uuid.Nil) {
		return fmt.Errorf("valid --run required")
	}
	t, err := time.Parse(time.RFC3339Nano, *at)
	if ((*profile == "offline" || *at != "") && (err != nil || !t.Equal(t.Truncate(time.Second)))) || (*profile == "offline" && *id == 0) || uint64(*id) > uint64(^uint32(0)) || *db == "" {
		return fmt.Errorf("--database, --instrument and exact --at second required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := clickhouse.OpenReadOnly(ctx, clickhouse.Config{Addresses: []string{*addr}, Database: *db, Username: *user, Password: os.Getenv("CLICKHOUSE_PASSWORD")})
	if err != nil {
		return err
	}
	defer c.Close()
	if *profile == "live" {
		return printLive(ctx, c, r, t, uint32(*id))
	}
	e, err := c.LoadDerivativeBookEnvelope(ctx, r, t.UTC().Truncate(time.Minute))
	if err != nil {
		return err
	}
	for _, b := range e.Batches {
		if b.InstrumentID == uint32(*id) {
			result, err := replay.ReplayDerivative(b, uint8(t.UTC().Second()))
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(struct {
				Profile, Origin, EvidenceHash, BatchID string
				Result                                 replay.DerivativeResult
			}{"offline_book_foundation_v1", e.Origin, e.EvidenceHash, e.ID(), result})
		}
	}
	return fmt.Errorf("instrument not in committed offline batch")
}

func printLive(ctx context.Context, c *clickhouse.Client, id uuid.UUID, at time.Time, instrument uint32) error {
	if id == uuid.Nil {
		has, err := c.HasOptionsCatalog(ctx)
		if err != nil {
			return err
		}
		if has {
			if at.IsZero() {
				at = time.Now().UTC().Add(-time.Minute).Truncate(time.Minute).Add(59 * time.Second)
			}
			snapshot, err := c.LoadOptionsPlanSnapshot(ctx, at)
			if err != nil {
				return err
			}
			if instrument != 0 {
				for _, e := range snapshot.Runs {
					for _, b := range e.Books {
						if b.InstrumentID == instrument {
							result, err := replay.ReplayDerivative(b, uint8(at.Second()))
							if err != nil {
								return err
							}
							return json.NewEncoder(os.Stdout).Encode(result)
						}
					}
				}
				return fmt.Errorf("instrument missing from plan or uncommitted shard")
			}
			return json.NewEncoder(os.Stdout).Encode(snapshot)
		}
	}
	var r options.LiveRun
	var err error
	if id == uuid.Nil {
		r, err = c.LatestOptionsRun(ctx)
	} else {
		r, err = c.LoadOptionsRun(ctx, id)
	}
	if err != nil {
		return err
	}
	if at.IsZero() {
		at, err = c.LatestOptionsMinute(ctx, r.ID)
		if err != nil {
			return err
		}
		at = at.Add(59 * time.Second)
	}
	e, err := c.LoadOptionsMinute(ctx, r.ID, at.UTC().Truncate(time.Minute))
	if err != nil {
		return err
	}
	type coverage struct {
		InstrumentID uint32
		Symbol       string
		ValidSeconds int
		Reasons      map[uint8]int
	}
	type index struct {
		IndexID string
		Sample  options.IndexSample
	}
	result := struct {
		Profile     string
		Run         options.LiveRun
		SampleTime  time.Time
		BatchID     string
		Coverage    []coverage
		Indexes     []index
		Book        *replay.DerivativeResult
		Specs       []options.ContractSpec
		TradingRule *options.TradingRule
	}{Profile: "deribit_live_v1", Run: r, SampleTime: at.UTC(), BatchID: e.ID()}
	found := instrument == 0
	for n, b := range e.Books {
		item := coverage{InstrumentID: b.InstrumentID, Symbol: r.Members[n].Symbol, Reasons: map[uint8]int{}}
		if b.Minute != nil {
			item.ValidSeconds = bits.OnesCount64(b.Minute.ValidBitmap)
		}
		for _, q := range b.Quality {
			if !q.ReplayValid {
				item.Reasons[uint8(q.Reason)]++
			}
		}
		result.Coverage = append(result.Coverage, item)
		if b.InstrumentID == instrument {
			book, err := replay.ReplayDerivative(b, uint8(at.UTC().Second()))
			if err != nil {
				return err
			}
			result.Book = &book
			if book.Quality.TradingRuleID != "" {
				rule, err := c.LoadDerivativeTradingRule(ctx, book.Quality.TradingRuleID)
				if err != nil {
					return err
				}
				result.TradingRule = &rule
			}
			found = true
			result.Specs, err = c.LoadDerivativeSpecs(ctx, []uint32{instrument})
			if err != nil {
				return err
			}
		}
	}
	if !found {
		return fmt.Errorf("instrument not in live run")
	}
	for _, m := range e.Indexes {
		result.Indexes = append(result.Indexes, index{m.IndexID, m.Samples[at.UTC().Second()]})
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}
