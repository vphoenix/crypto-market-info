// perp-check records reproducible, read-only evidence for a perpetual run.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/config"
	"github.com/vphoenix/crypto-market-info/internal/model"
	chstore "github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
)

type replayCheck struct {
	Class             string      `json:"class"`
	Exchange          string      `json:"exchange"`
	Symbol            string      `json:"symbol"`
	InstrumentID      uint32      `json:"instrument_id"`
	Requested         int         `json:"requested"`
	Verified          int         `json:"verified"`
	SampleSeconds     []time.Time `json:"sample_seconds"`
	TotalMicroseconds int64       `json:"total_microseconds"`
	P50Microseconds   int64       `json:"p50_microseconds"`
	P99Microseconds   int64       `json:"p99_microseconds"`
	MaxMicroseconds   int64       `json:"max_microseconds"`
}
type report struct {
	Status              string                     `json:"status"`
	FullSoakVerified    bool                       `json:"full_soak_verified"`
	Database            string                     `json:"database"`
	CheckedAt           time.Time                  `json:"checked_at"`
	MinimumDuration     string                     `json:"minimum_duration"`
	RunAgeSeconds       int64                      `json:"run_age_seconds"`
	RecentWindowSeconds int64                      `json:"recent_window_seconds"`
	Data                chstore.PerpetualDataCheck `json:"data"`
	Replays             []replayCheck              `json:"replays"`
	Pending             []string                   `json:"pending"`
	Failures            []string                   `json:"failures"`
	Notes               []string                   `json:"notes"`
}

func main() { os.Exit(run()) }
func run() int {
	database := flag.String("database", "", "existing ClickHouse database (defaults to CLICKHOUSE_DATABASE)")
	runIDText := flag.String("run-id", "", "committed perpetual universe run UUID (required)")
	minimum := flag.Duration("min-duration", 0, "minimum observed duration per source; use 24h for soak evidence")
	recent := flag.Duration("recent", 10*time.Minute, "recent valid-second aggregation window")
	timeout := flag.Duration("timeout", 5*time.Minute, "overall read-only check timeout")
	flag.Parse()
	r := report{Status: "failed", CheckedAt: time.Now().UTC(), MinimumDuration: minimum.String(), Pending: []string{}, Failures: []string{}, Replays: []replayCheck{}, Notes: []string{
		"Facts do not contain run_id; book observations are scoped to committed members and complete UTC minutes after run start, ending before the next committed run. Concurrent collectors still cannot be distinguished.",
		"Funding is scoped by hour bucket, including the run's initial hour; it is not proof of when a particular estimate was inserted.",
		"system.parts values are actual active physical parts for the entire database, including older runs and metadata; they are not per-run increments.",
		"Representatives are highest and lowest changed-price-level count among sources with at least 100 valid seconds in each venue; low activity does not imply low trading volume.",
		"passed_checks covers database/replay checks only. CPU, network, request limits, continuous process uptime and sampler/writer p99 require runtime/host evidence.",
	}}
	finish := func(code int) int {
		e := json.NewEncoder(os.Stdout)
		e.SetIndent("", "  ")
		if err := e.Encode(r); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return code
	}
	fail := func(err error) int {
		r.Failures = append(r.Failures, err.Error())
		r.Status = "failed"
		return finish(1)
	}
	if *minimum < 0 || *recent <= 0 || *timeout <= 0 {
		return fail(fmt.Errorf("durations must be positive except min-duration may be zero"))
	}
	id, err := uuid.Parse(*runIDText)
	if err != nil || id == uuid.Nil {
		return fail(fmt.Errorf("-run-id must be a nonzero UUID"))
	}
	cfg, err := config.Load()
	if err != nil {
		return fail(err)
	}
	if *database != "" {
		cfg.ClickHouse.Database = *database
	}
	r.Database = cfg.ClickHouse.Database
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	client, err := chstore.OpenReadOnly(ctx, cfg.ClickHouse)
	if err != nil {
		return fail(err)
	}
	defer client.Close()
	r.Data, err = client.CheckPerpetualData(ctx, id, r.CheckedAt, *recent)
	if err != nil {
		return fail(err)
	}
	runThrough := r.CheckedAt
	if r.Data.RunSupersededAt != nil {
		runThrough = *r.Data.RunSupersededAt
	}
	r.RunAgeSeconds = int64(runThrough.Sub(r.Data.Run.StartedAt) / time.Second)
	r.RecentWindowSeconds = max(0, int64(r.Data.BookThroughExclusive.Sub(r.Data.RecentFrom)/time.Second))
	if runThrough.Sub(r.Data.Run.StartedAt) < *minimum {
		r.Pending = append(r.Pending, "run age has not reached minimum duration")
	}
	if r.Data.RunSupersededAt != nil {
		r.Pending = append(r.Pending, "run was superseded by another committed run; historical facts cannot verify the current collector")
	}
	if len(r.Data.Sources) == 0 {
		r.Pending = append(r.Pending, "committed run has no market-data sources")
	}
	var selectedConfig struct {
		Capacity struct{ FundingEnabled *bool }
	}
	if err = json.Unmarshal([]byte(r.Data.Run.SelectionConfigJSON), &selectedConfig); err != nil {
		return fail(err)
	}
	if selectedConfig.Capacity.FundingEnabled == nil {
		r.Pending = append(r.Pending, "run selection config does not declare FundingEnabled; funding requirements are unknown")
	}
	byVenue := make(map[string][]chstore.PerpetualSourceCheck)
	expectedVenues := make(map[string]bool)
	for _, s := range r.Data.Sources {
		expectedVenues[s.Exchange] = true
		if s.InvalidMinuteRows > 0 || s.ValidSeconds > s.MinuteRows*60 {
			return fail(fmt.Errorf("instrument %d has impossible valid-second count", s.InstrumentID))
		}
		fundingRequired := selectedConfig.Capacity.FundingEnabled != nil && *selectedConfig.Capacity.FundingEnabled
		r.Pending = append(r.Pending, sourcePending(s, r.Data.BookThroughExclusive, *minimum, fundingRequired)...)
		if s.ValidSeconds >= 100 {
			byVenue[s.Exchange] = append(byVenue[s.Exchange], s)
		}
	}
	seed := int64(binary.BigEndian.Uint64(id[:8]))
	for _, venue := range r.Data.Run.EnabledVenues {
		if !expectedVenues[venue] {
			continue // An enabled venue need not contribute to any common group.
		}
		candidates := byVenue[venue]
		if len(candidates) < 2 {
			r.Pending = append(r.Pending, fmt.Sprintf("%s needs two distinct sources with 100 valid seconds for active/low-activity replay", venue))
			continue
		}
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].ChangedPriceLevels == candidates[j].ChangedPriceLevels {
				return candidates[i].InstrumentID < candidates[j].InstrumentID
			}
			return candidates[i].ChangedPriceLevels < candidates[j].ChangedPriceLevels
		})
		for index, s := range []chstore.PerpetualSourceCheck{candidates[len(candidates)-1], candidates[0]} {
			class := "active"
			if index == 1 {
				class = "low_activity"
			}
			rc := replayCheck{Class: class, Exchange: venue, Symbol: s.Symbol, InstrumentID: s.InstrumentID, Requested: 100}
			rc.SampleSeconds, err = client.PerpetualReplaySeconds(ctx, s.InstrumentID, r.Data.BookFrom, r.Data.BookThroughExclusive, 100, seed^int64(s.InstrumentID))
			if err != nil {
				return fail(err)
			}
			if len(rc.SampleSeconds) < 100 {
				r.Pending = append(r.Pending, fmt.Sprintf("instrument %d has fewer than 100 valid replay seconds", s.InstrumentID))
				r.Replays = append(r.Replays, rc)
				continue
			}
			latencies := make([]int64, 0, 100)
			for _, at := range rc.SampleSeconds {
				start := time.Now()
				book, valid, replayErr := client.ReplayBook(ctx, s.InstrumentID, at)
				duration := time.Since(start).Microseconds()
				if replayErr != nil {
					return fail(fmt.Errorf("replay %d %s: %w", s.InstrumentID, at, replayErr))
				}
				if !valid {
					return fail(fmt.Errorf("valid bitmap second replayed invalid: %d %s", s.InstrumentID, at))
				}
				if err = validateBook(book, s.InstrumentID); err != nil {
					return fail(fmt.Errorf("replay %s: %w", at, err))
				}
				latencies = append(latencies, duration)
				rc.TotalMicroseconds += duration
				rc.Verified++
			}
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			rc.P50Microseconds, rc.P99Microseconds, rc.MaxMicroseconds = latencies[49], latencies[98], latencies[99]
			r.Replays = append(r.Replays, rc)
		}
	}
	if len(r.Pending) > 0 {
		r.Status = "pending"
		return finish(2)
	}
	r.Status = "passed_checks"
	return finish(0)
}

func sourcePending(s chstore.PerpetualSourceCheck, through time.Time, minimum time.Duration, fundingRequired bool) []string {
	var pending []string
	if s.MinuteRows == 0 || s.FirstMinute == nil || s.LatestMinute == nil {
		return []string{fmt.Sprintf("%s %s has no completed minute", s.Exchange, s.Symbol)}
	}
	span := s.LatestMinute.Add(time.Minute).Sub(*s.FirstMinute)
	if span < minimum {
		pending = append(pending, fmt.Sprintf("%s %s observed span %s below %s", s.Exchange, s.Symbol, span, minimum))
	}
	// Elapsed wall time alone must not pass a soak with mostly missing data.
	if minimum > 0 && s.ValidSeconds < uint64((minimum+time.Second-1)/time.Second) {
		pending = append(pending, fmt.Sprintf("%s %s has only %d captured valid seconds; minimum %s not met", s.Exchange, s.Symbol, s.ValidSeconds, minimum))
	}
	if s.LatestMinute.Before(through.Add(-3 * time.Minute)) {
		pending = append(pending, fmt.Sprintf("%s %s latest minute is stale", s.Exchange, s.Symbol))
	}
	if fundingRequired && s.FundingRows == 0 {
		pending = append(pending, fmt.Sprintf("%s %s has no funding row yet", s.Exchange, s.Symbol))
	} else if fundingRequired {
		if s.LatestFundingHour == nil || s.LatestFundingHour.Before(through.Truncate(time.Hour).Add(-time.Hour)) {
			pending = append(pending, fmt.Sprintf("%s %s latest funding hour is stale or unknown", s.Exchange, s.Symbol))
		}
		if minimum > 0 && s.FundingRows < uint64((minimum+time.Hour-1)/time.Hour) {
			pending = append(pending, fmt.Sprintf("%s %s has only %d funding hour rows; minimum %s not met", s.Exchange, s.Symbol, s.FundingRows, minimum))
		}
		if minimum >= 24*time.Hour && s.ActualFundingRows == 0 {
			pending = append(pending, fmt.Sprintf("%s %s has no confirmed actual funding row in the soak window", s.Exchange, s.Symbol))
		}
	}
	return pending
}

func validateBook(book model.BookSnapshot, instrumentID uint32) error {
	if book.StoredDepth != model.BookDepth && book.StoredDepth != model.LegacyBookDepth {
		return fmt.Errorf("replayed book has unsupported stored depth %d", book.StoredDepth)
	}
	if book.InstrumentID != instrumentID || len(book.Bids) < 1 || len(book.Bids) > int(book.StoredDepth) || len(book.Asks) < 1 || len(book.Asks) > int(book.StoredDepth) {
		return fmt.Errorf("replayed book has invalid identity/depth")
	}
	for side, levels := range [][]model.Level{book.Bids, book.Asks} {
		for i, level := range levels {
			if level.PriceTick <= 0 || level.QtyLot == 0 {
				return fmt.Errorf("replayed level has invalid price/quantity")
			}
			if i > 0 && ((side == 0 && levels[i-1].PriceTick <= level.PriceTick) || (side == 1 && levels[i-1].PriceTick >= level.PriceTick)) {
				return fmt.Errorf("replayed book is not strictly sorted")
			}
		}
	}
	if book.Bids[0].PriceTick >= book.Asks[0].PriceTick {
		return fmt.Errorf("replayed book is crossed or locked")
	}
	return nil
}
