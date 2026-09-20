package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

// OpenReadOnly never bootstraps a database or schema. The server's readonly
// setting also prevents this client from accidentally executing mutations.
func OpenReadOnly(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Database == "" {
		cfg.Database = "crypto_market_info"
	}
	if !identifierPattern.MatchString(cfg.Database) {
		return nil, fmt.Errorf("invalid ClickHouse database identifier %q", cfg.Database)
	}
	if len(cfg.Addresses) == 0 {
		cfg.Addresses = []string{"127.0.0.1:9000"}
	}
	if cfg.Username == "" {
		cfg.Username = "default"
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	conn, err := ch.Open(&ch.Options{Addr: cfg.Addresses, Auth: ch.Auth{Database: cfg.Database, Username: cfg.Username, Password: cfg.Password}, DialTimeout: cfg.DialTimeout, Settings: ch.Settings{"readonly": 2, "max_execution_time": 120}})
	if err != nil {
		return nil, err
	}
	if err = conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &Client{conn: conn, database: cfg.Database, writeTimeout: 10 * time.Second, maxAttempts: 1}, nil
}

type PerpetualSourceCheck struct {
	InstrumentID         uint32     `json:"instrument_id"`
	Exchange             string     `json:"exchange"`
	Symbol               string     `json:"symbol"`
	CanonicalMarketKey   string     `json:"canonical_market_key"`
	MinuteRows           uint64     `json:"minute_rows"`
	ValidSeconds         uint64     `json:"valid_seconds"`
	RecentValidSeconds   uint64     `json:"recent_valid_seconds"`
	InvalidMinuteRows    uint64     `json:"invalid_minute_rows"`
	FirstMinute          *time.Time `json:"first_minute"`
	LatestMinute         *time.Time `json:"latest_minute"`
	ChangedPriceLevels   uint64     `json:"changed_price_levels"`
	FundingRows          uint64     `json:"funding_rows"`
	EstimatedFundingRows uint64     `json:"estimated_funding_rows"`
	ActualFundingRows    uint64     `json:"actual_funding_rows"`
	LatestFundingHour    *time.Time `json:"latest_funding_hour"`
}

type PerpetualPartCheck struct {
	Table               string `json:"table"`
	PhysicalRows        uint64 `json:"physical_rows"`
	DataCompressedBytes uint64 `json:"data_compressed_bytes"`
	BytesOnDisk         uint64 `json:"bytes_on_disk"`
}

type PerpetualDataCheck struct {
	Run                     model.PerpetualUniverseRun `json:"run"`
	RunSupersededAt         *time.Time                 `json:"run_superseded_at"`
	BookFrom                time.Time                  `json:"book_from"`
	BookThroughExclusive    time.Time                  `json:"book_through_exclusive"`
	FundingFrom             time.Time                  `json:"funding_from"`
	FundingThroughExclusive time.Time                  `json:"funding_through_exclusive"`
	RecentFrom              time.Time                  `json:"recent_from"`
	Sources                 []PerpetualSourceCheck     `json:"sources"`
	DatabaseParts           []PerpetualPartCheck       `json:"database_parts"`
}

// CheckPerpetualData aggregates final logical facts by committed run members.
// The fact tables do not carry run_id; their attribution is member/time scoped.
func (c *Client) CheckPerpetualData(ctx context.Context, runID uuid.UUID, now time.Time, recent time.Duration) (PerpetualDataCheck, error) {
	var out PerpetualDataCheck
	if now.IsZero() || recent <= 0 {
		return out, fmt.Errorf("check requires UTC time and positive recent window")
	}
	run, members, err := c.PerpetualUniverse(ctx, runID)
	if err != nil {
		return out, err
	}
	out.Run = run
	if now.Before(run.StartedAt) {
		return out, fmt.Errorf("check time precedes run start")
	}
	// A new committed run marks a new process attempt. Never credit its facts to
	// an older run; same-millisecond starts are conservatively treated as overlap.
	var superseded time.Time
	err = c.conn.QueryRow(ctx, `SELECT started_at FROM `+c.table("perpetual_universe_run")+` FINAL WHERE run_id!=? AND started_at>=? AND started_at<=? ORDER BY started_at,run_id LIMIT 1`, runID, run.StartedAt, now.UTC()).Scan(&superseded)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	if err == nil {
		out.RunSupersededAt = &superseded
	}
	out.BookFrom = run.StartedAt.UTC().Truncate(time.Minute)
	if out.BookFrom.Before(run.StartedAt) {
		out.BookFrom = out.BookFrom.Add(time.Minute)
	}
	out.BookThroughExclusive = now.UTC().Truncate(time.Minute)
	out.FundingFrom = run.StartedAt.UTC().Truncate(time.Hour)
	out.FundingThroughExclusive = now.UTC().Truncate(time.Hour).Add(time.Hour)
	if out.RunSupersededAt != nil {
		out.BookThroughExclusive = superseded.UTC().Truncate(time.Minute)
		// The final partial hour could already have been replaced by the next run.
		out.FundingThroughExclusive = superseded.UTC().Truncate(time.Hour)
	}
	out.RecentFrom = now.UTC().Add(-recent).Truncate(time.Minute)
	if out.RecentFrom.Before(out.BookFrom) {
		out.RecentFrom = out.BookFrom
	}
	items, err := c.Instruments(ctx)
	if err != nil {
		return out, err
	}
	byID := make(map[uint32]model.Instrument, len(items))
	for _, i := range items {
		byID[i.ID] = i
	}
	indexes := make(map[uint32]int, len(members))
	out.Sources = make([]PerpetualSourceCheck, 0, len(members))
	for _, member := range members {
		i, ok := byID[member.InstrumentID]
		if !ok {
			return out, fmt.Errorf("universe member %d missing instrument", member.InstrumentID)
		}
		indexes[i.ID] = len(out.Sources)
		out.Sources = append(out.Sources, PerpetualSourceCheck{InstrumentID: i.ID, Exchange: i.Exchange, Symbol: i.ExchangeSymbol, CanonicalMarketKey: member.CanonicalMarketKey})
	}
	memberSQL := `SELECT instrument_id FROM ` + c.table("perpetual_universe_member") + ` FINAL WHERE run_id=?`
	rows, err := c.conn.Query(ctx, `SELECT instrument_id,count(),sum(bitCount(valid_bitmap)),sumIf(bitCount(valid_bitmap),minute_time>=?),countIf(bitAnd(valid_bitmap,1)=0 OR bitShiftRight(valid_bitmap,60)!=0),min(minute_time),max(minute_time)
FROM `+c.table("order_book_minute")+` FINAL WHERE instrument_id IN (`+memberSQL+`) AND minute_time>=? AND minute_time<? GROUP BY instrument_id`, out.RecentFrom, runID, out.BookFrom, out.BookThroughExclusive)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var id uint32
		var count, valid, recentValid, invalidMinutes uint64
		var first, last time.Time
		if err = rows.Scan(&id, &count, &valid, &recentValid, &invalidMinutes, &first, &last); err != nil {
			_ = rows.Close()
			return out, err
		}
		s := &out.Sources[indexes[id]]
		s.MinuteRows, s.ValidSeconds, s.RecentValidSeconds = count, valid, recentValid
		s.InvalidMinuteRows = invalidMinutes
		s.FirstMinute = &first
		s.LatestMinute = &last
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return out, err
	}
	rows, err = c.conn.Query(ctx, `SELECT toUInt32(bitAnd(minute_id,4294967295)) AS source_id,sum(length(bid_change_prices)+length(ask_change_prices))
FROM `+c.table("order_book_second_delta")+` FINAL WHERE bitShiftRight(minute_id,32)>=? AND bitShiftRight(minute_id,32)<?
AND minute_id IN (SELECT id FROM `+c.table("order_book_minute")+` FINAL WHERE instrument_id IN (`+memberSQL+`) AND minute_time>=? AND minute_time<?)
GROUP BY source_id`, uint64(out.BookFrom.Unix()/60), uint64(out.BookThroughExclusive.Unix()/60), runID, out.BookFrom, out.BookThroughExclusive)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var id uint32
		var changes uint64
		if err = rows.Scan(&id, &changes); err != nil {
			_ = rows.Close()
			return out, err
		}
		out.Sources[indexes[id]].ChangedPriceLevels = changes
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return out, err
	}
	rows, err = c.conn.Query(ctx, `SELECT instrument_id,count(),countIf(is_actual),max(hour_time) FROM `+c.table("funding_rate_hourly")+` FINAL WHERE instrument_id IN (`+memberSQL+`) AND hour_time>=? AND hour_time<? GROUP BY instrument_id`, runID, out.FundingFrom, out.FundingThroughExclusive)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var id uint32
		var count, actual uint64
		var latest time.Time
		if err = rows.Scan(&id, &count, &actual, &latest); err != nil {
			_ = rows.Close()
			return out, err
		}
		s := &out.Sources[indexes[id]]
		s.FundingRows, s.ActualFundingRows, s.LatestFundingHour = count, actual, &latest
		s.EstimatedFundingRows = count - actual
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return out, err
	}
	rows, err = c.conn.Query(ctx, `SELECT table,sum(rows),sum(data_compressed_bytes),sum(bytes_on_disk) FROM system.parts WHERE active AND database=? GROUP BY table ORDER BY table`, c.database)
	if err != nil {
		return out, err
	}
	out.DatabaseParts = []PerpetualPartCheck{}
	for rows.Next() {
		var p PerpetualPartCheck
		if err = rows.Scan(&p.Table, &p.PhysicalRows, &p.DataCompressedBytes, &p.BytesOnDisk); err != nil {
			_ = rows.Close()
			return out, err
		}
		out.DatabaseParts = append(out.DatabaseParts, p)
	}
	err = rows.Err()
	_ = rows.Close()
	return out, err
}

// PerpetualReplaySeconds uses a fixed-seed reservoir to sample valid seconds
// uniformly without materializing all seconds of a long-running dataset.
func (c *Client) PerpetualReplaySeconds(ctx context.Context, instrumentID uint32, from, through time.Time, count int, seed int64) ([]time.Time, error) {
	if instrumentID == 0 || count <= 0 || count > 1000 || from.IsZero() || through.Before(from) || !from.Equal(from.Truncate(time.Minute)) || !through.Equal(through.Truncate(time.Minute)) {
		return nil, fmt.Errorf("invalid replay sample window/count")
	}
	rows, err := c.conn.Query(ctx, `SELECT minute_time,valid_bitmap FROM `+c.table("order_book_minute")+` FINAL WHERE instrument_id=? AND minute_time>=? AND minute_time<? ORDER BY minute_time`, instrumentID, from.UTC(), through.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	rng := rand.New(rand.NewSource(seed))
	samples := make([]time.Time, 0, count)
	seen := int64(0)
	for rows.Next() {
		var minute time.Time
		var bitmap uint64
		if err = rows.Scan(&minute, &bitmap); err != nil {
			return nil, err
		}
		if bitmap>>60 != 0 || bitmap&1 == 0 {
			return nil, fmt.Errorf("invalid bitmap for instrument %d minute %s", instrumentID, minute)
		}
		for second := 0; second < 60; second++ {
			if bitmap&(uint64(1)<<second) == 0 {
				continue
			}
			at := minute.UTC().Add(time.Duration(second) * time.Second)
			seen++
			if len(samples) < count {
				samples = append(samples, at)
			} else if index := rng.Int63n(seen); index < int64(count) {
				samples[index] = at
			}
		}
	}
	return samples, rows.Err()
}
