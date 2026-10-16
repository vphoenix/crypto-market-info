// cutover-live checks newly committed production data through read-only APIs.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 3 {
		return fmt.Errorf("usage: cutover-live UTC_SINCE OUTPUT_JSON")
	}
	since, err := time.Parse(time.RFC3339, os.Args[1])
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cfg := mysql.Config{Socket: "/home/ubuntu/crypto-market-info/var/storage/mysql/run/mysql.sock", Username: "cmi_query", PasswordFile: "/home/ubuntu/.config/crypto-market-info-storage/cmi_query-password", Database: "crypto_market_info", MaxConnections: 1, ReadOnly: true, ReadTimeout: 2 * time.Minute, SelectTimeout: time.Minute}
	db, err := mysql.Connect(ctx, cfg)
	if err != nil {
		return err
	}
	type bookKey struct {
		ID       uint32
		Minute   time.Time
		Exchange string
		Market   model.MarketType
		Symbol   string
	}
	rows, err := db.QueryContext(ctx, "SELECT m.instrument_id,MAX(m.minute_time),i.exchange,i.market_type,i.exchange_symbol FROM order_book_minute m JOIN instrument i ON i.instrument_id=m.instrument_id WHERE m.minute_time>=? GROUP BY m.instrument_id,i.exchange,i.market_type,i.exchange_symbol ORDER BY i.exchange,i.market_type,i.exchange_symbol", since)
	if err != nil {
		db.Close()
		return err
	}
	var keys []bookKey
	for rows.Next() {
		var k bookKey
		if err = rows.Scan(&k.ID, &k.Minute, &k.Exchange, &k.Market, &k.Symbol); err != nil {
			rows.Close()
			db.Close()
			return err
		}
		keys = append(keys, k)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		db.Close()
		return err
	}
	type optionKey struct {
		Run    []byte
		Minute time.Time
	}
	rows, err = db.QueryContext(ctx, "SELECT run_id,MAX(minute_time) FROM (SELECT run_id,minute_time FROM options_live_minute_commit WHERE minute_time>=? UNION ALL SELECT run_id,minute_time FROM options_catalog_live_minute_commit WHERE minute_time>=?) recent GROUP BY run_id", since, since)
	if err != nil {
		db.Close()
		return err
	}
	var optionKeys []optionKey
	for rows.Next() {
		var k optionKey
		if err = rows.Scan(&k.Run, &k.Minute); err != nil {
			rows.Close()
			db.Close()
			return err
		}
		optionKeys = append(optionKeys, k)
	}
	err = rows.Err()
	rows.Close()
	db.Close()
	if err != nil {
		return err
	}
	c, err := mysql.Open(ctx, cfg)
	if err != nil {
		return err
	}
	defer c.Close()
	type bookCheck struct {
		Instrument   uint32    `json:"instrument"`
		Exchange     string    `json:"exchange"`
		Symbol       string    `json:"symbol"`
		Minute       time.Time `json:"minute"`
		Depth        uint8     `json:"depth"`
		ValidSeconds int       `json:"valid_seconds"`
	}
	checks := []bookCheck{}
	for _, k := range keys {
		m, e := c.LoadMinute(ctx, k.ID, k.Minute)
		if e != nil {
			return fmt.Errorf("CEX %d: %w", k.ID, e)
		}
		ds, e := c.LoadDeltas(ctx, m.ID, 59)
		if e != nil {
			return e
		}
		check := bookCheck{Instrument: k.ID, Exchange: k.Exchange, Symbol: k.Symbol, Minute: k.Minute, Depth: m.StoredDepth}
		for sec := uint8(0); sec < 60; sec++ {
			s, valid, e := replay.AtSecond(m, ds, sec)
			if e != nil {
				return e
			}
			if valid != (m.ValidBitmap&(uint64(1)<<sec) != 0) {
				return fmt.Errorf("valid bitmap mismatch")
			}
			if valid {
				s.SourceTime = k.Minute.Add(time.Duration(sec) * time.Second)
				if e = s.Validate(int(m.StoredDepth)); e != nil {
					return e
				}
				check.ValidSeconds++
			}
		}
		checks = append(checks, check)
	}
	optionBooks, optionValid := 0, 0
	for _, k := range optionKeys {
		id, e := uuid.FromBytes(k.Run)
		if e != nil {
			return e
		}
		env, e := c.LoadOptionsMinute(ctx, id, k.Minute)
		if e != nil {
			return e
		}
		for _, b := range env.Books {
			optionBooks++
			for sec := uint8(0); sec < 60; sec++ {
				r, e := replay.ReplayDerivative(b, sec)
				if e != nil {
					return e
				}
				if r.Quality.ReplayValid {
					if e = r.Snapshot.Validate(int(r.StoredDepth), b.Signed); e != nil {
						return e
					}
					optionValid++
				}
			}
		}
	}
	out := map[string]any{"checked_at_utc": time.Now().UTC(), "since_utc": since, "cex": checks, "option_runs": len(optionKeys), "option_books": optionBooks, "option_valid_seconds": optionValid, "public_api_requests": 0, "production_writes": 0, "history_complete": false}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(os.Args[2], append(b, '\n'), 0600)
}
