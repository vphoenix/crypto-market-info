// Capacity fixture: repeated real typed rows with synthetic identities/times.
// This program creates and removes only its unique temporary databases.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/vphoenix/crypto-market-info/internal/across"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	store "github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
)

type part struct {
	Table                                               string `json:"table"`
	Rows, CompressedBytes, UncompressedBytes, DiskBytes uint64
}
type query struct {
	Name, SQL string
	Micros    []int64
	Result    string
}
type sample struct {
	ChainID                                       uint64
	Kind                                          string
	SourceCapture                                 string
	SourceFromBlock, SourceToBlock                uint64
	SourceStart, SourceEnd                        time.Time
	SourceHeaderSpanSeconds, RepeatCadenceSeconds int64
	CopiesIncludingPartial                        int
	SourceRows, SyntheticRows                     map[string]int
	SyntheticDay                                  time.Time
	InsertMillis                                  int64
	Parts                                         []part
	Queries                                       []query
	SourceEvidenceGzipBytes                       int64
	TemporaryDatabase                             string
	Cleaned                                       bool
}

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func sourceRows(b across.Batch) map[string]int {
	return map[string]int{"deposit": len(b.Deposits), "update": len(b.Updates), "fill": len(b.Fills), "refund": len(b.Refunds), "receipt": len(b.Receipts), "probe": len(b.Probes)}
}
func appendBatch(dst *across.Batch, b across.Batch) {
	dst.Deposits = append(dst.Deposits, b.Deposits...)
	dst.Updates = append(dst.Updates, b.Updates...)
	dst.Fills = append(dst.Fills, b.Fills...)
	dst.Refunds = append(dst.Refunds, b.Refunds...)
	dst.Receipts = append(dst.Receipts, b.Receipts...)
	dst.Probes = append(dst.Probes, b.Probes...)
}
func clone[T any](src T, chain uint64, copyNo int, shift time.Duration, blockShift uint64, capture string) T {
	dst := src
	v := reflect.ValueOf(&dst).Elem()
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		name := v.Type().Field(i).Name
		if f.Type() == reflect.TypeOf(time.Time{}) {
			f.Set(reflect.ValueOf(f.Interface().(time.Time).Add(shift)))
			continue
		}
		if f.Type() == reflect.TypeOf((*time.Time)(nil)) && !f.IsNil() {
			f.Set(reflect.ValueOf(across.Ptr(f.Interface().(*time.Time).Add(shift))))
			continue
		}
		switch name {
		case "CaptureId":
			f.SetString(capture)
		case "BlockNumber":
			if f.Kind() == reflect.Uint64 {
				f.SetUint(f.Uint() + blockShift)
			} else if !f.IsNil() {
				f.Set(reflect.ValueOf(across.Ptr(f.Elem().Uint() + blockShift)))
			}
		case "BlockHash", "TxHash", "OriginBlockHash", "ProbeId":
			if f.Kind() == reflect.String {
				f.SetString(across.ID(struct {
					Chain          uint64
					Copy           int
					Kind, Original string
				}{chain, copyNo, name, f.String()}))
			} else if !f.IsNil() {
				f.Set(reflect.ValueOf(across.Ptr(across.ID(struct {
					Chain          uint64
					Copy           int
					Kind, Original string
				}{chain, copyNo, name, f.Elem().String()}))))
			}
		case "DepositId":
			old := f.Interface().(*big.Int)
			h := across.ID(struct {
				Chain    uint64
				Copy     int
				Original string
			}{chain, copyNo, old.String()})
			f.Set(reflect.ValueOf(new(big.Int).SetBytes([]byte(h))))
		case "QuoteTimestamp", "FillDeadline", "ExclusivityDeadline":
			if f.Uint() != 0 {
				n := int64(f.Uint()) + int64(shift/time.Second)
				if n < 0 || n > 1<<32-1 {
					panic("synthetic deadline overflow")
				}
				f.SetUint(uint64(n))
			}
		}
	}
	return dst
}
func copyRows[T any](src []T, chain uint64, copyNo int, shift time.Duration, blockShift uint64, capture string, end time.Time) []T {
	out := []T{}
	for _, v := range src {
		row := clone(v, chain, copyNo, shift, blockShift, capture)
		tm := reflect.ValueOf(row).FieldByName("BlockTime")
		var at time.Time
		if tm.Kind() == reflect.Pointer {
			if tm.IsNil() {
				continue
			}
			at = tm.Elem().Interface().(time.Time)
		} else {
			at = tm.Interface().(time.Time)
		}
		if at.Before(end) {
			out = append(out, row)
		}
	}
	return out
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	m, e := across.LoadManifest("config/across-research.json")
	if e != nil {
		return e
	}
	src, e := store.OpenDEXReader(ctx, store.Config{Addresses: []string{"127.0.0.1:9000"}, Database: "crypto_market_info_across"})
	if e != nil {
		return e
	}
	defer src.Close()
	admin, e := ch.Open(&ch.Options{Addr: []string{"127.0.0.1:9000"}, Auth: ch.Auth{Database: "default", Username: "default"}})
	if e != nil {
		return e
	}
	defer admin.Close()
	caps, e := src.AcrossCaptures(ctx, m.Hash)
	if e != nil {
		return e
	}
	archive := ethereum.Archive{Dir: "var/across/evidence"}
	out := []sample{}
	for _, chain := range []uint64{8453, 42161} {
		var seed *across.Capture
		for _, c := range caps {
			if c.ChainId == chain && c.CaptureKind == "logs" && c.Committed && c.Canonical && c.Status == "complete" && c.FromBlock != nil && *c.ToBlock-*c.FromBlock == 511 {
				v := c
				seed = &v
				break
			}
		}
		if seed == nil {
			return fmt.Errorf("missing seed for %d", chain)
		}
		base, e := src.AcrossBatch(ctx, *seed)
		if e != nil {
			return e
		}
		proof, e := across.ReadCaptureEvidence(archive, *seed)
		if e != nil {
			return e
		}
		if proof.FromTime == nil || proof.ToTime == nil {
			return fmt.Errorf("seed header time missing")
		}
		start, end := *proof.FromTime, *proof.ToTime
		span := int64(end.Sub(start) / time.Second)
		cadence := span + 1
		if cadence <= 0 {
			return fmt.Errorf("invalid source span")
		}
		evidenceHashes := map[string]bool{across.Hex(seed.EvidenceHash): true}
		for _, h := range proof.RPCPayloads {
			evidenceHashes[h] = true
		}
		// Include only canonical, committed receipts for this actual seed range and
		// deduplicate cross-capture physical attempts before synthetic replication.
		seenReceipts := map[string]bool{}
		for _, c := range caps {
			if c.ChainId != chain || c.CaptureKind != "receipts" || !c.Canonical || !c.Committed || c.CompletedTasks != 1 || c.FromBlock == nil || *c.FromBlock < *seed.FromBlock || *c.ToBlock > *seed.ToBlock {
				continue
			}
			b, e := src.AcrossBatch(ctx, c)
			if e != nil {
				return e
			}
			for _, r := range b.Receipts {
				k := r.BlockHash + r.TxHash
				if !seenReceipts[k] {
					base.Receipts = append(base.Receipts, r)
					seenReceipts[k] = true
				}
			}
			ep, e := across.ReadCaptureEvidence(archive, c)
			if e != nil {
				return e
			}
			evidenceHashes[across.Hex(c.EvidenceHash)] = true
			for _, h := range ep.RPCPayloads {
				evidenceHashes[h] = true
			}
		}
		s := sample{ChainID: chain, Kind: "synthetic_24h_from_repeated_real_512_block_window", SourceCapture: across.Hex(seed.CaptureId), SourceFromBlock: *seed.FromBlock, SourceToBlock: *seed.ToBlock, SourceStart: start, SourceEnd: end, SourceHeaderSpanSeconds: span, RepeatCadenceSeconds: cadence, SourceRows: sourceRows(base), SyntheticDay: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), CopiesIncludingPartial: int((86400 + cadence - 1) / cadence)}
		for h := range evidenceHashes {
			hex := strings.TrimPrefix(h, "0x")
			st, e := os.Stat(filepath.Join(archive.Dir, hex[:2], hex+".json.gz"))
			if e != nil {
				return e
			}
			s.SourceEvidenceGzipBytes += st.Size()
		}
		dayEnd := s.SyntheticDay.Add(24 * time.Hour)
		batch := across.Batch{}
		captureID := across.ID(struct {
			Kind  string
			Chain uint64
		}{"synthetic_capacity_24h", chain})
		for rep := 0; rep < s.CopiesIncludingPartial; rep++ {
			shift := s.SyntheticDay.Add(time.Duration(int64(rep)*cadence) * time.Second).Sub(start)
			blockShift := uint64(1_000_000_000) - *seed.FromBlock + uint64(rep)*512
			b := across.Batch{Deposits: copyRows(base.Deposits, chain, rep, shift, blockShift, captureID, dayEnd), Updates: copyRows(base.Updates, chain, rep, shift, blockShift, captureID, dayEnd), Fills: copyRows(base.Fills, chain, rep, shift, blockShift, captureID, dayEnd), Refunds: copyRows(base.Refunds, chain, rep, shift, blockShift, captureID, dayEnd), Receipts: copyRows(base.Receipts, chain, rep, shift, blockShift, captureID, dayEnd), Probes: copyRows(base.Probes, chain, rep, shift, blockShift, captureID, dayEnd)}
			for i := range b.Deposits {
				h, e := across.RelayHash(b.Deposits[i])
				if e != nil {
					return e
				}
				b.Deposits[i].RelayHash = h
			}
			appendBatch(&batch, b)
		}
		s.SyntheticRows = sourceRows(batch)
		var minBlock, maxBlock uint64
		var minHash, maxHash string
		available := dayEnd
		for _, rows := range []any{batch.Deposits, batch.Updates, batch.Fills, batch.Refunds, batch.Receipts, batch.Probes} {
			v := reflect.ValueOf(rows)
			for i := 0; i < v.Len(); i++ {
				row := v.Index(i)
				n := row.FieldByName("BlockNumber").Uint()
				h := row.FieldByName("BlockHash").String()
				if minBlock == 0 || n < minBlock {
					minBlock, minHash = n, h
				}
				if n > maxBlock {
					maxBlock, maxHash = n, h
				}
				at := row.FieldByName("AvailableAt").Interface().(time.Time)
				if at.After(available) {
					available = at
				}
			}
		}
		batch.Capture = across.Capture{ManifestHash: m.Hash, CaptureId: captureID, ChainId: chain, CaptureKind: "synthetic_capacity", CaptureMode: "synthetic", FromBlock: &minBlock, ToBlock: &maxBlock, FromHash: &minHash, ToHash: &maxHash, StartedAt: s.SyntheticDay, AvailableAt: available, SourceId: "synthetic_repeat_of_public_typed_facts", Status: "complete", Reason: "SYNTHETIC capacity fixture; no onchain or profitability meaning", ExpectedTasks: 1, CompletedTasks: 1, EvidenceHash: across.ID(struct {
			Chain  uint64
			Source string
		}{chain, s.SourceCapture}), Canonical: true, Finality: "synthetic", Revision: 1}
		across.Seal(&batch)
		if e = across.Validate(batch); e != nil {
			return e
		}
		s.TemporaryDatabase = fmt.Sprintf("crypto_market_info_across_capacity_%d_%d", chain, time.Now().UnixNano())
		func() {
			db := s.TemporaryDatabase
			defer func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cleanupCancel()
				if err := admin.Exec(cleanupCtx, "DROP DATABASE IF EXISTS `"+db+"` SYNC"); err != nil && e == nil {
					e = err
				} else if err == nil {
					s.Cleaned = true
				}
			}()
			var dst *store.Client
			dst, e = store.Open(ctx, store.Config{Addresses: []string{"127.0.0.1:9000"}, Database: db, WriteTimeout: time.Minute})
			if e != nil {
				return
			}
			defer dst.Close()
			if e = dst.InitAcrossSchema(ctx); e != nil {
				return
			}
			then := time.Now()
			if e = dst.WriteAcrossBatch(ctx, batch); e != nil {
				return
			}
			s.InsertMillis = time.Since(then).Milliseconds()
			for _, table := range []string{"across_capture", "across_deposit", "across_deposit_update", "across_fill", "across_refund", "across_tx_receipt", "across_order_probe"} {
				if e = admin.Exec(ctx, "OPTIMIZE TABLE `"+db+"`.`"+table+"` FINAL"); e != nil {
					return
				}
			}
			rows, err := admin.Query(ctx, "SELECT table,sum(rows),sum(data_compressed_bytes),sum(data_uncompressed_bytes),sum(bytes_on_disk) FROM system.parts WHERE database=? AND active GROUP BY table ORDER BY table", db)
			if err != nil {
				e = err
				return
			}
			for rows.Next() {
				var p part
				if e = rows.Scan(&p.Table, &p.Rows, &p.CompressedBytes, &p.UncompressedBytes, &p.DiskBytes); e != nil {
					rows.Close()
					return
				}
				s.Parts = append(s.Parts, p)
			}
			e = rows.Err()
			rows.Close()
			if e != nil {
				return
			}
			qbase := "`" + db + "`."
			queries := []query{{Name: "deposit_sum_by_token_and_destination", SQL: "SELECT toString(count()) FROM (SELECT destination_chain_id,input_token,count(),sum(input_amount_raw) FROM " + qbase + "across_deposit FINAL WHERE block_time>=? AND block_time<? GROUP BY destination_chain_id,input_token)"}, {Name: "hourly_fill_aggregation", SQL: "SELECT toString(count()) FROM (SELECT toStartOfHour(block_time),fill_type,count(),sum(updated_output_amount_raw) FROM " + qbase + "across_fill FINAL WHERE block_time>=? AND block_time<? GROUP BY toStartOfHour(block_time),fill_type)"}, {Name: "receipt_cost_completeness", SQL: "SELECT concat(toString(count()),':',toString(countIf(fee_complete))) FROM " + qbase + "across_tx_receipt FINAL WHERE block_time>=? AND block_time<?"}}
			for i := range queries {
				q := &queries[i]
				for run := 0; run < 7; run++ {
					start := time.Now()
					var result string
					if e = admin.QueryRow(ctx, q.SQL, s.SyntheticDay, dayEnd).Scan(&result); e != nil {
						return
					}
					q.Micros = append(q.Micros, time.Since(start).Microseconds())
					q.Result = result
				}
				q.SQL = strings.ReplaceAll(q.SQL, db, "<temporary_database>")
			}
			s.Queries = queries
		}()
		if e != nil {
			return e
		}
		out = append(out, s)
		fmt.Printf("chain=%d synthetic_rows=%v parts=%d cleaned=%v\n", chain, s.SyntheticRows, len(s.Parts), s.Cleaned)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ChainID < out[j].ChainID })
	report := map[string]any{"synthetic": true, "created_at": time.Now().UTC(), "source_database": "crypto_market_info_across", "samples": out, "limitations": []string{"These are synthetic 24-hour capacity fixtures, not observed daily traffic or earnings.", "Window cadence is endpoint timestamp difference plus one second; repetitions are clipped at the synthetic 24-hour boundary.", "Amounts remain exact UInt256/big.Int; balances/fees are not interpolated. Capture, block, transaction, deposit and relay identities are synthetic.", "Repeated token/address/message/fee values make compression optimistic despite unique random hashes. One capture per synthetic day also undercounts production capture overhead.", "No probes or deposit updates exist in source windows; capacity for these paths is not measured.", "Receipt coverage is only the receipts already present for these windows, not complete future daily receipt workload.", "Query runs include one first run and six warm runs; microseconds are client wall clock. This is not a sustained load benchmark.", "Gzip bytes are measured for deduplicated original-window capture and RPC evidence only, not synthetic daily raw evidence."}}
	raw, e := json.MarshalIndent(report, "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile("research/2026-10-02-across-implementation/capacity.json", append(raw, '\n'), 0644)
}
