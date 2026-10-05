package justlendkeeper

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// Export validates database members and serializes typed observations. It performs no reward,
// return, episode, opportunity or profitability analysis.
func Export(ctx context.Context, r ReportReader, _ Archive, cfg Config, from, to time.Time, out string) error {
	if !from.Before(to) {
		return errors.New("invalid_export_window")
	}
	if _, err := os.Stat(out); !errors.Is(err, os.ErrNotExist) {
		if err != nil {
			return err
		}
		return errors.New("export_output_exists_use_new_directory")
	}
	if err := os.MkdirAll(filepath.Dir(out), 0700); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(filepath.Dir(out), ".keeper-export-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	types := []any{Capture{}, IndexedEvent{}, RentalEvent{}, TxReceipt{}, Probe{}, CostObservation{}, IndexPage{}}
	names := []string{"captures", "indexed_events", "verified_events", "receipts", "probes", "cost_observations", "index_pages"}
	files := make([]*os.File, len(types))
	writers := make([]*csv.Writer, len(types))
	counts := make([]uint64, len(types))
	defer func() {
		for _, f := range files {
			if f != nil {
				f.Close()
			}
		}
	}()
	for i, typ := range types {
		files[i], err = os.OpenFile(filepath.Join(tmp, names[i]+".csv"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		writers[i] = csv.NewWriter(files[i])
		t := reflect.TypeOf(typ)
		header := make([]string, t.NumField())
		for j := range header {
			header[j] = t.Field(j).Tag.Get("ch")
		}
		if err = writers[i].Write(header); err != nil {
			return err
		}
	}
	caps, err := r.KeeperCaptures(ctx, UTC(from), UTC(to))
	if err != nil {
		return err
	}
	accepted := make([]Capture, 0, len(caps))
	for _, cap := range caps {
		v := reflect.ValueOf(cap)
		t := v.Type()
		cells := make([]string, v.NumField())
		for k := range cells {
			value := exportValue(v.Field(k), t.Field(k).Tag.Get("ch"))
			if s, ok := value.(string); ok {
				cells[k] = s
			} else {
				raw, err := json.Marshal(value)
				if err != nil {
					return err
				}
				cells[k] = string(raw)
			}
		}
		if err = writers[0].Write(cells); err != nil {
			return err
		}
		counts[0]++
		if cap.Committed && cap.ConfigHash == cfg.Hash() {
			accepted = append(accepted, cap)
		}
	}
	caps = accepted
	parents := map[uuid.UUID]Capture{}
	for _, cap := range caps {
		parents[cap.CaptureId] = cap
	}
	for start := 0; start < len(caps); start += 256 {
		group := caps[start:min(start+256, len(caps))]
		batches := map[uuid.UUID]Batch{}
		if bulk, ok := r.(BulkReportReader); ok {
			batches, err = bulk.KeeperBatches(ctx, group)
		} else {
			for _, cap := range group {
				batches[cap.CaptureId], err = r.KeeperBatch(ctx, cap)
				if err != nil {
					break
				}
			}
		}
		if err != nil {
			return err
		}
		var missingParents []uuid.UUID
		seenParents := map[uuid.UUID]bool{}
		for _, cap := range group {
			if cap.ParentCaptureId != nil {
				id := *cap.ParentCaptureId
				if _, ok := parents[id]; !ok && !seenParents[id] {
					missingParents = append(missingParents, id)
					seenParents[id] = true
				}
			}
		}
		if len(missingParents) > 0 {
			pr, ok := r.(interface {
				KeeperParentCaptures(context.Context, []uuid.UUID) ([]Capture, error)
			})
			if !ok {
				return errors.New("export_parent_reader_required")
			}
			loaded, err := pr.KeeperParentCaptures(ctx, missingParents)
			if err != nil {
				return err
			}
			for _, parent := range loaded {
				parents[parent.CaptureId] = parent
			}
		}
		for _, cap := range group {
			if err = ctx.Err(); err != nil {
				return err
			}
			if cap.ConfigHash != cfg.Hash() {
				return errors.New("export_config_mismatch")
			}
			b, ok := batches[cap.CaptureId]
			if !ok || !captureEqual(cap, b.Capture) {
				return errors.New("export_capture_missing_or_changed")
			}
			if err = Validate(b); err != nil {
				return err
			}
			if cap.CaptureKind == "event_index" && cap.Status == "complete" && b.IndexPage == nil {
				return errors.New("index_page_progress_missing_run_migrate_pages")
			}
			if cap.ParentCaptureId != nil {
				parent, ok := parents[*cap.ParentCaptureId]
				if !ok {
					return errors.New("export_parent_missing")
				}
				if err := ValidateEnrichmentParent(cap, parent); err != nil {
					return err
				}
			}
			var pages []IndexPage
			if b.IndexPage != nil {
				pages = []IndexPage{*b.IndexPage}
			}
			rows := []any{[]Capture{}, b.IndexedEvents, b.Events, b.Receipts, b.Probes, b.Costs, pages}
			for i, list := range rows {
				v := reflect.ValueOf(list)
				for j := 0; j < v.Len(); j++ {
					row := v.Index(j)
					t := row.Type()
					cells := make([]string, row.NumField())
					for k := range cells {
						value := exportValue(row.Field(k), t.Field(k).Tag.Get("ch"))
						if s, ok := value.(string); ok {
							cells[k] = s
						} else {
							raw, err := json.Marshal(value)
							if err != nil {
								return err
							}
							cells[k] = string(raw)
						}
					}
					if err = writers[i].Write(cells); err != nil {
						return err
					}
					counts[i]++
				}
			}
		}
	}
	for i, w := range writers {
		w.Flush()
		if err = w.Error(); err != nil {
			return err
		}
		if err = files[i].Sync(); err != nil {
			return err
		}
		if err = files[i].Close(); err != nil {
			return err
		}
		files[i] = nil
	}
	meta := map[string]any{"format": "jl-keeper-data-export-v2", "from": UTC(from), "to": UTC(to), "time_filter": "capture_started_at, [from,to)", "created_at": time.Now().UTC(), "validation": "database row counts and stable member digests; original API/RPC bodies are not retained or reverified", "raw_responses_retained": false, "null": "unknown; never zero", "indexed_events": "provider observations; block hash and receipt position are unverified", "verified_events": "receipt-verified solid block facts; rental events cover the selected sample", "rows": map[string]uint64{}}
	meta["capture_audit"] = "all capture rows in window, including retracted or foreign-config captures; their members are excluded"
	meta["member_filter"] = "committed=true AND config_hash=" + Hex(cfg.Hash())
	meta["effective_available_at"] = "max(row.available_at,capture.available_at); block_time is chain time, not local availability"
	meta["enrichment_scope"] = "all returned liquidations; rental events only the selected cohort, including complete children with zero selected events"
	for i, name := range names {
		meta["rows"].(map[string]uint64)[name] = counts[i]
	}
	if err = Atomic(filepath.Join(tmp, "metadata.json"), JSONEvidence(meta)); err != nil {
		return err
	}
	return os.Rename(tmp, out)
}

func exportValue(v reflect.Value, column string) any {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return "unknown"
		}
		return exportValue(v.Elem(), column)
	}
	if v.CanInterface() {
		switch value := v.Interface().(type) {
		case time.Time:
			return value.UTC().Format(time.RFC3339Nano)
		case uuid.UUID:
			return value.String()
		case big.Int:
			return value.String()
		case decimal.Decimal:
			return value.String()
		}
	}
	switch v.Kind() {
	case reflect.String:
		value := v.String()
		if strings.HasSuffix(column, "_hash") || strings.HasSuffix(column, "_digest") || strings.HasSuffix(column, "_address") || strings.HasSuffix(column, "_selector") || column == "tx_id" || column == "renter" || column == "receiver" || column == "liquidator" || column == "sender" || column == "recipient" || column == "outer_target" || column == "candidate_origin_tx" {
			return Hex(value)
		}
		return value
	case reflect.Bool:
		return v.Bool()
	case reflect.Uint, reflect.Uint8, reflect.Uint32, reflect.Uint64:
		return fmt.Sprint(v.Uint())
	case reflect.Int, reflect.Int32, reflect.Int64:
		return fmt.Sprint(v.Int())
	case reflect.Slice:
		out := make([]any, v.Len())
		for i := range out {
			out[i] = exportValue(v.Index(i), "")
		}
		return out
	case reflect.Struct:
		out := map[string]any{}
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i).Tag.Get("ch")
			out[field] = exportValue(v.Field(i), field)
		}
		return out
	}
	panic("unsupported_keeper_export_type: " + v.Type().String())
}
