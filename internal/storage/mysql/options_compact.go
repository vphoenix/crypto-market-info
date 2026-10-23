package mysql

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type compactField struct{ name, typ, kind string }

func optionCompactFields(table string) []compactField {
	hexField := func(name string) compactField { return compactField{name, "FixedString(64)", "hex"} }
	rle := func(name, typ string) compactField { return compactField{name, typ, "rle"} }
	var fields []compactField
	switch table {
	case "derivative_book_quality_minute", qualityMinuteV2:
		fields = []compactField{hexField("batch_id"), hexField("row_hash")}
		for _, name := range []string{"source_times", "received_times", "captured_times", "last_snapshot_times", "connection_confirmed_times", "rule_published_times", "market_state_times"} {
			fields = append(fields, compactField{name, "Array(Nullable(DateTime64(6, 'UTC')))", "time"})
		}
		fields = append(fields, rle("connection_epochs", "Array(Nullable(UUID))"), rle("change_ids", "Array(UInt64)"), compactField{"trading_rule_ids", "Array(Nullable(FixedString(64)))", "rlehex"})
		for _, name := range []string{"market_state_bases", "reasons", "bid_level_counts", "ask_level_counts"} {
			fields = append(fields, rle(name, "Array(UInt8)"))
		}
	case "options_catalog_quality_evidence_minute":
		fields = []compactField{hexField("batch_id"), hexField("row_hash")}
		for _, name := range []string{"state_ids", "rule_observation_ids", "platform_ids", "maintenance_ids", "lock_ids"} {
			fields = append(fields, rle(name, "Array(Nullable(UUID))"))
		}
		fields = append(fields, rle("state_kinds", "Array(UInt8)"), rle("lifecycle_epochs", "Array(UUID)"), compactField{"lifecycle_confirmed", "Array(Nullable(DateTime64(6, 'UTC')))", "time"})
	case "derivative_book_foundation_commit", "options_live_minute_commit", "options_catalog_live_minute_commit":
		fields = []compactField{hexField("batch_id")}
		if table != "derivative_book_foundation_commit" {
			fields = append(fields, hexField("run_hash"))
		}
		if table == "options_catalog_live_minute_commit" {
			fields = append(fields, hexField("plan_hash"))
		}
		for _, name := range []string{"member_hashes", "index_hashes", "evidence_hashes"} {
			if name == "index_hashes" && table == "derivative_book_foundation_commit" || name == "evidence_hashes" && table != "options_catalog_live_minute_commit" {
				continue
			}
			fields = append(fields, compactField{name, "Array(FixedString(64))", "hexarray"})
		}
	}
	return fields
}

func compactFieldMap(table string) map[string]compactField {
	m := map[string]compactField{}
	for _, f := range optionCompactFields(table) {
		m[f.name] = f
	}
	return m
}

func (f compactField) physicalNames() []string {
	if f.kind == "hex" || f.kind == "hexarray" {
		return []string{"compact_" + f.name}
	}
	return []string{"compact_" + f.name + "_offsets", "compact_" + f.name + "_values"}
}

func compactBinaryDigest(s string) (string, error) {
	if len(s) != 64 {
		return "", fmt.Errorf("invalid compact digest length")
	}
	b, err := hex.DecodeString(s)
	if err != nil || hex.EncodeToString(b) != s {
		return "", fmt.Errorf("invalid compact digest")
	}
	return string(b), nil
}

func compactArray(f compactField, input any, minute time.Time) ([]uint8, any, error) {
	v := reflect.ValueOf(input)
	if v.Kind() != reflect.Slice || v.Len() != 60 {
		return nil, nil, fmt.Errorf("%s requires 60 slots", f.name)
	}
	var offsets []uint8
	var indices []int
	for i := 0; i < 60; i++ {
		if i == 0 || !reflect.DeepEqual(v.Index(i).Interface(), v.Index(i-1).Interface()) {
			offsets = append(offsets, uint8(i))
			indices = append(indices, i)
		}
	}
	if f.kind == "time" {
		values := make([]*int64, 0, len(indices))
		for _, i := range indices {
			t, ok := v.Index(i).Interface().(*time.Time)
			if !ok {
				return nil, nil, fmt.Errorf("%s requires nullable timestamps", f.name)
			}
			if t == nil {
				values = append(values, nil)
				continue
			}
			if !t.Equal(t.Truncate(time.Microsecond)) {
				return nil, nil, fmt.Errorf("timestamp precision exceeds microseconds")
			}
			diff := t.UnixMicro() - minute.UnixMicro()
			values = append(values, &diff)
		}
		return offsets, values, nil
	}
	if f.kind == "rlehex" {
		values := make([]*string, 0, len(indices))
		for _, i := range indices {
			s, ok := v.Index(i).Interface().(*string)
			if !ok {
				return nil, nil, fmt.Errorf("%s requires nullable digests", f.name)
			}
			if s == nil {
				values = append(values, nil)
				continue
			}
			x, err := compactBinaryDigest(*s)
			if err != nil {
				return nil, nil, err
			}
			values = append(values, &x)
		}
		return offsets, values, nil
	}
	values := reflect.MakeSlice(v.Type(), 0, len(indices))
	for _, i := range indices {
		values = reflect.Append(values, v.Index(i))
	}
	return offsets, values.Interface(), nil
}

func compactInsertValues(table, columns string, rows [][]any) (string, [][]any, error) {
	mapping := compactFieldMap(table)
	cols := strings.Split(columns, ",")
	var outputColumns []string
	minuteIndex := -1
	for i, name := range cols {
		name = strings.TrimSpace(name)
		cols[i] = name
		if name == "minute_time" {
			minuteIndex = i
		}
		if f, ok := mapping[name]; ok {
			outputColumns = append(outputColumns, f.physicalNames()...)
		} else {
			outputColumns = append(outputColumns, name)
		}
	}
	output := make([][]any, len(rows))
	for n, row := range rows {
		if len(row) != len(cols) {
			return "", nil, fmt.Errorf("compact row width mismatch")
		}
		var minute time.Time
		if minuteIndex >= 0 {
			var ok bool
			minute, ok = row[minuteIndex].(time.Time)
			if !ok {
				return "", nil, fmt.Errorf("compact minute must be timestamp")
			}
		}
		for i, name := range cols {
			f, ok := mapping[name]
			if !ok {
				output[n] = append(output[n], row[i])
				continue
			}
			switch f.kind {
			case "hex":
				s, ok := row[i].(string)
				if !ok {
					return "", nil, fmt.Errorf("compact digest must be string")
				}
				x, err := compactBinaryDigest(s)
				if err != nil {
					return "", nil, err
				}
				output[n] = append(output[n], x)
			case "hexarray":
				ss, ok := row[i].([]string)
				if !ok {
					return "", nil, fmt.Errorf("compact digest array must be strings")
				}
				out := make([]string, len(ss))
				for k, s := range ss {
					x, err := compactBinaryDigest(s)
					if err != nil {
						return "", nil, err
					}
					out[k] = x
				}
				output[n] = append(output[n], out)
			default:
				offsets, values, err := compactArray(f, row[i], minute)
				if err != nil {
					return "", nil, err
				}
				output[n] = append(output[n], offsets, values)
			}
		}
	}
	return strings.Join(outputColumns, ","), output, nil
}

// Old installations remain writable until explicitly migrated by an operator.
// Cache per connection; schema replacement must be followed by writer restart.
