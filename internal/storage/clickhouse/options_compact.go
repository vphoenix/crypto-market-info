package clickhouse

// Compact option tables remove repeated physical representations, while their
// ALIAS columns expose the exact legacy values. Existing hashes and readers
// therefore remain valid, including for minutes migrated from the old schema.
import (
	"context"
	"encoding/hex"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
)

type compactField struct{ name, typ, kind string }

func optionCompactFields(table string) []compactField {
	hexField := func(name string) compactField { return compactField{name, "FixedString(64)", "hex"} }
	rle := func(name, typ string) compactField { return compactField{name, typ, "rle"} }
	var fields []compactField
	switch table {
	case "derivative_book_quality_minute":
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

// Split only top-level commas, preserving nested types and SQL strings.
func compactDeclarations(sql string) (string, []string, string) {
	start := strings.Index(sql, "(")
	depth, quoted := 0, false
	var fields []string
	last := start + 1
	for i := start + 1; i < len(sql); i++ {
		ch := sql[i]
		if ch == '\'' {
			quoted = !quoted
		}
		if quoted {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth == 0 {
				fields = append(fields, strings.TrimSpace(sql[last:i]))
				return sql[:start+1], fields, sql[i:]
			}
			depth--
		case ',':
			if depth == 0 {
				fields = append(fields, strings.TrimSpace(sql[last:i]))
				last = i + 1
			}
		}
	}
	panic("invalid option DDL")
}

func compactDDL(sql string) string {
	for _, table := range []string{"derivative_book_quality_minute", "options_catalog_quality_evidence_minute", "derivative_book_foundation_commit", "options_live_minute_commit", "options_catalog_live_minute_commit"} {
		if compactTableName(sql) != table {
			continue
		}
		prefix, decls, suffix := compactDeclarations(sql)
		mapping := compactFieldMap(table)
		var out []string
		for _, decl := range decls {
			name := strings.Fields(decl)[0]
			if name == "CONSTRAINT" {
				// Expanded length is always 60. The compact change-point constraints
				// above validate the actual stored arrays instead of virtual aliases.
				if strings.Contains(decl, "_60 CHECK") {
					continue
				}
				for _, f := range optionCompactFields(table) {
					if f.kind == "hex" || f.kind == "hexarray" {
						decl = regexp.MustCompile(`\b`+f.name+`\b`).ReplaceAllString(decl, f.physicalNames()[0])
					}
				}
				out = append(out, decl)
				continue
			}
			f, ok := mapping[name]
			if !ok {
				out = append(out, decl)
				continue
			}
			names := f.physicalNames()
			if f.kind == "hex" {
				out = append(out, names[0]+" FixedString(32) CODEC(ZSTD(3))", name+" "+f.typ+" ALIAS lower(hex("+names[0]+"))")
				continue
			}
			if f.kind == "hexarray" {
				out = append(out, names[0]+" Array(FixedString(32)) CODEC(ZSTD(3))", name+" "+f.typ+" ALIAS arrayMap(x -> lower(hex(x)), "+names[0]+")")
				continue
			}
			typ := f.typ
			if f.kind == "time" {
				typ = "Array(Nullable(Int64))"
			}
			if f.kind == "rlehex" {
				typ = "Array(Nullable(FixedString(32)))"
			}
			codec := "ZSTD(3)"
			if f.kind == "time" || name == "change_ids" {
				codec = "Delta, ZSTD(3)"
			}
			values := names[1]
			if f.kind == "time" {
				values = "arrayMap(value -> fromUnixTimestamp64Micro(toInt64(toUnixTimestamp(minute_time))*1000000+value,'UTC')," + values + ")"
			}
			if f.kind == "rlehex" {
				values = "arrayMap(value -> lower(hex(value))," + values + ")"
			}
			// Expand each run once, rather than searching all change points for
			// each second. This keeps query work linear in the minute's 60 slots.
			expand := "arrayFlatten(arrayMap((start,next,value) -> arrayMap(dummy -> value,range(toUInt64(next-start)))," + names[0] + ",arrayPushBack(arrayPopFront(" + names[0] + "),toUInt8(60))," + values + "))"
			out = append(out, names[0]+" Array(UInt8) CODEC(ZSTD(3))", names[1]+" "+typ+" CODEC("+codec+")", name+" "+f.typ+" ALIAS "+expand)
			out = append(out, "CONSTRAINT compact_"+name+"_valid CHECK length("+names[0]+")=length("+names[1]+") AND length("+names[0]+") BETWEEN 1 AND 60 AND "+names[0]+"[1]=0 AND arrayAll((a,b) -> a<b,arrayPopBack("+names[0]+"),arrayPopFront("+names[0]+")) AND "+names[0]+"[-1]<60")
		}
		// MergeTree sorting keys must use stored columns, not ALIAS columns.
		suffix = regexp.MustCompile(`\bbatch_id\b`).ReplaceAllString(suffix, "compact_batch_id")
		return prefix + "\n" + strings.Join(out, ",\n") + "\n" + suffix
	}
	return sql
}

func compactTableName(sql string) string {
	m := regexp.MustCompile("CREATE TABLE IF NOT EXISTS (?:`?[A-Za-z0-9_]+`?\\.)?`?([A-Za-z0-9_]+)`?").FindStringSubmatch(sql)
	if len(m) != 2 {
		return ""
	}
	return m[1]
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
func (c *Client) compactOptionInsert(ctx context.Context, table, columns string, rows [][]any) (string, [][]any, error) {
	if len(optionCompactFields(table)) == 0 {
		return columns, rows, nil
	}
	c.optionCompactMu.Lock()
	defer c.optionCompactMu.Unlock()
	compact, ok := c.optionCompactTables[table]
	if !ok {
		var count uint64
		if err := c.conn.QueryRow(ctx, "SELECT count() FROM system.columns WHERE database=? AND table=? AND name='compact_batch_id'", c.database, table).Scan(&count); err != nil {
			return "", nil, err
		}
		compact = count == 1
		if c.optionCompactTables == nil {
			c.optionCompactTables = map[string]bool{}
		}
		c.optionCompactTables[table] = compact
	}
	if !compact {
		return columns, rows, nil
	}
	return compactInsertValues(table, columns, rows)
}

// Migration plans are SQL only. They never mutate tables on collector startup.
type OptionsCompactMigration struct {
	Table             string
	CreateSQL         string
	Columns           []string
	SelectExpressions []string
	LegacyColumns     []string
}

func OptionsCompactMigrations(database string) ([]OptionsCompactMigration, error) {
	a, err := DerivativeSchemaStatements(database)
	if err != nil {
		return nil, err
	}
	b, err := OptionsCatalogSchemaStatements(database)
	if err != nil {
		return nil, err
	}
	// The legacy fixed-list live commit is also retained.
	b = append(b, compactDDL(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (run_id UUID,minute_time DateTime64(0,'UTC'),batch_id FixedString(64),run_hash FixedString(64),prepared_at DateTime64(6,'UTC'),instrument_ids Array(UInt32),member_hashes Array(FixedString(64)),anchor_count UInt32,delta_count UInt32,index_ids Array(String),index_hashes Array(FixedString(64))) ENGINE=ReplacingMergeTree PARTITION BY toYYYYMM(minute_time) ORDER BY (run_id,minute_time)`, "`"+database+"`.`options_live_minute_commit`")))
	var result []OptionsCompactMigration
	for _, sql := range append(a, b...) {
		table := compactTableName(sql)
		if len(optionCompactFields(table)) == 0 {
			continue
		}
		_, decls, _ := compactDeclarations(sql)
		plan := OptionsCompactMigration{Table: table, CreateSQL: sql}
		for _, decl := range decls {
			name := strings.Fields(decl)[0]
			if name == "CONSTRAINT" {
				continue
			}
			if !strings.HasPrefix(name, "compact_") {
				plan.LegacyColumns = append(plan.LegacyColumns, name)
			}
			if strings.Contains(decl, " ALIAS ") {
				continue
			}
			plan.Columns = append(plan.Columns, name)
			expression := name
			for _, f := range optionCompactFields(table) {
				names := f.physicalNames()
				if name != names[0] && (len(names) != 2 || name != names[1]) {
					continue
				}
				if f.kind == "hex" {
					expression = "unhex(" + f.name + ")"
					break
				}
				if f.kind == "hexarray" {
					expression = "arrayMap(x -> unhex(x)," + f.name + ")"
					break
				}
				x := f.name
				// Native NULL-safe comparison avoids converting every source slot
				// to a string during large historical migrations.
				offsets := "arrayFilter(i -> i=0 OR NOT isNotDistinctFrom(" + x + "[i+1]," + x + "[i]),range(60))"
				expression = offsets
				if name == names[1] {
					value := x + "[i+1]"
					if f.kind == "time" {
						value = "toUnixTimestamp64Micro(" + value + ")-toInt64(toUnixTimestamp(minute_time))*1000000"
					}
					if f.kind == "rlehex" {
						value = "unhex(" + value + ")"
					}
					expression = "arrayMap(i -> " + value + "," + offsets + ")"
				}
				break
			}
			plan.SelectExpressions = append(plan.SelectExpressions, expression)
		}
		result = append(result, plan)
	}
	return result, nil
}
