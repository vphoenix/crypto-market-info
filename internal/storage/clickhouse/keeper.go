package clickhouse

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
)

//go:embed keeper_schema.sql
var keeperDDL string

func KeeperSchemaStatements(database string) ([]string, error) {
	if !identifierPattern.MatchString(database) {
		return nil, errors.New("invalid_database")
	}
	ddl := regexp.MustCompile(`(?m)--[^\n]*`).ReplaceAllString(keeperDDL, "")
	ss := regexp.MustCompile(`(?s)CREATE TABLE IF NOT EXISTS (jl_keeper_\w+).*?;`).FindAllString(ddl, -1)
	if len(ss) != 5 {
		return nil, errors.New("keeper_schema_expected_five_tables")
	}
	for i, s := range ss {
		ss[i] = regexp.MustCompile(`CREATE TABLE IF NOT EXISTS (jl_keeper_\w+)`).ReplaceAllString(s, "CREATE TABLE IF NOT EXISTS `"+database+"`.`$1`")
	}
	// Additive migration for the initial deployment, before any rental facts
	// were committed. Collection never executes DDL.
	for _, column := range []string{"security_deposit_sun", "rent_index"} {
		ss = append(ss, "ALTER TABLE `"+database+"`.`jl_keeper_rental_event` ADD COLUMN IF NOT EXISTS "+column+" Nullable(UInt256)")
	}
	return ss, nil
}
func (c *Client) InitKeeperSchema(ctx context.Context) error {
	if c.readOnly {
		return errors.New("read_only")
	}
	ss, e := KeeperSchemaStatements(c.database)
	if e != nil {
		return e
	}
	for _, s := range ss {
		if e = c.conn.Exec(ctx, s); e != nil {
			return e
		}
	}
	return nil
}

// OpenKeeperWriter performs no DDL; init-schema is the only table creation command.
func OpenKeeperWriter(ctx context.Context, cfg Config) (*Client, error) {
	return OpenLSTWriter(ctx, cfg)
}
func keeperColumns(t reflect.Type) string {
	cols := []string{}
	for i := 0; i < t.NumField(); i++ {
		cols = append(cols, t.Field(i).Tag.Get("ch"))
	}
	return strings.Join(cols, ",")
}
func keeperValues[T any](rows []T) [][]any {
	out := make([][]any, len(rows))
	for i, row := range rows {
		v := reflect.ValueOf(row)
		out[i] = make([]any, v.NumField())
		for j := range out[i] {
			out[i][j] = v.Field(j).Interface()
		}
	}
	return out
}
func keeperRead[T any](ctx context.Context, c *Client, table, where string, args ...any) ([]T, error) {
	var zero T
	rs, e := c.conn.Query(ctx, "SELECT "+keeperColumns(reflect.TypeOf(zero))+" FROM "+c.table(table)+" FINAL "+where, args...)
	if e != nil {
		return nil, e
	}
	defer rs.Close()
	out := []T{}
	for rs.Next() {
		var row T
		v := reflect.ValueOf(&row).Elem()
		dest := make([]any, v.NumField())
		for i := range dest {
			dest[i] = v.Field(i).Addr().Interface()
		}
		if e = rs.Scan(dest...); e != nil {
			return nil, e
		}
		out = append(out, row)
	}
	return out, rs.Err()
}
func (c *Client) KeeperCaptures(ctx context.Context, from, to any) ([]justlendkeeper.Capture, error) {
	return keeperRead[justlendkeeper.Capture](ctx, c, "jl_keeper_capture", "WHERE capture_started_at>=? AND capture_started_at<? ORDER BY capture_started_at,capture_id", from, to)
}
func (c *Client) KeeperBatch(ctx context.Context, cap justlendkeeper.Capture) (justlendkeeper.Batch, error) {
	b := justlendkeeper.Batch{Capture: cap}
	var e error
	if b.Events, e = keeperRead[justlendkeeper.RentalEvent](ctx, c, "jl_keeper_rental_event", "WHERE capture_id=?", cap.CaptureId); e != nil {
		return b, e
	}
	if b.Receipts, e = keeperRead[justlendkeeper.TxReceipt](ctx, c, "jl_keeper_tx_receipt", "WHERE capture_id=?", cap.CaptureId); e != nil {
		return b, e
	}
	if b.Probes, e = keeperRead[justlendkeeper.Probe](ctx, c, "jl_keeper_probe", "WHERE capture_id=?", cap.CaptureId); e != nil {
		return b, e
	}
	if b.Costs, e = keeperRead[justlendkeeper.CostObservation](ctx, c, "jl_keeper_cost_observation", "WHERE capture_id=?", cap.CaptureId); e != nil {
		return b, e
	}
	return b, justlendkeeper.Validate(b)
}
func (c *Client) WriteKeeperBatch(ctx context.Context, b justlendkeeper.Batch) error {
	if c.readOnly {
		return errors.New("read_only")
	}
	if e := justlendkeeper.Validate(b); e != nil {
		return e
	}
	old, e := keeperRead[justlendkeeper.Capture](ctx, c, "jl_keeper_capture", "WHERE capture_id=?", b.Capture.CaptureId)
	if e != nil {
		return e
	}
	for _, v := range old {
		a, _ := justlendkeeper.Freeze(v)
		z, _ := justlendkeeper.Freeze(b.Capture)
		if !bytes.Equal(a, z) {
			return errors.New("capture_retry_mutated")
		}
		_, e = c.KeeperBatch(ctx, v)
		return e
	}
	if e = c.keeperAnchors(ctx, b); e != nil {
		return e
	}
	if e = keeperPartial(ctx, c, "jl_keeper_rental_event", b.Capture.CaptureId, b.Events); e != nil {
		return e
	}
	if e = keeperPartial(ctx, c, "jl_keeper_tx_receipt", b.Capture.CaptureId, b.Receipts); e != nil {
		return e
	}
	if e = keeperPartial(ctx, c, "jl_keeper_probe", b.Capture.CaptureId, b.Probes); e != nil {
		return e
	}
	if e = keeperPartial(ctx, c, "jl_keeper_cost_observation", b.Capture.CaptureId, b.Costs); e != nil {
		return e
	}
	for _, w := range []struct {
		Table string
		Cols  string
		Rows  [][]any
	}{
		{"jl_keeper_rental_event", keeperColumns(reflect.TypeOf(justlendkeeper.RentalEvent{})), keeperValues(b.Events)},
		{"jl_keeper_tx_receipt", keeperColumns(reflect.TypeOf(justlendkeeper.TxReceipt{})), keeperValues(b.Receipts)},
		{"jl_keeper_probe", keeperColumns(reflect.TypeOf(justlendkeeper.Probe{})), keeperValues(b.Probes)},
		{"jl_keeper_cost_observation", keeperColumns(reflect.TypeOf(justlendkeeper.CostObservation{})), keeperValues(b.Costs)},
		{"jl_keeper_capture", keeperColumns(reflect.TypeOf(justlendkeeper.Capture{})), keeperValues([]justlendkeeper.Capture{b.Capture})}} {
		if e = c.dexInsert(ctx, w.Table, w.Cols, w.Rows); e != nil {
			return e
		}
	}
	return nil
}

func keeperPartial[T any](ctx context.Context, c *Client, table string, id any, expected []T) error {
	old, e := keeperRead[T](ctx, c, table, "WHERE capture_id=?", id)
	if e != nil {
		return e
	}
	members := map[string]bool{}
	for _, r := range expected {
		b, _ := justlendkeeper.FactBytes(r)
		members[justlendkeeper.Hash(b)] = true
	}
	for _, r := range old {
		b, _ := justlendkeeper.FactBytes(r)
		if !members[justlendkeeper.Hash(b)] {
			return errors.New("partial_capture_retry_mutated")
		}
	}
	return nil
}
func (c *Client) keeperAnchors(ctx context.Context, b justlendkeeper.Batch) error {
	known := map[uint64]string{}
	for _, r := range b.Events {
		if h, ok := known[r.BlockNumber]; ok && h != r.BlockHash {
			return errors.New("batch_solid_hash_conflict")
		}
		known[r.BlockNumber] = r.BlockHash
	}
	for _, r := range b.Receipts {
		if h, ok := known[r.BlockNumber]; ok && h != r.BlockHash {
			return errors.New("batch_solid_hash_conflict")
		}
		known[r.BlockNumber] = r.BlockHash
	}
	if len(known) == 0 {
		return nil
	}
	ns := []string{}
	for n := range known {
		ns = append(ns, strconv.FormatUint(n, 10))
	}
	sort.Strings(ns)
	filter := " WHERE block_number IN (" + strings.Join(ns, ",") + ")"
	rs, e := c.conn.Query(ctx, "SELECT block_number,block_hash FROM "+c.table("jl_keeper_rental_event")+" FINAL"+filter+" UNION ALL SELECT block_number,block_hash FROM "+c.table("jl_keeper_tx_receipt")+" FINAL"+filter)
	if e != nil {
		return e
	}
	defer rs.Close()
	for rs.Next() {
		var n uint64
		var h string
		if e = rs.Scan(&n, &h); e != nil {
			return e
		}
		if h != known[n] {
			return errors.New("stored_solid_hash_conflict")
		}
	}
	return rs.Err()
}
