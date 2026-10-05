package clickhouse

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"github.com/google/uuid"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

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
	if len(ss) != 7 {
		return nil, errors.New("keeper_schema_expected_seven_tables")
	}
	for i, s := range ss {
		ss[i] = regexp.MustCompile(`CREATE TABLE IF NOT EXISTS (jl_keeper_\w+)`).ReplaceAllString(s, "CREATE TABLE IF NOT EXISTS `"+database+"`.`$1`")
	}
	// Additive migration for the initial deployment, before any rental facts
	// were committed. Collection never executes DDL.
	for _, column := range []string{"security_deposit_sun", "rent_index"} {
		ss = append(ss, "ALTER TABLE `"+database+"`.`jl_keeper_rental_event` ADD COLUMN IF NOT EXISTS "+column+" Nullable(UInt256)")
	}
	for _, column := range []string{"indexed_rows UInt32 DEFAULT 0", "indexed_digest Nullable(FixedString(32)) DEFAULT NULL", "parent_capture_id Nullable(UUID) DEFAULT NULL"} {
		ss = append(ss, "ALTER TABLE `"+database+"`.`jl_keeper_capture` ADD COLUMN IF NOT EXISTS "+column)
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
	return keeperRead[justlendkeeper.Capture](ctx, c, "jl_keeper_capture", "WHERE capture_started_at>=toDateTime64(?,6,'UTC') AND capture_started_at<toDateTime64(?,6,'UTC') ORDER BY capture_started_at,capture_id", keeperTimeArg(from), keeperTimeArg(to))
}

// Positional time.Time binding defaults to whole seconds in the native driver.
// Keep the database's microsecond boundary semantics explicit.
func keeperTimeArg(v any) any {
	if t, ok := v.(time.Time); ok {
		return t.UTC().Format("2006-01-02 15:04:05.000000")
	}
	return v
}

// Admit one background page. Failed enrichment is retained, with at most three
// attempts and a thirty minute retry delay. None of this blocks source fetching.
func (c *Client) KeeperNextEnrichment(ctx context.Context, configHash string, now time.Time) (*justlendkeeper.Capture, error) {
	where := `WHERE committed AND capture_kind='event_index' AND status='complete'
	 AND config_hash=? AND capture_mode!='bootstrap'
	 AND capture_id NOT IN (SELECT parent_capture_id FROM ` + c.table("jl_keeper_capture") + ` FINAL
	 WHERE committed AND capture_kind='enrichment' AND config_hash=? AND parent_capture_id IS NOT NULL
	 GROUP BY parent_capture_id HAVING countIf(status='complete')>0 OR count()>=3 OR max(available_at)>toDateTime64(?,6,'UTC'))
	 ORDER BY capture_mode IN ('live','catchup') DESC,requested_from,capture_started_at,capture_id LIMIT 1`
	caps, err := keeperRead[justlendkeeper.Capture](ctx, c, "jl_keeper_capture", where, configHash, configHash, keeperTimeArg(now.Add(-30*time.Minute)))
	if err != nil || len(caps) == 0 {
		return nil, err
	}
	return &caps[0], nil
}

// Find the contiguous prefix of closed source windows whose every committed
// page has a successful child. Later complete windows cannot hide an earlier
// missing page, but become usable as soon as the earlier gap is filled.
func (c *Client) KeeperEvidenceFrontier(ctx context.Context, configHash, kind string, from, to time.Time, chainComplete func([]justlendkeeper.Capture) (bool, error)) (time.Time, error) {
	if from.IsZero() || !from.Before(to) {
		return from, nil
	}
	caps, err := keeperRead[justlendkeeper.Capture](ctx, c, "jl_keeper_capture", `WHERE committed AND capture_kind='event_index' AND status='complete' AND config_hash=?
	 AND event_kind=? AND capture_mode IN ('live','catchup') AND requested_from<=toDateTime64(?,6,'UTC') AND requested_to>toDateTime64(?,6,'UTC') AND requested_to<=toDateTime64(?,6,'UTC')
	 ORDER BY requested_from,requested_to,capture_started_at,capture_id`, configHash, kind, keeperTimeArg(from), keeperTimeArg(from), keeperTimeArg(to))
	if err != nil || len(caps) == 0 {
		return from, err
	}
	ids := make([]uuid.UUID, 0, len(caps))
	for _, cap := range caps {
		ids = append(ids, cap.CaptureId)
	}
	rs, err := c.conn.Query(ctx, "SELECT DISTINCT assumeNotNull(parent_capture_id) FROM "+c.table("jl_keeper_capture")+" FINAL WHERE committed AND capture_kind='enrichment' AND status='complete' AND config_hash=? AND parent_capture_id IN (?)", configHash, ids)
	if err != nil {
		return from, err
	}
	defer rs.Close()
	done := map[uuid.UUID]bool{}
	for rs.Next() {
		var id uuid.UUID
		if err = rs.Scan(&id); err != nil {
			return from, err
		}
		done[id] = true
	}
	if err = rs.Err(); err != nil {
		return from, err
	}
	at := from
	for i := 0; i < len(caps); {
		j := i + 1
		for j < len(caps) && caps[j].RequestedFrom.Equal(*caps[i].RequestedFrom) && caps[j].RequestedTo.Equal(*caps[i].RequestedTo) {
			j++
		}
		lo, hi := *caps[i].RequestedFrom, *caps[i].RequestedTo
		if lo.After(at) {
			break
		}
		complete := true
		for _, cap := range caps[i:j] {
			if !done[cap.CaptureId] {
				complete = false
				break
			}
		}
		if complete {
			complete, err = chainComplete(caps[i:j])
			if err != nil {
				return from, err
			}
		}
		if !complete {
			break
		}
		if hi.After(at) {
			at = hi
		}
		i = j
	}
	return at, nil
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
	if b.IndexedEvents, e = keeperRead[justlendkeeper.IndexedEvent](ctx, c, "jl_keeper_indexed_event", "WHERE capture_id=? ORDER BY row_ordinal", cap.CaptureId); e != nil {
		return b, e
	}
	if cap.CaptureKind == "event_index" && cap.Status == "complete" {
		pages, err := c.KeeperIndexPages(ctx, []justlendkeeper.Capture{cap})
		if err != nil {
			return b, err
		}
		if p, ok := pages[cap.CaptureId]; ok {
			b.IndexPage = &p
		}
	}
	return b, justlendkeeper.Validate(b)
}

// KeeperBatches reads all four tables, including expected-empty members, and
// validates each capture. The bounded snapshot replaces report's N+1 reads.
func (c *Client) KeeperBatches(ctx context.Context, caps []justlendkeeper.Capture) (map[uuid.UUID]justlendkeeper.Batch, error) {
	if len(caps) > 256 {
		return nil, errors.New("keeper_report_batch_too_large")
	}
	out := make(map[uuid.UUID]justlendkeeper.Batch, len(caps))
	ids := make([]uuid.UUID, 0, len(caps))
	for _, cap := range caps {
		if _, ok := out[cap.CaptureId]; ok {
			return nil, errors.New("duplicate_capture_id")
		}
		out[cap.CaptureId] = justlendkeeper.Batch{Capture: cap}
		ids = append(ids, cap.CaptureId)
	}
	if len(ids) == 0 {
		return out, nil
	}
	events, e := keeperRead[justlendkeeper.RentalEvent](ctx, c, "jl_keeper_rental_event", "WHERE capture_id IN (?)", ids)
	if e != nil {
		return nil, e
	}
	for _, row := range events {
		b := out[row.CaptureId]
		b.Events = append(b.Events, row)
		out[row.CaptureId] = b
	}
	receipts, e := keeperRead[justlendkeeper.TxReceipt](ctx, c, "jl_keeper_tx_receipt", "WHERE capture_id IN (?)", ids)
	if e != nil {
		return nil, e
	}
	for _, row := range receipts {
		b := out[row.CaptureId]
		b.Receipts = append(b.Receipts, row)
		out[row.CaptureId] = b
	}
	probes, e := keeperRead[justlendkeeper.Probe](ctx, c, "jl_keeper_probe", "WHERE capture_id IN (?)", ids)
	if e != nil {
		return nil, e
	}
	for _, row := range probes {
		b := out[row.CaptureId]
		b.Probes = append(b.Probes, row)
		out[row.CaptureId] = b
	}
	costs, e := keeperRead[justlendkeeper.CostObservation](ctx, c, "jl_keeper_cost_observation", "WHERE capture_id IN (?)", ids)
	if e != nil {
		return nil, e
	}
	for _, row := range costs {
		b := out[row.CaptureId]
		b.Costs = append(b.Costs, row)
		out[row.CaptureId] = b
	}
	indexed, e := keeperRead[justlendkeeper.IndexedEvent](ctx, c, "jl_keeper_indexed_event", "WHERE capture_id IN (?) ORDER BY capture_id,row_ordinal", ids)
	if e != nil {
		return nil, e
	}
	for _, row := range indexed {
		b := out[row.CaptureId]
		b.IndexedEvents = append(b.IndexedEvents, row)
		out[row.CaptureId] = b
	}
	pages, e := c.KeeperIndexPages(ctx, caps)
	if e != nil {
		return nil, e
	}
	for id, p := range pages {
		b := out[id]
		page := p
		b.IndexPage = &page
		out[id] = b
	}
	for _, b := range out {
		if e = justlendkeeper.Validate(b); e != nil {
			return nil, e
		}
	}
	return out, nil
}

func (c *Client) WriteKeeperBatch(ctx context.Context, b justlendkeeper.Batch) error {
	if c.readOnly {
		return errors.New("read_only")
	}
	if e := justlendkeeper.Validate(b); e != nil {
		return e
	}
	if b.Capture.ParentCaptureId != nil {
		parents, e := keeperRead[justlendkeeper.Capture](ctx, c, "jl_keeper_capture", "WHERE capture_id=?", *b.Capture.ParentCaptureId)
		if e != nil {
			return e
		}
		if len(parents) != 1 {
			return errors.New("enrichment_parent_missing")
		}
		p, child := parents[0], b.Capture
		if e := justlendkeeper.ValidateEnrichmentParent(child, p); e != nil {
			return e
		}
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
		if b.IndexPage != nil {
			if e := c.WriteKeeperIndexPages(ctx, []justlendkeeper.IndexPage{*b.IndexPage}); e != nil {
				return e
			}
		}
		_, e = c.KeeperBatch(ctx, v)
		return e
	}
	if b.IndexPage != nil {
		if e := c.WriteKeeperIndexPages(ctx, []justlendkeeper.IndexPage{*b.IndexPage}); e != nil {
			return e
		}
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
	if e = keeperPartial(ctx, c, "jl_keeper_indexed_event", b.Capture.CaptureId, b.IndexedEvents); e != nil {
		return e
	}
	for _, w := range []struct {
		Table string
		Cols  string
		Rows  [][]any
	}{
		{"jl_keeper_indexed_event", keeperColumns(reflect.TypeOf(justlendkeeper.IndexedEvent{})), keeperValues(b.IndexedEvents)},
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

func (c *Client) KeeperIndexPages(ctx context.Context, caps []justlendkeeper.Capture) (map[uuid.UUID]justlendkeeper.IndexPage, error) {
	out := map[uuid.UUID]justlendkeeper.IndexPage{}
	// Frontier can include more than 256 pages in a single source window.
	for i := 0; i < len(caps); i += 256 {
		var ids []uuid.UUID
		for _, cap := range caps[i:min(i+256, len(caps))] {
			ids = append(ids, cap.CaptureId)
		}
		rows, err := keeperRead[justlendkeeper.IndexPage](ctx, c, "jl_keeper_index_page", "WHERE capture_id IN (?)", ids)
		if err != nil {
			return nil, err
		}
		for _, p := range rows {
			if _, ok := out[p.CaptureId]; ok {
				return nil, errors.New("duplicate_index_page")
			}
			out[p.CaptureId] = p
		}
	}
	return out, nil
}

func (c *Client) WriteKeeperIndexPages(ctx context.Context, pages []justlendkeeper.IndexPage) error {
	if c.readOnly {
		return errors.New("read_only")
	}
	if len(pages) > 256 {
		return errors.New("index_page_batch_too_large")
	}
	if len(pages) == 0 {
		return nil
	}
	var ids []uuid.UUID
	expected := map[uuid.UUID]justlendkeeper.IndexPage{}
	for _, p := range pages {
		if _, ok := expected[p.CaptureId]; ok {
			return errors.New("duplicate_index_page")
		}
		expected[p.CaptureId] = p
		ids = append(ids, p.CaptureId)
	}
	old, err := keeperRead[justlendkeeper.IndexPage](ctx, c, "jl_keeper_index_page", "WHERE capture_id IN (?)", ids)
	if err != nil {
		return err
	}
	for _, p := range old {
		a, _ := justlendkeeper.FactBytes(p)
		b, _ := justlendkeeper.FactBytes(expected[p.CaptureId])
		if !bytes.Equal(a, b) {
			return errors.New("index_page_retry_mutated")
		}
		delete(expected, p.CaptureId)
	}
	var pending []justlendkeeper.IndexPage
	for _, p := range pages {
		if _, ok := expected[p.CaptureId]; ok {
			pending = append(pending, p)
		}
	}
	return c.dexInsert(ctx, "jl_keeper_index_page", keeperColumns(reflect.TypeOf(justlendkeeper.IndexPage{})), keeperValues(pending))
}

func (c *Client) KeeperParentCaptures(ctx context.Context, ids []uuid.UUID) ([]justlendkeeper.Capture, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if len(ids) > 256 {
		return nil, errors.New("parent_batch_too_large")
	}
	return keeperRead[justlendkeeper.Capture](ctx, c, "jl_keeper_capture", "WHERE capture_id IN (?)", ids)
}
