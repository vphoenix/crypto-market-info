package clickhouse

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/lst"
)

//go:embed lst_schema.sql
var lstDDL string

// OpenLSTWriter connects to an existing database without creating a database or
// running any schema statements. Only an explicit InitLSTSchema call performs DDL.
func OpenLSTWriter(ctx context.Context, cfg Config) (*Client, error) {
	if len(cfg.Addresses) == 0 {
		cfg.Addresses = []string{"127.0.0.1:9000"}
	}
	if cfg.Database == "" {
		cfg.Database = "crypto_market_info_lst"
	}
	if !identifierPattern.MatchString(cfg.Database) {
		return nil, errors.New("invalid_database")
	}
	if cfg.Username == "" {
		cfg.Username = "default"
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 10 * time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = 250 * time.Millisecond
	}
	conn, e := ch.Open(&ch.Options{Addr: cfg.Addresses, Auth: ch.Auth{Database: cfg.Database, Username: cfg.Username, Password: cfg.Password}, DialTimeout: cfg.DialTimeout, Settings: ch.Settings{"date_time_input_format": "best_effort"}})
	if e != nil {
		return nil, e
	}
	if e = conn.Ping(ctx); e != nil {
		_ = conn.Close()
		return nil, e
	}
	return &Client{conn: conn, database: cfg.Database, writeTimeout: cfg.WriteTimeout, maxAttempts: cfg.MaxAttempts, retryDelay: cfg.RetryDelay}, nil
}
func (c *Client) lstWritable() error {
	if c == nil {
		return errors.New("lst_client_not_connected")
	}
	if c.readOnly {
		return errors.New("lst_client_read_only")
	}
	if c.maxAttempts <= 0 {
		return errors.New("lst_write_attempts_not_positive")
	}
	if c.conn == nil {
		return errors.New("lst_client_not_connected")
	}
	if c.writeTimeout <= 0 {
		return errors.New("lst_write_timeout_not_positive")
	}
	return nil
}

// LSTSchemaStatements explicitly selects only the seven LST tables and the
// existing instrument definition; calling InitSchema would create unrelated data.
func LSTSchemaStatements(database string) ([]string, error) {
	if !identifierPattern.MatchString(database) {
		return nil, errors.New("invalid_database")
	}
	statements := regexp.MustCompile(`(?s)CREATE TABLE IF NOT EXISTS (lst_\w+).*?;`).FindAllString(regexp.MustCompile(`(?m)--[^\n]*`).ReplaceAllString(lstDDL, ""), -1)
	if len(statements) != 7 {
		return nil, errors.New("lst_schema_expected_seven_tables")
	}
	for i, s := range statements {
		statements[i] = regexp.MustCompile(`CREATE TABLE IF NOT EXISTS (lst_\w+)`).ReplaceAllString(s, "CREATE TABLE IF NOT EXISTS `"+database+"`.`$1`")
	}
	existing, e := SchemaStatements(database)
	if e != nil {
		return nil, e
	}
	instrument := "CREATE TABLE IF NOT EXISTS `" + database + "`.instrument\n"
	selected := 0
	for _, s := range existing {
		if strings.HasPrefix(s, instrument) {
			statements = append(statements, s)
			selected++
		}
	}
	if selected != 1 {
		return nil, errors.New("lst_instrument_schema_not_unique")
	}
	return statements, nil
}
func (c *Client) InitLSTSchema(ctx context.Context) error {
	statements, e := LSTSchemaStatements(c.database)
	if e != nil {
		return e
	}
	for _, s := range statements {
		if e = c.conn.Exec(ctx, s); e != nil {
			return e
		}
	}
	return nil
}
func lstColumns(t reflect.Type) string {
	out := []string{}
	for i := 0; i < t.NumField(); i++ {
		out = append(out, t.Field(i).Tag.Get("ch"))
	}
	return strings.Join(out, ",")
}
func lstRows[T any](rows []T) [][]any {
	out := make([][]any, 0, len(rows))
	for _, row := range rows {
		v := reflect.ValueOf(row)
		values := make([]any, v.NumField())
		for i := range values {
			values[i] = v.Field(i).Interface()
		}
		out = append(out, values)
	}
	return out
}
func lstRead[T any](ctx context.Context, c *Client, table, where string, args ...any) ([]T, error) {
	var zero T
	rs, e := c.conn.Query(ctx, "SELECT "+lstColumns(reflect.TypeOf(zero))+" FROM "+c.table(table)+" FINAL "+where, args...)
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
func (c *Client) LSTCaptures(ctx context.Context, manifest string) ([]lst.Capture, error) {
	if len(manifest) != 32 {
		return nil, errors.New("invalid_manifest_hash")
	}
	// FINAL is evaluated before predicates. Keep orphaned rows for coverage reports.
	return lstRead[lst.Capture](ctx, c, "lst_capture", "WHERE manifest_hash=? AND committed ORDER BY started_at,capture_id", manifest)
}
func (c *Client) lstCapture(ctx context.Context, id uuid.UUID) (lst.Capture, bool, error) {
	rows, e := lstRead[lst.Capture](ctx, c, "lst_capture", "WHERE capture_id=?", id)
	if e != nil {
		return lst.Capture{}, false, e
	}
	if len(rows) > 1 {
		return lst.Capture{}, false, errors.New("duplicate_capture_across_partitions")
	}
	if len(rows) == 0 {
		return lst.Capture{}, false, nil
	}
	return rows[0], true, nil
}
func (c *Client) lstBatchRows(ctx context.Context, cap lst.Capture) (lst.Batch, error) {
	b := lst.Batch{Capture: cap}
	var e error
	where := "WHERE capture_id=?"
	if b.Protocols, e = lstRead[lst.ProtocolState](ctx, c, "lst_protocol_state", where, cap.CaptureId); e != nil {
		return b, e
	}
	if b.Quotes, e = lstRead[lst.Quote](ctx, c, "lst_quote_observation", where, cap.CaptureId); e != nil {
		return b, e
	}
	if b.Requests, e = lstRead[lst.WithdrawalRequest](ctx, c, "lst_withdrawal_request", where, cap.CaptureId); e != nil {
		return b, e
	}
	if b.Finalizations, e = lstRead[lst.WithdrawalFinalization](ctx, c, "lst_withdrawal_finalization", where, cap.CaptureId); e != nil {
		return b, e
	}
	if b.Claims, e = lstRead[lst.WithdrawalClaim](ctx, c, "lst_withdrawal_claim", where, cap.CaptureId); e != nil {
		return b, e
	}
	if b.Funding, e = lstRead[lst.FundingSettlement](ctx, c, "lst_funding_settlement", where, cap.CaptureId); e != nil {
		return b, e
	}
	return b, nil
}
func (c *Client) LSTBatch(ctx context.Context, cap lst.Capture) (lst.Batch, error) {
	current, ok, e := c.lstCapture(ctx, cap.CaptureId)
	if e != nil {
		return lst.Batch{}, e
	}
	if !ok || !current.Committed {
		return lst.Batch{}, errors.New("capture_not_committed")
	}
	if lst.CaptureIdentity(current) != lst.CaptureIdentity(cap) {
		return lst.Batch{}, errors.New("capture_identity_mismatch")
	}
	b, e := c.lstBatchRows(ctx, current)
	if e != nil {
		return b, e
	}
	return b, lst.Validate(b)
}
func lstFrozenRows(b lst.Batch) map[string]string {
	out := map[string]string{}
	groups := []struct {
		name string
		v    reflect.Value
	}{{"protocol", reflect.ValueOf(b.Protocols)}, {"quote", reflect.ValueOf(b.Quotes)}, {"request", reflect.ValueOf(b.Requests)}, {"finalization", reflect.ValueOf(b.Finalizations)}, {"claim", reflect.ValueOf(b.Claims)}, {"funding", reflect.ValueOf(b.Funding)}}
	for _, g := range groups {
		for i := 0; i < g.v.Len(); i++ {
			v := g.v.Index(i)
			key := g.name
			switch r := v.Interface().(type) {
			case lst.Quote:
				key += r.QuoteId
			case lst.WithdrawalRequest:
				key += lst.CanonicalHash([]any{r.BlockNumber, r.TransactionHash, r.LogIndex})
			case lst.WithdrawalFinalization:
				key += lst.CanonicalHash([]any{r.BlockNumber, r.TransactionHash, r.LogIndex})
			case lst.WithdrawalClaim:
				key += lst.CanonicalHash([]any{r.BlockNumber, r.TransactionHash, r.LogIndex})
			case lst.FundingSettlement:
				key += lst.CanonicalHash([]any{r.InstrumentId, r.FundingTime})
			}
			out[key] = lst.CanonicalHash(v.Interface())
		}
	}
	return out
}
func (c *Client) WriteLST(ctx context.Context, b lst.Batch) error {
	if e := c.lstWritable(); e != nil {
		return e
	}
	if e := lst.Validate(b); e != nil {
		return e
	}
	// Serializes membership checks and final commit within this Client. Captures use
	// fresh UUIDs across independent collectors; no distributed transaction is claimed.
	c.metadataMu.Lock()
	defer c.metadataMu.Unlock()
	old, ok, e := c.lstCapture(ctx, b.Capture.CaptureId)
	if e != nil {
		return e
	}
	if ok {
		if lst.CaptureIdentity(old) != lst.CaptureIdentity(b.Capture) {
			return errors.New("capture_retry_changed_frozen_data")
		}
		_, e = c.LSTBatch(ctx, old)
		return e
	}
	previous, e := c.lstBatchRows(ctx, b.Capture)
	if e != nil {
		return e
	}
	expected := lstFrozenRows(b)
	for id, hash := range lstFrozenRows(previous) {
		if expected[id] != hash {
			return errors.New("capture_retry_changed_partial_membership")
		}
	}
	if e = c.lstCheckFacts(ctx, b); e != nil {
		return e
	}
	for _, v := range []struct {
		table, columns string
		rows           [][]any
	}{
		{"lst_protocol_state", lstColumns(reflect.TypeOf(lst.ProtocolState{})), lstRows(b.Protocols)},
		{"lst_quote_observation", lstColumns(reflect.TypeOf(lst.Quote{})), lstRows(b.Quotes)},
		{"lst_withdrawal_request", lstColumns(reflect.TypeOf(lst.WithdrawalRequest{})), lstRows(b.Requests)},
		{"lst_withdrawal_finalization", lstColumns(reflect.TypeOf(lst.WithdrawalFinalization{})), lstRows(b.Finalizations)},
		{"lst_withdrawal_claim", lstColumns(reflect.TypeOf(lst.WithdrawalClaim{})), lstRows(b.Claims)},
		{"lst_funding_settlement", lstColumns(reflect.TypeOf(lst.FundingSettlement{})), lstRows(b.Funding)},
		{"lst_capture", lstColumns(reflect.TypeOf(lst.Capture{})), lstRows([]lst.Capture{b.Capture})},
	} {
		if e = c.dexInsert(ctx, v.table, v.columns, v.rows); e != nil {
			return fmt.Errorf("%s: %w", v.table, e)
		}
	}
	return nil
}
func (c *Client) WriteLSTRevision(ctx context.Context, next lst.Capture) error {
	if e := c.lstWritable(); e != nil {
		return e
	}
	c.metadataMu.Lock()
	defer c.metadataMu.Unlock()
	old, ok, e := c.lstCapture(ctx, next.CaptureId)
	if e != nil {
		return e
	}
	if !ok || !old.Committed {
		return errors.New("revision_capture_missing")
	}
	if lst.CaptureIdentity(old) != lst.CaptureIdentity(next) {
		return errors.New("revision_changed_frozen_capture")
	}
	if next.Revision <= old.Revision {
		if lst.CanonicalHash(next) == lst.CanonicalHash(old) {
			return nil
		}
		return errors.New("revision_not_increasing")
	}
	if old.Finality == "finalized" && (next.Finality != "finalized" || next.Canonical != old.Canonical) {
		return errors.New("finalized_capture_conflict")
	}
	b, e := c.lstBatchRows(ctx, next)
	if e != nil {
		return e
	}
	if e = lst.Validate(b); e != nil {
		return e
	}
	return c.dexInsert(ctx, "lst_capture", lstColumns(reflect.TypeOf(next)), lstRows([]lst.Capture{next}))
}

// Event facts deliberately omit acquisition metadata and optional receipt
// enrichment. A later receipt is evidence, never a conflicting protocol event.
func lstEventFact(row any) string {
	v := reflect.ValueOf(row)
	copy := reflect.New(v.Type()).Elem()
	copy.Set(v)
	for _, name := range []string{"CaptureId", "EventPayloadHash", "AvailableAt", "ReceiptStatus", "TransactionSender", "TransactionTo", "InputSelector", "TransactionOperationCount", "GasSampleClass", "GasUsed", "EffectiveGasPriceWei", "ReceiptPayloadHash", "ReceiptAvailableAt", "RowHash"} {
		f := copy.FieldByName(name)
		if f.IsValid() {
			f.SetZero()
		}
	}
	return lst.CanonicalHash(copy.Interface())
}
func lstReceiptCompatible(a, b any) bool {
	av, bv := reflect.ValueOf(a), reflect.ValueOf(b)
	for _, name := range []string{"ReceiptStatus", "TransactionSender", "TransactionTo", "InputSelector", "TransactionOperationCount", "GasUsed", "EffectiveGasPriceWei"} {
		x, y := av.FieldByName(name), bv.FieldByName(name)
		if x.IsValid() && y.IsValid() && !x.IsNil() && !y.IsNil() && lst.CanonicalHash(x.Interface()) != lst.CanonicalHash(y.Interface()) {
			return false
		}
	}
	return true
}
func lstCheckEvent[T any](ctx context.Context, c *Client, table string, rows []T) error {
	for _, row := range rows {
		v := reflect.ValueOf(row)
		existing, e := lstRead[T](ctx, c, table, "WHERE chain_id=? AND queue_address=? AND block_hash=? AND transaction_hash=? AND log_index=?", v.FieldByName("ChainId").Interface(), v.FieldByName("QueueAddress").Interface(), v.FieldByName("BlockHash").Interface(), v.FieldByName("TransactionHash").Interface(), v.FieldByName("LogIndex").Interface())
		if e != nil {
			return e
		}
		for _, old := range existing {
			if lstEventFact(old) != lstEventFact(row) {
				return errors.New("conflicting_protocol_event")
			}
			if !lstReceiptCompatible(old, row) {
				return errors.New("conflicting_receipt_evidence")
			}
		}
	}
	return nil
}
func (c *Client) lstCheckFacts(ctx context.Context, b lst.Batch) error {
	if e := lstCheckEvent(ctx, c, "lst_withdrawal_request", b.Requests); e != nil {
		return e
	}
	if e := lstCheckEvent(ctx, c, "lst_withdrawal_finalization", b.Finalizations); e != nil {
		return e
	}
	if e := lstCheckEvent(ctx, c, "lst_withdrawal_claim", b.Claims); e != nil {
		return e
	}
	for _, r := range b.Funding {
		old, e := lstRead[lst.FundingSettlement](ctx, c, "lst_funding_settlement", "WHERE instrument_id=? AND funding_time=fromUnixTimestamp64Micro(?)", r.InstrumentId, r.FundingTime.UnixMicro())
		if e != nil {
			return e
		}
		for _, v := range old {
			if !v.FundingRate.Equal(r.FundingRate) || v.SettlementMarkPriceTickE8 != nil && r.SettlementMarkPriceTickE8 != nil && *v.SettlementMarkPriceTickE8 != *r.SettlementMarkPriceTickE8 {
				return errors.New("conflicting_funding_settlement")
			}
		}
	}
	return nil
}
