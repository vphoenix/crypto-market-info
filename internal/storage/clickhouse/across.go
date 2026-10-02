package clickhouse

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/vphoenix/crypto-market-info/internal/across"
)

//go:embed across_schema.sql
var acrossDDL string

// AcrossSchemaStatements contains only the seven Across research tables.
func AcrossSchemaStatements(database string) ([]string, error) {
	if !identifierPattern.MatchString(database) {
		return nil, errors.New("invalid_database")
	}
	// SQL comments contain prose semicolons; discard them before splitting the
	// fixed trusted DDL (no SQL string literal in this schema contains --).
	ddl := regexp.MustCompile(`(?m)--[^\n]*`).ReplaceAllString(acrossDDL, "")
	ss := regexp.MustCompile(`(?s)CREATE TABLE IF NOT EXISTS (across_\w+).*?;`).FindAllString(ddl, -1)
	if len(ss) != 7 {
		return nil, errors.New("across_schema_expected_seven_tables")
	}
	for i, s := range ss {
		ss[i] = regexp.MustCompile(`CREATE TABLE IF NOT EXISTS (across_\w+)`).ReplaceAllString(s, "CREATE TABLE IF NOT EXISTS `"+database+"`.`$1`")
	}
	return ss, nil
}

func (c *Client) InitAcrossSchema(ctx context.Context) error {
	ss, err := AcrossSchemaStatements(c.database)
	if err != nil {
		return err
	}
	for _, s := range ss {
		if err = c.conn.Exec(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

func acrossColumns(t reflect.Type) string {
	cols := make([]string, t.NumField())
	for i := range cols {
		cols[i] = t.Field(i).Tag.Get("ch")
	}
	return strings.Join(cols, ",")
}

func acrossRows[T any](rows []T) [][]any {
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

func acrossRead[T any](ctx context.Context, c *Client, table, where string, args ...any) ([]T, error) {
	var item T
	rows, err := c.conn.Query(ctx, "SELECT "+acrossColumns(reflect.TypeOf(item))+" FROM "+c.table(table)+" FINAL "+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []T{}
	for rows.Next() {
		var v T
		x := reflect.ValueOf(&v).Elem()
		dest := make([]any, x.NumField())
		for i := range dest {
			dest[i] = x.Field(i).Addr().Interface()
		}
		if err = rows.Scan(dest...); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// FINAL selects the latest revision first. Do not put canonical, committed or
// status predicates here: an orphaned revision must suppress its older success.
func (c *Client) AcrossCaptures(ctx context.Context, manifest string) ([]across.Capture, error) {
	return acrossRead[across.Capture](ctx, c, "across_capture", "WHERE manifest_hash=? ORDER BY started_at,capture_id", manifest)
}

func (c *Client) AcrossBatch(ctx context.Context, cap across.Capture) (across.Batch, error) {
	b := across.Batch{Capture: cap}
	var err error
	where := "WHERE capture_id=?"
	if b.Deposits, err = acrossRead[across.Deposit](ctx, c, "across_deposit", where, cap.CaptureId); err != nil {
		return b, err
	}
	if b.Updates, err = acrossRead[across.DepositUpdate](ctx, c, "across_deposit_update", where, cap.CaptureId); err != nil {
		return b, err
	}
	if b.Fills, err = acrossRead[across.Fill](ctx, c, "across_fill", where, cap.CaptureId); err != nil {
		return b, err
	}
	if b.Refunds, err = acrossRead[across.Refund](ctx, c, "across_refund", where, cap.CaptureId); err != nil {
		return b, err
	}
	if b.Receipts, err = acrossRead[across.TxReceipt](ctx, c, "across_tx_receipt", where, cap.CaptureId); err != nil {
		return b, err
	}
	if b.Probes, err = acrossRead[across.OrderProbe](ctx, c, "across_order_probe", where, cap.CaptureId); err != nil {
		return b, err
	}
	// Physical insert retries collapse before exact frozen member verification.
	return b, across.Validate(b)
}

func (c *Client) WriteAcrossBatch(ctx context.Context, b across.Batch) error {
	if err := across.Validate(b); err != nil {
		return err
	}
	if !b.Capture.Committed {
		return errors.New("across_batch_must_commit_last")
	}
	old, err := acrossRead[across.Capture](ctx, c, "across_capture", "WHERE manifest_hash=? AND chain_id=? AND capture_kind=? AND capture_id=?", b.Capture.ManifestHash, b.Capture.ChainId, b.Capture.CaptureKind, b.Capture.CaptureId)
	if err != nil {
		return err
	}
	for _, cap := range old {
		if acrossCaptureMembersID(cap) != acrossCaptureMembersID(b.Capture) {
			return errors.New("across_capture_retry_mutated")
		}
		if cap.Revision == b.Capture.Revision && across.ID(cap) != across.ID(b.Capture) {
			return errors.New("across_conflicting_capture_revision")
		}
	}
	for _, w := range []struct {
		table, columns string
		rows           [][]any
	}{
		{"across_deposit", acrossColumns(reflect.TypeOf(across.Deposit{})), acrossRows(b.Deposits)},
		{"across_deposit_update", acrossColumns(reflect.TypeOf(across.DepositUpdate{})), acrossRows(b.Updates)},
		{"across_fill", acrossColumns(reflect.TypeOf(across.Fill{})), acrossRows(b.Fills)},
		{"across_refund", acrossColumns(reflect.TypeOf(across.Refund{})), acrossRows(b.Refunds)},
		{"across_tx_receipt", acrossColumns(reflect.TypeOf(across.TxReceipt{})), acrossRows(b.Receipts)},
		{"across_order_probe", acrossColumns(reflect.TypeOf(across.OrderProbe{})), acrossRows(b.Probes)},
		{"across_capture", acrossColumns(reflect.TypeOf(across.Capture{})), acrossRows([]across.Capture{b.Capture})},
	} {
		if err := c.dexInsert(ctx, w.table, w.columns, w.rows); err != nil {
			return fmt.Errorf("%s: %w", w.table, err)
		}
	}
	return nil
}

func (c *Client) WriteAcrossRevision(ctx context.Context, cap across.Capture) error {
	old, err := acrossRead[across.Capture](ctx, c, "across_capture", "WHERE manifest_hash=? AND chain_id=? AND capture_kind=? AND capture_id=?", cap.ManifestHash, cap.ChainId, cap.CaptureKind, cap.CaptureId)
	if err != nil {
		return err
	}
	if len(old) != 1 {
		return errors.New("across_revision_missing_capture")
	}
	if acrossCaptureMembersID(old[0]) != acrossCaptureMembersID(cap) {
		return errors.New("across_revision_mutates_members")
	}
	if cap.Revision == old[0].Revision && across.ID(cap) != across.ID(old[0]) {
		return errors.New("across_conflicting_capture_revision")
	}
	return c.dexInsert(ctx, "across_capture", acrossColumns(reflect.TypeOf(cap)), acrossRows([]across.Capture{cap}))
}

func acrossCaptureMembersID(cap across.Capture) string {
	cap.Canonical = false
	cap.Finality = ""
	cap.Reason = ""
	cap.Revision = 0
	return across.ID(cap)
}
