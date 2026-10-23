package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

type rulePreparedKey struct{}
type ruleTransactionKey struct{}

func (c *Client) prepareTradingRuleRows(ctx context.Context, rules []options.TradingRule) ([]record, error) {
	ids := []uint32{}
	seen := map[uint32]bool{}
	rows := make([]record, 0, len(rules))
	for _, rule := range rules {
		if err := rule.Validate(); err != nil {
			return nil, err
		}
		if !seen[rule.InstrumentID] {
			seen[rule.InstrumentID] = true
			ids = append(ids, rule.InstrumentID)
		}
		above, ticks := []decimal.Decimal{}, []decimal.Decimal{}
		for _, band := range rule.Bands {
			above = append(above, band.AbovePrice)
			ticks = append(ticks, band.TickSize)
		}
		row, err := namedRecord(ruleColumns, []any{rule.ID(), rule.InstrumentID, rule.SourceTick, rule.MinAmount, rule.AmountStep, above, ticks, rule.ObservedAt, rule.KnownFrom, rule.EffectiveFrom, rule.EffectiveTimeBasis, rule.SourceURL, rule.PayloadHash})
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	// Contract identities are immutable; never retain a transaction connection
	// and generation lock while waiting for these reference reads.
	found := map[uint32]bool{}
	for start := 0; start < len(ids); start += 448 {
		part := ids[start:min(start+448, len(ids))]
		args := make([]any, len(part))
		for n, id := range part {
			args[n] = id
		}
		rs, err := c.db.QueryContext(ctx, "SELECT instrument_id FROM "+c.table("derivative_contract_spec")+" WHERE instrument_id IN ("+placeholders(len(args))+")", args...)
		if err != nil {
			return nil, err
		}
		for rs.Next() {
			var id uint32
			if err = rs.Scan(&id); err != nil {
				rs.Close()
				return nil, err
			}
			if found[id] {
				rs.Close()
				return nil, contracts.ErrConflict
			}
			found[id] = true
		}
		err = rs.Err()
		rs.Close()
		if err != nil {
			return nil, err
		}
	}
	if len(found) != len(ids) {
		return nil, fmt.Errorf("trading rule spec missing")
	}
	slices.SortFunc(rows, func(a, b record) int {
		return strings.Compare(a["trading_rule_id"].(string), b["trading_rule_id"].(string))
	})
	return rows, nil
}

func (c *Client) missingTradingRules(ctx context.Context, table Table, rows []record) ([]record, error) {
	want := map[string][32]byte{}
	unique := []record{}
	for _, row := range rows {
		id := row["trading_rule_id"].(string)
		hash, err := rowHash(table, row)
		if err != nil {
			return nil, err
		}
		if old, ok := want[id]; ok {
			if old != hash {
				return nil, contracts.ErrConflict
			}
			continue
		}
		want[id] = hash
		unique = append(unique, row)
	}
	stored := map[string]bool{}
	// Small primary-key reads bound each cold I/O operation. All candidates and
	// full field digests are checked; no missing result is cached.
	for start := 0; start < len(unique); start += 64 {
		part := unique[start:min(start+64, len(unique))]
		args := make([]any, len(part))
		for n, row := range part {
			args[n] = []byte(row["trading_rule_id"].(string))
		}
		existing, err := readRecords(ctx, c.db, c.table(table.Name), table, "WHERE trading_rule_id IN ("+placeholders(len(args))+")", args...)
		if err != nil {
			return nil, err
		}
		for _, row := range existing {
			id := row["trading_rule_id"].(string)
			hash, err := rowHash(table, row)
			if err != nil {
				return nil, err
			}
			if expected, ok := want[id]; !ok || stored[id] || hash != expected {
				return nil, contracts.ErrConflict
			}
			stored[id] = true
		}
	}
	missing := []record{}
	for _, row := range unique {
		if !stored[row["trading_rule_id"].(string)] {
			missing = append(missing, row)
		}
	}
	return missing, nil
}

func (c *Client) writePreparedTradingRules(ctx context.Context, rules []options.TradingRule) (err error) {
	defer func() { err = storageError(err) }()
	ctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	if c.readOnly {
		return fmt.Errorf("read-only MySQL client")
	}

	started := time.Now()
	rows, err := c.prepareTradingRuleRows(ctx, rules)
	if err != nil {
		return err
	}
	table, err := c.mapping("derivative_trading_rule")
	if err != nil {
		return err
	}
	if table.HashedKey || table.Multiplicity || len(table.Key) != 1 || table.Key[0] != "trading_rule_id" || len(table.TargetKey) != 1 || table.TargetKey[0] != "trading_rule_id" {
		return fmt.Errorf("unexpected immutable trading rule identity")
	}
	for attempt := 0; attempt < 3; attempt++ {
		missing, e := c.missingTradingRules(ctx, table, rows)
		if e != nil {
			return e
		}
		if elapsed := time.Since(started); elapsed >= time.Second {
			metadataDiagnostics.Warn("slow storage metadata preparation", "database", c.database,
				"operation", "trading_rules", "rows", len(rows), "missing", len(missing), "prepare_ms", elapsed.Milliseconds())
		}
		if hook, ok := ctx.Value(rulePreparedKey{}).(func() error); ok {
			if e = hook(); e != nil {
				return e
			}
		}
		err = c.tradingRuleTransaction(ctx, func(tx *sql.Tx) error {
			if hook, ok := ctx.Value(ruleTransactionKey{}).(func() error); ok {
				if e := hook(); e != nil {
					return e
				}
			}
			return insertAbsentImmutableRows(ctx, tx, c.table(table.Name), table, missing)
		})
		var duplicate *mysqlDriver.MySQLError
		if !errors.As(err, &duplicate) || duplicate.Number != 1062 {
			return err
		}
		// Rollback has completed. A competing identical insert is accepted only
		// after another complete primary-key/content comparison, then fencing.
	}
	return err
}
