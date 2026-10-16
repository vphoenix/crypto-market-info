package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
)

// Optional runtime tables are deliberately outside the frozen 125-table source
// migration catalog. Neither mapping.json nor existing migration hashes change.
var capitalSchemaOnce sync.Once
var capitalSchemaTables []Table
var capitalSchemaError error

func CapitalVersionTableNames() []string {
	return []string{"okx_discount_rule_version", "okx_risk_rule_version", "okx_capital_observation_v2"}
}
func capitalVersionMappings() ([]Table, error) {
	capitalSchemaOnce.Do(func() {
		decimalType := "Decimal(38,18)"
		discountTiers := "Array(Tuple(minimum " + decimalType + ", maximum " + decimalType + ", rate " + decimalType + ", penalty " + decimalType + "))"
		riskTiers := "Array(Tuple(minimum " + decimalType + ", maximum " + decimalType + ", initial " + decimalType + ", maintenance " + decimalType + ", maximum_leverage " + decimalType + "))"
		str := func(name string) Column { return Column{Name: name, SourceType: "String", MySQLType: "VARBINARY(160)"} }
		digest := func(name string) Column {
			return Column{Name: name, SourceType: "FixedString(32)", MySQLType: "BINARY(32)"}
		}
		instrument := Column{Name: "instrument_id", SourceType: "UInt32", MySQLType: "INT UNSIGNED"}
		defs := []Table{
			{Name: "okx_discount_rule_version", Key: []string{"version_hash"}, Columns: []Column{digest("version_hash"), str("exchange"), str("semantics"), str("currency"), str("amount_unit"), {Name: "tiers", SourceType: discountTiers, MySQLType: "MEDIUMBLOB"}}},
			{Name: "okx_risk_rule_version", Key: []string{"version_hash"}, Columns: []Column{digest("version_hash"), str("exchange"), str("semantics"), instrument, str("base"), str("amount_unit"), str("role"), str("mode"), {Name: "tiers", SourceType: riskTiers, MySQLType: "MEDIUMBLOB"}}},
			{Name: "okx_capital_observation_v2", Key: []string{"observation_id"}, Columns: []Column{str("observation_id"), instrument, {Name: "observed_ms", SourceType: "Int64", MySQLType: "BIGINT"}, {Name: "source_ms", SourceType: "Int64", MySQLType: "BIGINT"}, {Name: "effective_ms", SourceType: "Int64", MySQLType: "BIGINT"}, {Name: "valid_until_ms", SourceType: "Int64", MySQLType: "BIGINT"}, str("base"), {Name: "borrow_leverage", SourceType: decimalType, MySQLType: "DECIMAL(38,18)"}, digest("model_hash"), digest("quote_version"), digest("base_version"), digest("perp_version"), digest("borrow_version"), digest("row_hash")}},
		}
		for _, t := range defs {
			t.Database = "crypto_market_info"
			t.Engine = "RuntimeImmutable"
			t.TargetKey = append([]string(nil), t.Key...)
			lines := []string{}
			for n := range t.Columns {
				col := &t.Columns[n]
				var err error
				col.Type, err = codec.Parse(col.SourceType)
				if err != nil {
					capitalSchemaError = err
					return
				}
				lines = append(lines, "  `"+col.Name+"` "+col.MySQLType+" NOT NULL")
			}
			lines = append(lines, "  PRIMARY KEY (`"+strings.Join(t.Key, "`,`")+"`)")
			t.DDL = "CREATE TABLE IF NOT EXISTS `crypto_market_info`.`" + t.Name + "` (\n" + strings.Join(lines, ",\n") + "\n) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8"
			capitalSchemaTables = append(capitalSchemaTables, t)
		}
	})
	return capitalSchemaTables, capitalSchemaError
}

// InstallCapitalVersionSchema is an explicit, separate administrative action.
// It never ALTERs an existing table, rewrites old facts, or resumes migration.
// Unknown pre-existing tables are refused, rather than silently blessed.
func InstallCapitalVersionSchema(ctx context.Context, db *sql.DB, target string) error {
	if !identifier.MatchString(target) {
		return fmt.Errorf("invalid capital schema target")
	}
	tables, err := capitalVersionMappings()
	if err != nil {
		return err
	}
	for _, t := range tables {
		hash := sha256.Sum256([]byte(t.DDL))
		var version uint16
		var old []byte
		err = db.QueryRowContext(ctx, "SELECT version,ddl_hash FROM cmi_storage.schema_version WHERE database_name=? AND table_name=?", target, t.Name).Scan(&version, &old)
		if err == nil {
			if version != 1 || string(old) != string(hash[:]) {
				return fmt.Errorf("capital schema version conflict for %s", t.Name)
			}
			continue
		}
		if err != sql.ErrNoRows {
			return err
		}
		var exists int
		if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", target, t.Name).Scan(&exists); err != nil {
			return err
		}
		if exists != 0 {
			return fmt.Errorf("unregistered capital table %s: verify physical definition before registering", t.Name)
		}
		ddl := strings.Replace(t.DDL, "`crypto_market_info`.", "`"+target+"`.", 1)
		if _, err = db.ExecContext(ctx, ddl); err != nil {
			return err
		}
		if _, err = db.ExecContext(ctx, "INSERT INTO cmi_storage.schema_version(database_name,table_name,version,ddl_hash,installed_at) VALUES(?,?,?,?,UTC_TIMESTAMP(6))", target, t.Name, 1, hash[:]); err != nil {
			return err
		}
	}
	return nil
}
