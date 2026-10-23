package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
)

var keeperDynamicOnce sync.Once
var keeperDynamicTables []Table
var keeperDynamicErr error

func keeperDynamicMappings() ([]Table, error) {
	keeperDynamicOnce.Do(func() {
		for _, def := range keeper.DynamicTables() {
			t := Table{Database: keeperRentalFamily, Name: def.Name, Key: def.Key, TargetKey: def.Key, Engine: "RuntimeImmutable"}
			var lines []string
			for _, field := range keeper.DynamicColumns(def.Model) {
				typ, err := codec.Parse(field.Type)
				if err != nil {
					keeperDynamicErr = err
					return
				}
				base := typ
				nullable := typ.Name == "Nullable"
				if nullable {
					base = typ.Children[0]
				}
				sqlType := ""
				switch base.Name {
				case "UUID":
					sqlType = "BINARY(16)"
				case "UInt256":
					sqlType = "BINARY(32)"
				case "FixedString":
					sqlType = fmt.Sprintf("BINARY(%d)", base.Size)
				case "String":
					sqlType = "MEDIUMBLOB"
				case "UInt8", "Bool":
					sqlType = "TINYINT UNSIGNED"
				case "UInt32":
					sqlType = "INT UNSIGNED"
				case "UInt64":
					sqlType = "BIGINT UNSIGNED"
				case "DateTime64":
					sqlType = "DATETIME(6)"
				default:
					keeperDynamicErr = fmt.Errorf("unsupported dynamic type %s", field.Type)
					return
				}
				t.Columns = append(t.Columns, Column{Name: field.Name, SourceType: field.Type, MySQLType: sqlType, Nullable: nullable, Type: typ})
				null := " NOT NULL"
				if nullable {
					null = " NULL"
				}
				lines = append(lines, "  `"+field.Name+"` "+sqlType+null)
				if base.Name == "Bool" {
					lines = append(lines, "  CHECK (`"+field.Name+"` IN (0,1))")
				}
			}
			lines = append(lines, "  PRIMARY KEY (`"+strings.Join(t.Key, "`,`")+"`)")
			t.DDL = "CREATE TABLE IF NOT EXISTS `" + keeperRentalFamily + "`.`" + t.Name + "` (\n" + strings.Join(lines, ",\n") + "\n) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8"
			keeperDynamicTables = append(keeperDynamicTables, t)
		}
	})
	return keeperDynamicTables, keeperDynamicErr
}
func KeeperDynamicTableNames() []string {
	var names []string
	for _, t := range keeper.DynamicTables() {
		names = append(names, t.Name)
	}
	return names
}

func InstallKeeperDynamicSchema(ctx context.Context, db *sql.DB, target string) error {
	if !identifier.MatchString(target) {
		return fmt.Errorf("invalid keeper dynamic target")
	}
	tables, err := keeperDynamicMappings()
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
				return fmt.Errorf("keeper dynamic schema conflict: %s", t.Name)
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
			return fmt.Errorf("unregistered keeper dynamic table %s", t.Name)
		}
		ddl := strings.Replace(t.DDL, "`"+keeperRentalFamily+"`.", "`"+target+"`.", 1)
		if _, err = db.ExecContext(ctx, ddl); err != nil {
			return err
		}
		if _, err = db.ExecContext(ctx, "INSERT INTO cmi_storage.schema_version(database_name,table_name,version,ddl_hash,installed_at) VALUES(?,?,?,?,UTC_TIMESTAMP(6))", target, t.Name, 1, hash[:]); err != nil {
			return err
		}
	}
	return nil
}
func (c *Client) keeperDynamicAvailable(ctx context.Context, q executor) (bool, error) {
	c.keeperDynamicSchemaMu.Lock()
	defer c.keeperDynamicSchemaMu.Unlock()
	if c.keeperDynamicSchemaChecked {
		return c.keeperDynamicSchemaAvailable, nil
	}
	tables, err := keeperDynamicMappings()
	if err != nil {
		return false, err
	}
	count := 0
	for _, t := range tables {
		var version uint16
		var old []byte
		err = q.QueryRowContext(ctx, "SELECT version,ddl_hash FROM cmi_storage.schema_version WHERE database_name=? AND table_name=?", c.database, t.Name).Scan(&version, &old)
		if err == sql.ErrNoRows {
			var n int
			if err = q.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", c.database, t.Name).Scan(&n); err != nil {
				return false, err
			}
			if n != 0 {
				return false, fmt.Errorf("unregistered dynamic schema: %s", t.Name)
			}
			continue
		}
		if err != nil {
			return false, err
		}
		h := sha256.Sum256([]byte(t.DDL))
		if version != 1 || string(old) != string(h[:]) {
			return false, fmt.Errorf("dynamic schema mismatch: %s", t.Name)
		}
		count++
	}
	if count != 0 && count != len(tables) {
		return false, fmt.Errorf("incomplete keeper dynamic schema")
	}
	c.keeperDynamicSchemaAvailable = count == len(tables)
	c.keeperDynamicSchemaChecked = true
	return c.keeperDynamicSchemaAvailable, nil
}
func (c *Client) CheckKeeperDynamicSchema(ctx context.Context) error {
	c.keeperDynamicSchemaMu.Lock()
	c.keeperDynamicSchemaChecked = false
	c.keeperDynamicSchemaMu.Unlock()
	ok, err := c.keeperDynamicAvailable(ctx, c.db)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("keeper dynamic schema requires administrative installation")
	}
	if err = c.checkSchema(ctx, KeeperDynamicTableNames()); err != nil {
		return err
	}
	_, err = c.keeperBatchDynamicColumns(ctx)
	return err
}
func (c *Client) keeperBatchDynamicColumns(ctx context.Context) (bool, error) {
	if _, err := readModels[keeper.RentalRateObservation](ctx, c, c.db, "jl_keeper_rental_rate", "LIMIT 0"); err != nil {
		return false, err
	}
	if _, err := readModels[keeper.RentalRateBatchProof](ctx, c, c.db, "jl_keeper_rental_rate_batch", "LIMIT 0"); err != nil {
		return false, err
	}
	if _, err := readModels[keeper.SelectionMember](ctx, c, c.db, "jl_keeper_selection_member", "LIMIT 0"); err != nil {
		return false, err
	}
	if _, err := readModels[keeper.SelectionRound](ctx, c, c.db, "jl_keeper_selection_round", "LIMIT 0"); err != nil {
		return false, err
	}
	return true, nil
}
func (c *Client) InitKeeperDynamicSchema(ctx context.Context) error {
	return c.CheckKeeperDynamicSchema(ctx)
}
func (c *Client) readKeeperDynamic(ctx context.Context, q executor, b *keeper.Batch) error {
	ok, err := c.keeperDynamicAvailable(ctx, q)
	if err != nil {
		return err
	}
	if !ok {
		if b.Capture.CaptureKind == "selection_round" || b.Capture.CaptureKind == "rental_rate" {
			return fmt.Errorf("keeper dynamic schema required")
		}
		return nil
	}
	b.Rates, err = readModels[keeper.RentalRateObservation](ctx, c, q, "jl_keeper_rental_rate", "WHERE capture_id=? ORDER BY observation_index", b.Capture.CaptureId[:])
	if err != nil {
		return err
	}
	proofs, err := readModels[keeper.RentalRateBatchProof](ctx, c, q, "jl_keeper_rental_rate_batch", "WHERE capture_id=?", b.Capture.CaptureId[:])
	if err != nil {
		return err
	}
	if len(proofs) > 1 {
		return fmt.Errorf("duplicate rate proof")
	}
	if len(proofs) == 1 {
		b.RateProof = &proofs[0]
	}
	b.Selections, err = readModels[keeper.SelectionMember](ctx, c, q, "jl_keeper_selection_member", "WHERE capture_id=? ORDER BY slot_no", b.Capture.CaptureId[:])
	if err != nil {
		return err
	}
	rounds, err := readModels[keeper.SelectionRound](ctx, c, q, "jl_keeper_selection_round", "WHERE capture_id=?", b.Capture.CaptureId[:])
	if err != nil {
		return err
	}
	if len(rounds) > 1 {
		return fmt.Errorf("duplicate selection proof")
	}
	if len(rounds) == 1 {
		b.SelectionRound = &rounds[0]
	}
	return nil
}
func (c *Client) writeKeeperDynamic(ctx context.Context, q executor, b keeper.Batch) error {
	ok, err := c.keeperDynamicAvailable(ctx, q)
	if err != nil {
		return err
	}
	if !ok {
		if b.Capture.CaptureKind == "selection_round" || b.Capture.CaptureKind == "rental_rate" {
			return fmt.Errorf("keeper dynamic schema required")
		}
		return nil
	}
	if err = putModels(ctx, c, q, "jl_keeper_rental_rate", b.Rates); err != nil {
		return err
	}
	if b.RateProof != nil {
		if err = putModels(ctx, c, q, "jl_keeper_rental_rate_batch", []keeper.RentalRateBatchProof{*b.RateProof}); err != nil {
			return err
		}
	}
	if err = putModels(ctx, c, q, "jl_keeper_selection_member", b.Selections); err != nil {
		return err
	}
	if b.SelectionRound != nil {
		return putModels(ctx, c, q, "jl_keeper_selection_round", []keeper.SelectionRound{*b.SelectionRound})
	}
	return nil
}
