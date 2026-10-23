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

const keeperRentalFamily = "crypto_market_info_justlend_keeper"

var keeperRentalSchemaOnce sync.Once
var keeperRentalTables []Table
var keeperRentalSchemaErr error

func KeeperRentalStateTableNames() []string {
	return []string{"jl_keeper_rental_state_observation", "jl_keeper_rental_state_batch"}
}

// Separate runtime mappings leave the frozen source migration catalog intact.
func keeperRentalStateMappings() ([]Table, error) {
	keeperRentalSchemaOnce.Do(func() {
		defs := []Table{
			{Name: "jl_keeper_rental_state_observation", Key: []string{"capture_id", "observation_index"}, Columns: []Column{
				{Name: "capture_id", SourceType: "UUID", MySQLType: "BINARY(16)", Nullable: false},
				{Name: "capture_started_at", SourceType: "DateTime64(6, 'UTC')", MySQLType: "DATETIME(6)", Nullable: false},
				{Name: "observation_index", SourceType: "UInt32", MySQLType: "INT UNSIGNED", Nullable: false},
				{Name: "contract_address", SourceType: "FixedString(21)", MySQLType: "BINARY(21)", Nullable: false},
				{Name: "renter", SourceType: "FixedString(21)", MySQLType: "BINARY(21)", Nullable: false},
				{Name: "receiver", SourceType: "FixedString(21)", MySQLType: "BINARY(21)", Nullable: false},
				{Name: "resource_type", SourceType: "UInt8", MySQLType: "TINYINT UNSIGNED", Nullable: false},
				{Name: "candidate_origin_tx", SourceType: "Nullable(FixedString(32))", MySQLType: "BINARY(32)", Nullable: true},
				{Name: "candidate_origin_event_index", SourceType: "Nullable(UInt32)", MySQLType: "INT UNSIGNED", Nullable: true},
				{Name: "caller_address", SourceType: "FixedString(21)", MySQLType: "BINARY(21)", Nullable: false},
				{Name: "scheduled_at", SourceType: "DateTime64(6, 'UTC')", MySQLType: "DATETIME(6)", Nullable: false},
				{Name: "request_started_at", SourceType: "Nullable(DateTime64(6, 'UTC'))", MySQLType: "DATETIME(6)", Nullable: true},
				{Name: "response_received_at", SourceType: "Nullable(DateTime64(6, 'UTC'))", MySQLType: "DATETIME(6)", Nullable: true},
				{Name: "available_at", SourceType: "DateTime64(6, 'UTC')", MySQLType: "DATETIME(6)", Nullable: false},
				{Name: "endpoint_view", SourceType: "String", MySQLType: "MEDIUMBLOB", Nullable: false},
				{Name: "state_binding", SourceType: "String", MySQLType: "MEDIUMBLOB", Nullable: false},
				{Name: "head_before_number", SourceType: "Nullable(UInt64)", MySQLType: "BIGINT UNSIGNED", Nullable: true},
				{Name: "head_before_hash", SourceType: "Nullable(FixedString(32))", MySQLType: "BINARY(32)", Nullable: true},
				{Name: "head_before_time", SourceType: "Nullable(DateTime64(6, 'UTC'))", MySQLType: "DATETIME(6)", Nullable: true},
				{Name: "head_after_number", SourceType: "Nullable(UInt64)", MySQLType: "BIGINT UNSIGNED", Nullable: true},
				{Name: "head_after_hash", SourceType: "Nullable(FixedString(32))", MySQLType: "BINARY(32)", Nullable: true},
				{Name: "head_after_time", SourceType: "Nullable(DateTime64(6, 'UTC'))", MySQLType: "DATETIME(6)", Nullable: true},
				{Name: "implementation_address", SourceType: "Nullable(FixedString(21))", MySQLType: "BINARY(21)", Nullable: true},
				{Name: "implementation_code_hash", SourceType: "Nullable(FixedString(32))", MySQLType: "BINARY(32)", Nullable: true},
				{Name: "implementation_checked_at", SourceType: "Nullable(DateTime64(6, 'UTC'))", MySQLType: "DATETIME(6)", Nullable: true},
				{Name: "identity_status", SourceType: "String", MySQLType: "MEDIUMBLOB", Nullable: false},
				{Name: "status", SourceType: "String", MySQLType: "MEDIUMBLOB", Nullable: false},
				{Name: "api_success", SourceType: "Nullable(Bool)", MySQLType: "TINYINT UNSIGNED", Nullable: true},
				{Name: "tvm_result", SourceType: "String", MySQLType: "MEDIUMBLOB", Nullable: false},
				{Name: "security_deposit_sun", SourceType: "Nullable(UInt256)", MySQLType: "BINARY(32)", Nullable: true},
				{Name: "rent_index", SourceType: "Nullable(UInt256)", MySQLType: "BINARY(32)", Nullable: true},
				{Name: "liquidatable", SourceType: "Nullable(Bool)", MySQLType: "TINYINT UNSIGNED", Nullable: true},
				{Name: "energy_used", SourceType: "Nullable(UInt64)", MySQLType: "BIGINT UNSIGNED", Nullable: true},
				{Name: "energy_penalty", SourceType: "Nullable(UInt64)", MySQLType: "BIGINT UNSIGNED", Nullable: true},
				{Name: "error_code", SourceType: "String", MySQLType: "MEDIUMBLOB", Nullable: false},
				{Name: "error_message", SourceType: "String", MySQLType: "MEDIUMBLOB", Nullable: false},
				{Name: "request_payload_hash", SourceType: "Nullable(FixedString(32))", MySQLType: "BINARY(32)", Nullable: true},
				{Name: "response_payload_hash", SourceType: "Nullable(FixedString(32))", MySQLType: "BINARY(32)", Nullable: true},
			}},
			{Name: "jl_keeper_rental_state_batch", Key: []string{"capture_id"}, Columns: []Column{
				{Name: "capture_id", SourceType: "UUID", MySQLType: "BINARY(16)", Nullable: false},
				{Name: "capture_started_at", SourceType: "DateTime64(6, 'UTC')", MySQLType: "DATETIME(6)", Nullable: false},
				{Name: "row_count", SourceType: "UInt32", MySQLType: "INT UNSIGNED", Nullable: false},
				{Name: "digest", SourceType: "FixedString(32)", MySQLType: "BINARY(32)", Nullable: false},
			}},
		}
		for _, t := range defs {
			t.Database = keeperRentalFamily
			t.Engine = "RuntimeImmutable"
			t.TargetKey = append([]string(nil), t.Key...)
			var lines []string
			for i := range t.Columns {
				col := &t.Columns[i]
				col.Type, keeperRentalSchemaErr = codec.Parse(col.SourceType)
				if keeperRentalSchemaErr != nil {
					return
				}
				null := " NOT NULL"
				if col.Nullable {
					null = " NULL"
				}
				lines = append(lines, "  `"+col.Name+"` "+col.MySQLType+null)
				if col.SourceType == "Bool" || col.SourceType == "Nullable(Bool)" {
					lines = append(lines, "  CHECK (`"+col.Name+"` IN (0,1))")
				}
			}
			lines = append(lines, "  PRIMARY KEY (`"+strings.Join(t.Key, "`,`")+"`)")
			t.DDL = "CREATE TABLE IF NOT EXISTS `" + keeperRentalFamily + "`.`" + t.Name + "` (\n" + strings.Join(lines, ",\n") + "\n) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8"
			keeperRentalTables = append(keeperRentalTables, t)
		}
	})
	return keeperRentalTables, keeperRentalSchemaErr
}

// InstallKeeperRentalStateSchema is explicit administration, never collector DDL.
// Unknown physical tables are rejected; existing registered definitions must match.
func InstallKeeperRentalStateSchema(ctx context.Context, db *sql.DB, target string) error {
	if !identifier.MatchString(target) {
		return fmt.Errorf("invalid keeper rental-state target")
	}
	tables, err := keeperRentalStateMappings()
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
				return fmt.Errorf("keeper rental-state schema conflict: %s", t.Name)
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
			return fmt.Errorf("unregistered keeper rental-state table %s: verify physical definition before registering", t.Name)
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

func (c *Client) CheckKeeperRentalStateSchema(ctx context.Context) error {
	if err := c.checkSchema(ctx, KeeperRentalStateTableNames()); err != nil {
		return err
	}
	if _, err := readModels[keeper.RentalStateObservation](ctx, c, c.db, "jl_keeper_rental_state_observation", "LIMIT 0"); err != nil {
		return err
	}
	if _, err := readModels[keeper.RentalStateBatchProof](ctx, c, c.db, "jl_keeper_rental_state_batch", "LIMIT 0"); err != nil {
		return err
	}
	c.keeperRentalSchemaMu.Lock()
	defer c.keeperRentalSchemaMu.Unlock()
	c.keeperRentalSchemaChecked = true
	c.keeperRentalSchemaAvailable = true
	return nil
}
func (c *Client) InitKeeperRentalStateSchema(ctx context.Context) error {
	return c.CheckKeeperRentalStateSchema(ctx)
}

// Discover once per client, including old seven-table readers. A partial or
// conflicting installation is an error, never permission to discard members.
func (c *Client) keeperRentalStateAvailable(ctx context.Context, q executor) (bool, error) {
	c.keeperRentalSchemaMu.Lock()
	defer c.keeperRentalSchemaMu.Unlock()
	if c.keeperRentalSchemaChecked {
		return c.keeperRentalSchemaAvailable, nil
	}
	tables, err := keeperRentalStateMappings()
	if err != nil {
		return false, err
	}
	rows, err := q.QueryContext(ctx, "SELECT table_name,version,ddl_hash FROM cmi_storage.schema_version WHERE database_name=? AND table_name IN (?,?)", c.database, tables[0].Name, tables[1].Name)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	seen := map[string]bool{}
	for rows.Next() {
		var name string
		var version uint16
		var hash []byte
		if err = rows.Scan(&name, &version, &hash); err != nil {
			return false, err
		}
		for _, t := range tables {
			if t.Name == name {
				expected := sha256.Sum256([]byte(t.DDL))
				if seen[name] || version != 1 || string(hash) != string(expected[:]) {
					return false, fmt.Errorf("keeper rental-state schema conflict: %s", name)
				}
				seen[name] = true
			}
		}
	}
	if err = rows.Err(); err != nil {
		return false, err
	}
	if len(seen) != 0 && len(seen) != 2 {
		return false, fmt.Errorf("keeper rental-state schema incomplete")
	}
	if len(seen) == 0 {
		// Do not hide unregistered extension facts behind a legacy-schema result.
		if err = rows.Close(); err != nil {
			return false, err
		}
		var count int
		if err = q.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME IN (?,?)", c.database, tables[0].Name, tables[1].Name).Scan(&count); err != nil {
			return false, err
		}
		if count != 0 {
			return false, fmt.Errorf("unregistered keeper rental-state tables require administrative verification")
		}
	}
	c.keeperRentalSchemaChecked = true
	c.keeperRentalSchemaAvailable = len(seen) == 2
	return c.keeperRentalSchemaAvailable, nil
}
