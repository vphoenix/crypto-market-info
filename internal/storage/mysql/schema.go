package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"regexp"
	"sync"

	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
)

// These are reviewed physical-column mappings. ALIAS declarations are retained
// for verification, never inserted as a second copy of the same fact.
//
//go:embed mapping.json
var mappingJSON []byte

type Column struct {
	Name       string      `json:"name"`
	SourceType string      `json:"source_type"`
	MySQLType  string      `json:"mysql_type"`
	Nullable   bool        `json:"nullable"`
	Type       *codec.Type `json:"-"`
}
type Table struct {
	Database      string   `json:"database"`
	Name          string   `json:"table"`
	Engine        string   `json:"source_engine"`
	SourceDDLHash string   `json:"source_ddl_sha256"`
	Key           []string `json:"source_key"`
	TargetKey     []string `json:"target_key"`
	HashedKey     bool     `json:"hashed_key"`
	VersionColumn string   `json:"version_column"`
	Multiplicity  bool     `json:"preserve_multiplicity"`
	Columns       []Column `json:"columns"`
	Aliases       []struct {
		Name       string `json:"name"`
		SourceType string `json:"source_type"`
		Expression string `json:"expression"`
	} `json:"aliases"`
	DDL string `json:"ddl"`
}
type Catalog struct {
	Version uint16  `json:"version"`
	Tables  []Table `json:"tables"`
}

var identifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`)

var catalogOnce sync.Once
var catalogValue Catalog
var catalogError error

// Mappings returns a read-only catalog parsed and validated once. The hot path
// does not parse the whole source inventory for each stored row.
func Mappings() (Catalog, error) {
	catalogOnce.Do(func() { catalogValue, catalogError = loadMappings() })
	return catalogValue, catalogError
}
func loadMappings() (Catalog, error) {
	var c Catalog
	if e := json.Unmarshal(mappingJSON, &c); e != nil {
		return c, e
	}
	if c.Version != 1 || len(c.Tables) != 125 {
		return c, fmt.Errorf("unexpected schema manifest")
	}
	seen := map[string]bool{}
	for i := range c.Tables {
		t := &c.Tables[i]
		key := t.Database + "." + t.Name
		if !identifier.MatchString(t.Database) || !identifier.MatchString(t.Name) || seen[key] {
			return c, fmt.Errorf("invalid table mapping")
		}
		seen[key] = true
		fields := map[string]bool{}
		for j := range t.Columns {
			f := &t.Columns[j]
			if !identifier.MatchString(f.Name) || fields[f.Name] {
				return c, fmt.Errorf("invalid column mapping")
			}
			fields[f.Name] = true
			var e error
			f.Type, e = codec.Parse(f.SourceType)
			if e != nil {
				return c, fmt.Errorf("%s.%s: %w", key, f.Name, e)
			}
		}
		for _, k := range t.Key {
			if !fields[k] {
				return c, fmt.Errorf("missing key field")
			}
		}
	}
	return c, nil
}
func Mapping(database, name string) (Table, error) {
	c, e := Mappings()
	if e != nil {
		return Table{}, e
	}
	for _, t := range c.Tables {
		if t.Database == database && t.Name == name {
			return t, nil
		}
	}
	if database == keeperRentalFamily {
		tables, err := keeperRentalStateMappings()
		if err == nil {
			var more []Table
			more, err = keeperDynamicMappings()
			tables = append(tables, more...)
		}
		if err != nil {
			return Table{}, err
		}
		for _, t := range tables {
			if t.Name == name {
				return t, nil
			}
		}
	}
	if database == "crypto_market_info" {
		tables, err := capitalVersionMappings()
		if err != nil {
			return Table{}, err
		}
		for _, t := range tables {
			if t.Name == name {
				return t, nil
			}
		}
		quality, err := qualityStorageMappings()
		if err != nil {
			return Table{}, err
		}
		for _, t := range quality {
			if t.Name == name {
				return t, nil
			}
		}
		sources, err := sourceStorageMappings()
		if err != nil {
			return Table{}, err
		}
		for _, t := range sources {
			if t.Name == name {
				return t, nil
			}
		}
	}
	return Table{}, fmt.Errorf("unmapped table %s.%s", database, name)
}

const controlDDL = `CREATE DATABASE IF NOT EXISTS cmi_storage;
CREATE TABLE IF NOT EXISTS cmi_storage.identity_allocator (
 database_name VARBINARY(64) NOT NULL, kind VARBINARY(64) NOT NULL,
 next_id BIGINT UNSIGNED NOT NULL, PRIMARY KEY(database_name,kind)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
CREATE TABLE IF NOT EXISTS cmi_storage.schema_version (
 database_name VARBINARY(64) NOT NULL, table_name VARBINARY(64) NOT NULL,
 version SMALLINT UNSIGNED NOT NULL, ddl_hash BINARY(32) NOT NULL,
 installed_at DATETIME(6) NOT NULL, PRIMARY KEY(database_name,table_name)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
CREATE TABLE IF NOT EXISTS cmi_storage.producer_generation (
 service VARBINARY(160) PRIMARY KEY, generation BIGINT UNSIGNED NOT NULL,
 holder BINARY(16) NOT NULL, activated_at DATETIME(6) NOT NULL
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
CREATE TABLE IF NOT EXISTS cmi_storage.ingest_batch (
 database_name VARBINARY(64) NOT NULL, kind VARBINARY(160) NOT NULL, batch_id VARBINARY(160) NOT NULL,
 service VARBINARY(160) NOT NULL, generation BIGINT UNSIGNED NOT NULL,
 logical_time DATETIME NOT NULL, codec SMALLINT UNSIGNED NOT NULL,
 content_hash BINARY(32) NOT NULL, member_count INT UNSIGNED NOT NULL,
 committed_at DATETIME(6) NOT NULL,
 PRIMARY KEY(database_name,kind,batch_id), KEY(logical_time)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
CREATE TABLE IF NOT EXISTS cmi_storage.ingest_member (
 database_name VARBINARY(64) NOT NULL, kind VARBINARY(160) NOT NULL,
 member_id BIGINT UNSIGNED NOT NULL, batch_id VARBINARY(160) NOT NULL,
 member_hash BINARY(32) NOT NULL,
 PRIMARY KEY(database_name,kind,member_id), KEY(database_name,kind,batch_id)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
CREATE TABLE IF NOT EXISTS cmi_storage.migration_run (
 run_id BINARY(16) PRIMARY KEY, inventory_hash BINARY(32) NOT NULL,
 mapping_version SMALLINT UNSIGNED NOT NULL, started_at DATETIME(6) NOT NULL,
 frozen_at DATETIME(6) NULL, state TINYINT UNSIGNED NOT NULL
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
CREATE TABLE IF NOT EXISTS cmi_storage.migration_table (
 run_id BINARY(16) NOT NULL, source_database VARBINARY(64) NOT NULL,
 database_name VARBINARY(64) NOT NULL, table_name VARBINARY(64) NOT NULL,
 mapping_hash BINARY(32) NOT NULL, upper_key MEDIUMBLOB NULL,
 last_key MEDIUMBLOB NULL, source_empty TINYINT UNSIGNED NOT NULL,
 state TINYINT UNSIGNED NOT NULL, error VARBINARY(2048) NULL,
 updated_at DATETIME(6) NOT NULL,
 PRIMARY KEY(run_id,database_name,table_name)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
CREATE TABLE IF NOT EXISTS cmi_storage.migration_resolution (
 chunk_id BINARY(32) NOT NULL, key_hash BINARY(32) NOT NULL,
 source_hash BINARY(32) NOT NULL, target_hash BINARY(32) NOT NULL,
 source_version MEDIUMBLOB NOT NULL, target_version MEDIUMBLOB NOT NULL,
 reason VARBINARY(256) NOT NULL, PRIMARY KEY(chunk_id,key_hash)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
CREATE TABLE IF NOT EXISTS cmi_storage.migration_chunk (
 chunk_id BINARY(32) PRIMARY KEY, run_id BINARY(16) NOT NULL,
 database_name VARBINARY(64) NOT NULL, table_name VARBINARY(64) NOT NULL,
 key_start MEDIUMBLOB NOT NULL, key_end MEDIUMBLOB NOT NULL,
 mapping_hash BINARY(32) NOT NULL, source_count BIGINT UNSIGNED NOT NULL,
 source_hash BINARY(32) NOT NULL, target_hash BINARY(32) NULL,
 state TINYINT UNSIGNED NOT NULL, last_error VARBINARY(2048) NULL,
 updated_at DATETIME(6) NOT NULL, KEY(run_id,database_name,table_name)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
CREATE TABLE IF NOT EXISTS cmi_storage.migration_conflict (
 conflict_id BINARY(32) PRIMARY KEY, run_id BINARY(16) NULL, chunk_id BINARY(32) NOT NULL,
 key_hash BINARY(32) NOT NULL, source_hash BINARY(32) NOT NULL,
 target_hash BINARY(32) NOT NULL, reason VARBINARY(256) NOT NULL,
 resolved TINYINT UNSIGNED NOT NULL DEFAULT 0, KEY(run_id,resolved)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
CREATE TABLE IF NOT EXISTS cmi_storage.history_coverage (
 database_name VARBINARY(64) NOT NULL, table_name VARBINARY(64) NOT NULL,
 range_hash BINARY(32) NOT NULL, key_start MEDIUMBLOB NOT NULL, key_end MEDIUMBLOB NOT NULL,
 state TINYINT UNSIGNED NOT NULL, verified_at DATETIME(6) NULL,
 PRIMARY KEY(database_name,table_name,range_hash)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
CREATE TABLE IF NOT EXISTS cmi_storage.bootstrap_source_gap (
 run_id BINARY(16) NOT NULL, source_database VARBINARY(64) NOT NULL,
 database_name VARBINARY(64) NOT NULL, table_name VARBINARY(64) NOT NULL,
 key_hash BINARY(32) NOT NULL, source_key MEDIUMBLOB NOT NULL,
 source_row MEDIUMBLOB NOT NULL, source_row_hash BINARY(32) NOT NULL,
 reason VARBINARY(512) NOT NULL, evidence VARBINARY(1024) NOT NULL,
 accepted_at DATETIME(6) NOT NULL,
 PRIMARY KEY(database_name,table_name,key_hash), KEY(run_id)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;`

// InitSchema is an explicit administrative operation. Open never executes DDL.
// family identifies the source schema; target may be an isolated test database.
func InitSchema(ctx context.Context, db *sql.DB, family, target string) error {
	if !identifier.MatchString(target) {
		return fmt.Errorf("invalid target database")
	}
	c, e := Mappings()
	if e != nil {
		return e
	}
	selected := []Table{}
	for _, t := range c.Tables {
		if t.Database == family {
			selected = append(selected, t)
		}
	}
	if len(selected) == 0 {
		return fmt.Errorf("database outside allowlist")
	}
	for _, s := range splitDDL(controlDDL) {
		if _, e = db.ExecContext(ctx, s); e != nil {
			return fmt.Errorf("control schema: %w", e)
		}
	}
	var conflictRunColumn int
	if e = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='cmi_storage' AND TABLE_NAME='migration_conflict' AND COLUMN_NAME='run_id'").Scan(&conflictRunColumn); e != nil {
		return e
	}
	if conflictRunColumn == 0 {
		if _, e = db.ExecContext(ctx, "ALTER TABLE cmi_storage.migration_conflict ADD COLUMN run_id BINARY(16) NULL, ADD INDEX run_conflicts(run_id,resolved)"); e != nil {
			return e
		}
	}
	if _, e = db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+target+"`"); e != nil {
		return e
	}
	for _, t := range selected {
		// This changes only the generated, validated qualified table identifier.
		// Runtime domain SQL is written for MySQL; no SQL dialect translation exists.
		ddl := "CREATE TABLE IF NOT EXISTS `" + target + "`.`" + t.Name + "`" + t.DDL[len("CREATE TABLE IF NOT EXISTS `"+family+"`.`"+t.Name+"`"):]
		h := sha256.Sum256([]byte(t.DDL))
		var old []byte
		var version uint16
		e = db.QueryRowContext(ctx, "SELECT version,ddl_hash FROM cmi_storage.schema_version WHERE database_name=? AND table_name=?", target, t.Name).Scan(&version, &old)
		if e == nil {
			if version != c.Version || string(old) != string(h[:]) {
				return fmt.Errorf("schema version conflict for %s", t.Name)
			}
			continue
		}
		if e != sql.ErrNoRows {
			return e
		}
		if _, e = db.ExecContext(ctx, storageDDL(ddl)); e != nil {
			return fmt.Errorf("%s: %w", t.Name, e)
		}
		if _, e = db.ExecContext(ctx, "INSERT INTO cmi_storage.schema_version(database_name,table_name,version,ddl_hash,installed_at) VALUES(?,?,?,?,UTC_TIMESTAMP(6))", target, t.Name, c.Version, h[:]); e != nil {
			return e
		}
	}
	return initQueryIndexes(ctx, db, family, target)
}
