package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"sync"
)

var sourceSchemaOnce sync.Once
var sourceSchemaTables []Table
var sourceSchemaError error

var sourceStorageNames = []string{"source_publication_intents", "source_batch_status", "public_observation_provenance"}

func SourceStorageTableNames() []string {
	names := make([]string, len(sourceStorageNames))
	for i, name := range sourceStorageNames {
		names[i] = name + "_v2"
	}
	return names
}
func sourceStorageTable(name string) bool { return slices.Contains(sourceStorageNames, name) }

// Append full records to the clustered index. Random identity probes touch a
// narrow unique secondary index instead of historical full-record pages.
// Logical columns, key hashes and all historical physical occurrences survive.
func sourceStorageMappings() ([]Table, error) {
	sourceSchemaOnce.Do(func() {
		for _, name := range sourceStorageNames {
			t, err := Mapping("crypto_market_info", name)
			if err != nil {
				sourceSchemaError = err
				return
			}
			oldPK := "PRIMARY KEY (`_key_hash`)"
			newPK := "PRIMARY KEY (`_storage_id`),\n  UNIQUE KEY `immutable_identity` (`_key_hash`)"
			if t.Multiplicity {
				oldPK = "PRIMARY KEY (`_key_hash`,`_occurrence`)"
				newPK = "PRIMARY KEY (`_storage_id`),\n  UNIQUE KEY `immutable_identity` (`_key_hash`,`_occurrence`)"
			}
			if !strings.Contains(t.DDL, oldPK) {
				sourceSchemaError = fmt.Errorf("unexpected source primary key")
				return
			}
			t.Name = name + "_v2"
			t.Columns = slices.Clone(t.Columns)
			t.TargetKey = []string{"_storage_id"}
			t.DDL = strings.Replace(t.DDL, "`"+name+"`", "`"+t.Name+"`", 1)
			t.DDL = strings.Replace(t.DDL, oldPK, "`_storage_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,\n  "+newPK, 1)
			sourceSchemaTables = append(sourceSchemaTables, t)
		}
	})
	return sourceSchemaTables, sourceSchemaError
}

// Explicit administrative install. Existing facts and migration hashes stay intact.
func InstallSourceStorageSchema(ctx context.Context, db *sql.DB, target string) error {
	if !identifier.MatchString(target) {
		return fmt.Errorf("invalid source storage schema target")
	}
	tables, err := sourceStorageMappings()
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
				return fmt.Errorf("source storage schema version conflict for %s", t.Name)
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
			return fmt.Errorf("unregistered source storage table %s", t.Name)
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

func (c *Client) discoverSourceStorage(ctx context.Context) error {
	names := SourceStorageTableNames()
	var count int
	if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cmi_storage.schema_version WHERE database_name=? AND table_name IN (?,?,?)", c.database, names[0], names[1], names[2]).Scan(&count); err != nil {
		return err
	}
	if count == 0 && !c.sourceStorageV2 {
		return nil
	}
	if count != len(names) {
		return fmt.Errorf("source storage extension requires complete administrative installation")
	}
	if err := c.checkSchema(ctx, names); err != nil {
		return err
	}
	c.sourceStorageAvailable = true
	return nil
}
