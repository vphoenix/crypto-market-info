package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
)

const (
	qualityMinuteV2 = "derivative_book_quality_minute_v2"
	qualityProfile  = "options_catalog_quality_profile_minute"
	qualityMember   = "options_catalog_quality_member_minute"
)

var qualitySchemaOnce sync.Once
var qualitySchemaTables []Table
var qualitySchemaError error

func QualityStorageTableNames() []string {
	return []string{qualityMinuteV2, qualityProfile, qualityMember}
}

// Runtime extensions do not change the frozen source migration mappings.
func qualityStorageMappings() ([]Table, error) {
	qualitySchemaOnce.Do(func() {
		book, err := Mapping("crypto_market_info", "derivative_book_quality_minute")
		if err != nil {
			qualitySchemaError = err
			return
		}
		evidence, err := Mapping("crypto_market_info", "options_catalog_quality_evidence_minute")
		if err != nil {
			qualitySchemaError = err
			return
		}
		book.Name = qualityMinuteV2
		book.Columns = slices.Clone(book.Columns)
		book.Key = []string{"minute_time", "instrument_id", "compact_batch_id"}
		book.TargetKey = slices.Clone(book.Key)
		book.DDL = strings.Replace(book.DDL, "`derivative_book_quality_minute`", "`"+qualityMinuteV2+"`", 1)
		book.DDL = strings.Replace(book.DDL, "PRIMARY KEY (`instrument_id`,`minute_time`,`compact_batch_id`)", "PRIMARY KEY (`minute_time`,`instrument_id`,`compact_batch_id`),\n  KEY `instrument_minute` (`instrument_id`,`minute_time`)", 1)
		book.DDL = storageDDL(book.DDL)
		profile := Table{Database: "crypto_market_info", Name: qualityProfile, Engine: "RuntimeImmutable", Key: []string{"minute_time", "profile_hash"}}
		member := Table{Database: "crypto_market_info", Name: qualityMember, Engine: "RuntimeImmutable", Key: []string{"minute_time", "run_id", "instrument_id", "compact_batch_id"}}
		for _, col := range evidence.Columns {
			if col.Name == "minute_time" {
				profile.Columns = append(profile.Columns, col)
			}
			if strings.HasSuffix(col.Name, "_offsets") || strings.HasSuffix(col.Name, "_values") {
				profile.Columns = append(profile.Columns, col)
			} else {
				member.Columns = append(member.Columns, col)
			}
		}
		digest := Column{Name: "profile_hash", SourceType: "FixedString(32)", MySQLType: "BINARY(32)"}
		digest.Type, err = codec.Parse(digest.SourceType)
		if err != nil {
			qualitySchemaError = err
			return
		}
		profile.Columns = append(profile.Columns, digest)
		member.Columns = append(member.Columns, digest)
		partition := evidence.DDL[strings.Index(evidence.DDL, "PARTITION BY"):]
		for _, def := range []struct {
			table *Table
			index string
		}{
			{&profile, ""}, {&member, ",\n  KEY `batch_lookup` (`minute_time`,`compact_batch_id`),\n  KEY `run_instrument_minute` (`run_id`,`instrument_id`,`minute_time`)"},
		} {
			t := def.table
			t.TargetKey = slices.Clone(t.Key)
			lines := []string{}
			for _, col := range t.Columns {
				lines = append(lines, "  `"+col.Name+"` "+col.MySQLType+" NOT NULL")
			}
			lines = append(lines, "  PRIMARY KEY (`"+strings.Join(t.Key, "`,`")+"`)"+def.index)
			t.DDL = "CREATE TABLE IF NOT EXISTS `crypto_market_info`.`" + t.Name + "` (\n" + strings.Join(lines, ",\n") + "\n) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8\n" + partition
		}
		qualitySchemaTables = []Table{book, profile, member}
	})
	return qualitySchemaTables, qualitySchemaError
}

// Explicit administrative install. Existing facts and migration hashes stay intact.
func InstallQualityStorageSchema(ctx context.Context, db *sql.DB, target string) error {
	if !identifier.MatchString(target) {
		return fmt.Errorf("invalid quality schema target")
	}
	tables, err := qualityStorageMappings()
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
				return fmt.Errorf("quality schema version conflict for %s", t.Name)
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
			return fmt.Errorf("unregistered quality table %s", t.Name)
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

func (c *Client) discoverQualityStorage(ctx context.Context) error {
	names := QualityStorageTableNames()
	var count int
	if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cmi_storage.schema_version WHERE database_name=? AND table_name IN (?,?,?)", c.database, names[0], names[1], names[2]).Scan(&count); err != nil {
		return err
	}
	if count == 0 && !c.qualityStorageV2 {
		return nil
	}
	if count != len(names) {
		return fmt.Errorf("quality storage extension requires complete administrative installation")
	}
	if err := c.checkSchema(ctx, names); err != nil {
		return err
	}
	c.qualityStorageAvailable = true
	return nil
}
