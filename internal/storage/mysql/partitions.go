package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// ExtendPartitions is explicit DDL administration and only splits p_future.
// Existing history is retained, and no partition is dropped or exchanged.
func ExtendPartitions(ctx context.Context, db *sql.DB, family, target string, through time.Time) error {
	if !identifier.MatchString(target) || through.Day() != 1 || through.Hour() != 0 {
		return fmt.Errorf("explicit target and month boundary required")
	}
	catalog, e := Mappings()
	if e != nil {
		return e
	}
	selected := 0
	for _, t := range catalog.Tables {
		if t.Database != family || !strings.Contains(t.DDL, "PARTITION BY RANGE COLUMNS") {
			continue
		}
		selected++
		var description sql.NullString
		if e = db.QueryRowContext(ctx, "SELECT MAX(PARTITION_DESCRIPTION) FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND PARTITION_NAME<>'p_future'", target, t.Name).Scan(&description); e != nil {
			return e
		}
		if !description.Valid {
			return fmt.Errorf("missing reviewed partition schema for %s", t.Name)
		}
		start, e := time.Parse("2006-01-02", strings.Trim(description.String, "'"))
		if e != nil {
			return e
		}
		parts := []string{}
		for at := start; !at.After(through.UTC()); at = at.AddDate(0, 1, 0) {
			if len(parts) >= 24 {
				return fmt.Errorf("partition extension exceeds 24-month quota")
			}
			parts = append(parts, fmt.Sprintf("PARTITION `p_%s` VALUES LESS THAN ('%s')", at.Format("200601"), at.AddDate(0, 1, 0).Format("2006-01-02")))
		}
		if len(parts) == 0 {
			continue
		}
		parts = append(parts, "PARTITION `p_future` VALUES LESS THAN (MAXVALUE)")
		if _, e = db.ExecContext(ctx, "ALTER TABLE `"+target+"`.`"+t.Name+"` REORGANIZE PARTITION p_future INTO ("+strings.Join(parts, ",")+")"); e != nil {
			return fmt.Errorf("extend %s: %w", t.Name, e)
		}
	}
	if selected == 0 {
		return fmt.Errorf("family has no reviewed partitioned tables")
	}
	return nil
}
