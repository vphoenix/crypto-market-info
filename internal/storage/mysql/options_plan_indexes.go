package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

var optionsPlanQueryIndexes = map[string][]string{
	"cmi_options_plan_id":   {"plan_id"},
	"cmi_options_plan_time": {"effective_minute", "plan_id"},
}

// Neither index is unique: historical duplicate plan IDs or time boundaries
// must remain representable and must still be rejected by the typed reader.
func checkOptionsPlanIndex(ctx context.Context, db *sql.DB, target, name string) (bool, error) {
	want, ok := optionsPlanQueryIndexes[name]
	if !ok {
		return false, fmt.Errorf("unknown options plan index")
	}
	rows, e := db.QueryContext(ctx, "SELECT COLUMN_NAME,SUB_PART,NON_UNIQUE,IS_VISIBLE,COLLATION FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME='options_collection_plan' AND INDEX_NAME=? ORDER BY SEQ_IN_INDEX", target, name)
	if e != nil {
		return false, e
	}
	defer rows.Close()
	i := 0
	for rows.Next() {
		var column, visible, order string
		var prefix sql.NullInt64
		var nonUnique int
		if e = rows.Scan(&column, &prefix, &nonUnique, &visible, &order); e != nil {
			return false, e
		}
		if i >= len(want) || column != want[i] || prefix.Valid || nonUnique != 1 || visible != "YES" || order != "A" {
			return false, fmt.Errorf("options plan index shape conflict: %s", name)
		}
		i++
	}
	if e = rows.Err(); e != nil {
		return false, e
	}
	if i == 0 {
		return false, nil
	}
	if i != len(want) {
		return false, fmt.Errorf("options plan index shape conflict: %s", name)
	}
	return true, nil
}

func initOptionsPlanQueryIndexes(ctx context.Context, db *sql.DB, target string) error {
	if !identifier.MatchString(target) {
		return fmt.Errorf("invalid target database")
	}
	for _, name := range []string{"cmi_options_plan_id", "cmi_options_plan_time"} {
		installed, e := checkOptionsPlanIndex(ctx, db, target, name)
		if e != nil {
			return e
		}
		if installed {
			continue
		}
		columns := "`" + strings.Join(optionsPlanQueryIndexes[name], "`,`") + "`"
		if _, e = db.ExecContext(ctx, "ALTER TABLE `"+target+"`.`options_collection_plan` ADD INDEX `"+name+"` ("+columns+"), ALGORITHM=INPLACE, LOCK=NONE"); e != nil {
			return fmt.Errorf("options plan query index %s: %w", name, e)
		}
		installed, e = checkOptionsPlanIndex(ctx, db, target, name)
		if e != nil {
			return e
		}
		if !installed {
			return fmt.Errorf("options plan query index missing after installation: %s", name)
		}
	}
	return nil
}
