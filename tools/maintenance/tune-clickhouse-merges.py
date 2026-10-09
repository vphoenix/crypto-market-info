#!/usr/bin/env python3
"""Plan/apply a reversible 512 MiB merge-input ceiling for six local hot tables.

Only table settings change. No mutations, OPTIMIZE, deletes or server restart.
The default command only saves a plan; --apply is explicit and records progress.
"""
import argparse
import datetime
import json
import pathlib
import re
import urllib.request

DATABASE = "crypto_market_info"
TABLES = (
    "derivative_book_quality_minute", "derivative_book_second_delta",
    "derivative_book_minute", "order_book_second_delta",
    "options_catalog_live_minute_commit", "options_catalog_quality_evidence_minute",
)
SETTING = "max_bytes_to_merge_at_max_space_in_pool"
CEILING = 512 * 1024 * 1024


def query(sql, write=False):
    suffix = "" if write else "&readonly=1"
    request = urllib.request.Request(
        "http://127.0.0.1:8123/?max_execution_time=15" + suffix,
        data=(sql if write else sql + " FORMAT JSONEachRow").encode(),
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        raw = response.read().decode()
    return [] if write else [json.loads(line) for line in raw.splitlines() if line]


def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.output.exists():
        parser.error("output already exists; preserve the earlier audit")
    names = ",".join("'" + name + "'" for name in TABLES)
    metadata = query(f"SELECT name,engine,sorting_key,partition_key,create_table_query FROM system.tables WHERE database='{DATABASE}' AND name IN ({names}) ORDER BY name")
    if {row["name"] for row in metadata} != set(TABLES):
        raise RuntimeError("not all six target tables exist")
    defaults = query(f"SELECT value FROM system.merge_tree_settings WHERE name='{SETTING}'")
    default = int(defaults[0]["value"])
    parts = query(f"SELECT table,partition_id,count() parts FROM system.parts WHERE active AND database='{DATABASE}' AND table IN ({names}) GROUP BY table,partition_id")
    if any(int(row["parts"]) >= 200 for row in parts):
        raise RuntimeError("already near part-count pressure; do not reduce merge size")
    plan = {"created_at_utc": stamp(), "version": query("SELECT version() version"),
            "ceiling_bytes": CEILING, "parts_before": parts, "tables": [], "applied": []}
    for row in metadata:
        match = re.search(r"\b" + SETTING + r"\s*=\s*(\d+)", row["create_table_query"])
        old = int(match[1]) if match else default
        # Respect an existing tighter ceiling, including an intentional zero.
        if old <= CEILING:
            continue
        table = f"`{DATABASE}`.`{row['name']}`"
        rollback = (f"ALTER TABLE {table} MODIFY SETTING {SETTING}={old}" if match
                    else f"ALTER TABLE {table} RESET SETTING {SETTING}")
        plan["tables"].append({**row, "old_effective_ceiling": old,
                               "apply_sql": f"ALTER TABLE {table} MODIFY SETTING {SETTING}={CEILING}",
                               "rollback_sql": rollback})
    args.output.write_text(json.dumps(plan, indent=2) + "\n")
    if args.apply:
        for row in plan["tables"]:
            query(row["apply_sql"], write=True)
            actual = query(f"SELECT engine,sorting_key,partition_key,create_table_query FROM system.tables WHERE database='{DATABASE}' AND name='{row['name']}'")[0]
            match = re.search(r"\b" + SETTING + r"\s*=\s*(\d+)", actual["create_table_query"])
            if not match or int(match[1]) != CEILING:
                raise RuntimeError("ceiling readback failed for " + row["name"])
            if any(actual[k] != row[k] for k in ("engine", "sorting_key", "partition_key")):
                raise RuntimeError("table identity changed for " + row["name"])
            plan["applied"].append({"name": row["name"], "utc": stamp(), "readback": actual})
            args.output.write_text(json.dumps(plan, indent=2) + "\n")
    print(json.dumps({"audit": str(args.output.resolve()), "planned": len(plan["tables"]),
                      "applied": len(plan["applied"]), "ceiling_bytes": CEILING}))


if __name__ == "__main__":
    main()
