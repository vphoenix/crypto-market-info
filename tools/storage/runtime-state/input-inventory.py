#!/usr/bin/env python3
"""Read migration metadata for a stopped-service runtime conversion.

This report preserves unknown/unreadable input; it never certifies startup
readiness or full history. It does not read source facts or resume migration.
"""
import argparse
import datetime as dt
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
DATABASES = {
    'across': 'crypto_market_info_across',
    'reserve': 'crypto_market_info_reserve',
    'lst': 'crypto_market_info_lst',
    'keeper': 'crypto_market_info_justlend_keeper',
    'dex': 'crypto_market_info',
}
ROW_LIMIT = 4096


def query(database):
    if database not in DATABASES.values():
        raise ValueError('unreviewed database')
    # Each inventory class has an overflow sentinel. There is no arbitrary
    # LIMIT treated as a complete inventory, and no business-table scan.
    suffix = f" WHERE m.database_name='{database}'"
    return f"""SET SESSION MAX_EXECUTION_TIME=15000;
SET TRANSACTION READ ONLY;
START TRANSACTION WITH CONSISTENT SNAPSHOT;
SELECT JSON_OBJECT('kind','identity','server_uuid',@@server_uuid);
SELECT JSON_OBJECT('kind','plan','run',HEX(m.run_id),'source_database',CONVERT(m.source_database USING utf8mb4),'table',CONVERT(m.table_name USING utf8mb4),'state',m.state,'run_state',r.state,'empty',m.source_empty,'upper_hex',HEX(m.upper_key),'last_hex',HEX(m.last_key),'error_hex',HEX(m.error)) FROM cmi_storage.migration_table m JOIN cmi_storage.migration_run r ON r.run_id=m.run_id {suffix} ORDER BY m.run_id,m.table_name LIMIT {ROW_LIMIT + 1};
SELECT JSON_OBJECT('kind','coverage','table',CONVERT(m.table_name USING utf8mb4),'range_hash',HEX(m.range_hash),'start_hex',HEX(m.key_start),'end_hex',HEX(m.key_end),'state',m.state) FROM cmi_storage.history_coverage m {suffix} ORDER BY m.table_name,m.range_hash LIMIT {ROW_LIMIT + 1};
SELECT JSON_OBJECT('kind','unreadable','run',HEX(m.run_id),'source_database',CONVERT(m.source_database USING utf8mb4),'table',CONVERT(m.table_name USING utf8mb4),'key_hex',HEX(m.source_key),'key_hash',HEX(m.key_hash),'row_hash',HEX(m.source_row_hash),'reason_hex',HEX(m.reason),'evidence_hex',HEX(m.evidence)) FROM cmi_storage.bootstrap_source_gap m {suffix} ORDER BY m.table_name,m.key_hash LIMIT {ROW_LIMIT + 1};
COMMIT;"""


def classify(records, expected_server):
    identity = [r for r in records if r.get('kind') == 'identity']
    if identity != [{'kind': 'identity', 'server_uuid': expected_server}]:
        raise ValueError('inventory database identity changed')
    groups = {k: [] for k in ('plan', 'coverage', 'unreadable')}
    for row in records:
        kind = row.get('kind')
        if kind == 'identity':
            continue
        if kind not in groups:
            raise ValueError('unknown inventory record')
        groups[kind].append(row)
    if any(len(rows) > ROW_LIMIT for rows in groups.values()):
        raise ValueError('inventory exceeds reviewed bound; explicit paged recovery required')
    for row in groups['plan']:
        # State 5 seals copied rows only (including startup subsets); it never
        # makes absent original records an empty or completed source range.
        row['classification'] = ('verified_table' if row['state'] == 2 else
                                 'verified_rows_only' if row['state'] == 5 else
                                 'pending_or_unknown')
    for row in groups['coverage']:
        row['classification'] = 'verified_range' if row['state'] == 2 else 'pending_or_unknown'
    return {
        'plans': groups['plan'], 'coverage': groups['coverage'],
        'unreadable': groups['unreadable'],
        'unlisted_source_ranges': 'unknown',
        'runtime_dependency_proof': 'requires_frozen_scope_comparison',
        'full_history_certified': False,
    }


def evidence(path):
    path = Path(path).resolve(strict=True)
    if not path.is_relative_to(ROOT) or path.stat().st_size > 64 << 20:
        raise ValueError('evidence must be a bounded project file')
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return {'path': str(path), 'bytes': path.stat().st_size, 'sha256': digest}


def publish(path, data):
    path = Path(path)
    if path.exists():
        raise ValueError('inventory destination already exists')
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True) + '\n').encode()
    fd, temporary = tempfile.mkstemp(prefix='.inventory-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        # A hard-link publication refuses replacement even in a concurrent run.
        os.link(temporary, path)
        os.unlink(temporary)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return hashlib.sha256(raw).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--service', choices=DATABASES, required=True)
    parser.add_argument('--server', choices=('production', 'isolated'), required=True)
    parser.add_argument('--evidence', action='append', default=[])
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    supporting = [evidence(p) for p in args.evidence]
    spec = importlib.util.spec_from_file_location('runtime_mysql_client', ROOT / 'tools/storage/backlog-validation/mysql_client.py')
    client = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(client)
    client.verify(args.server)
    result = subprocess.run(client.command(args.server) + ['--raw', '-e', query(DATABASES[args.service])],
                            env=client.environment(), capture_output=True, timeout=60)
    if result.returncode:
        raise RuntimeError('read-only migration inventory failed; no inventory published')
    records = [json.loads(line) for line in result.stdout.decode().splitlines() if line]
    inventory = classify(records, client.SERVERS[args.server][1])
    inventory.update(version=1, service=args.service, database=DATABASES[args.service],
                     server_uuid=client.SERVERS[args.server][1], evidence=supporting,
                     collected_at=dt.datetime.now(dt.timezone.utc).isoformat())
    digest = publish(args.out, inventory)
    print(json.dumps({'path': str(Path(args.out).resolve()), 'sha256': digest,
                      'plans': len(inventory['plans']), 'unreadable': len(inventory['unreadable']),
                      'full_history_certified': False}))


if __name__ == '__main__':
    main()
