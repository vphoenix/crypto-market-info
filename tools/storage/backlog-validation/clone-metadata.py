#!/usr/bin/env python3
"""Clone immutable references into the dedicated, network-isolated test server.

Never copies minute facts, changes production generations, or weakens durability.
"""
import hashlib
import json
import os
os.environ.setdefault('DOCKER_API_VERSION', '1.45')
from pathlib import Path
import subprocess
import time
import argparse
import datetime as dt
from docker_control import inspect as inspect_project_container
from mysql_client import command as local_command, environment as local_environment, verify as verify_local

ROOT = Path('/home/ubuntu/crypto-market-info')
OUT = ROOT / 'var/storage/write-backlog-improvement'
BENCH_BASE = OUT
BENCH = 'cmi-backlog-validation-mysql'
TABLES = ['instrument', 'derivative_contract_spec', 'derivative_trading_rule',
          'options_live_run', 'options_metadata_observation',
          'options_catalog_scope_observation', 'options_lifecycle_observation',
          'options_collection_plan']

def run():
    global OUT
    parser=argparse.ArgumentParser()
    parser.add_argument('--run-dir',default=str(OUT))
    parser.add_argument('--reset-empty-bench',action='store_true')
    args=parser.parse_args()
    assert args.run_dir in [str(BENCH_BASE),str(BENCH_BASE/'full-scope-validation'),str(BENCH_BASE/'post-retry-validation'),str(BENCH_BASE/'dex-index-validation'),str(BENCH_BASE/'streaming-gap-validation'),str(BENCH_BASE/'discovery-bulk-validation'),str(BENCH_BASE/'cache-diagnostics-validation'),str(BENCH_BASE/'publication-prefetch-validation')]
    OUT=Path(args.run_dir)
    info = inspect_project_container(BENCH)
    assert info['HostConfig']['NetworkMode'] == 'none', 'test server must be isolated'
    assert any(m['Source'] == str(BENCH_BASE / 'bench-mysql/data') and
               m['Destination'] == '/var/lib/mysql' for m in info['Mounts']), 'wrong data directory'
    verify_local('production');verify_local('isolated')
    client_env=local_environment()
    mysql_command=local_command('isolated')+['-e']
    if args.reset_empty_bench:
        check=subprocess.run(mysql_command+["SELECT @@server_uuid; SELECT COUNT(*) FROM cmi_storage.ingest_batch;"],env=client_env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=15)
        assert check.returncode==0 and check.stdout.decode().splitlines()==['a93e84b2-c7c3-11f1-bf43-7ab521822a4d','0'], 'wrong or nonempty isolated server'
        # Only unused test reference tables are reset. Their prior SQL export
        # remains in the evidence directory; production is never a target.
        for table in TABLES:
            result=subprocess.run(mysql_command+['TRUNCATE TABLE crypto_market_info.`'+table+'`;'],env=client_env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=120)
            if result.returncode:raise RuntimeError('isolated reference reset failed')
    filters = json.loads((OUT / 'fixture-reference-filters.json').read_text())
    assert set(filters) <= set(TABLES), 'unreviewed reference table'
    # Plan lookup also needs the preceding plan. Read only the narrow indexed
    # window and its active predecessor; never export all historical plans.
    manifest=json.loads((OUT/'frozen-manifest.json').read_text())
    start=dt.datetime.fromisoformat(manifest['From'].replace('Z','+00:00')).astimezone(dt.timezone.utc)
    end=dt.datetime.fromisoformat(manifest['To'].replace('Z','+00:00')).astimezone(dt.timezone.utc)
    assert end-start==dt.timedelta(minutes=90)
    from_sql=start.strftime('%Y-%m-%d %H:%M:%S');to_sql=end.strftime('%Y-%m-%d %H:%M:%S')
    plan_sql = f"""SELECT /*+ MAX_EXECUTION_TIME(5000) */ CONCAT(HEX(plan_id),':',HEX(previous_plan_id))
      FROM crypto_market_info.options_collection_plan
      WHERE effective_minute>='{from_sql}' AND effective_minute<'{to_sql}'
      UNION ALL SELECT CONCAT(HEX(plan_id),':',HEX(previous_plan_id)) FROM
      (SELECT /*+ MAX_EXECUTION_TIME(5000) */ plan_id,previous_plan_id
       FROM crypto_market_info.options_collection_plan WHERE effective_minute<='{from_sql}'
       ORDER BY effective_minute DESC LIMIT 1) first_plan"""
    result = subprocess.run(local_command('production')+['-e',plan_sql], env=client_env,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
    if result.returncode: raise RuntimeError(result.stderr.decode()[:300])
    plan_ids = set()
    for line in result.stdout.decode().splitlines():
        for value in line.split(':'):
            assert len(value) == 32 and all(c in '0123456789ABCDEF' for c in value)
            if value != '0' * 32: plan_ids.add("UNHEX('" + value + "')")
    assert plan_ids, 'no active fixture plan'
    filters['options_collection_plan'] = ['plan_id IN (' + ','.join(sorted(plan_ids)) + ')']
    dump = OUT / f'bench-referenced-metadata-{time.time_ns()}.sql'
    # Credentials stay in the child environment, never in argv, logs or SQL.
    command = local_command('production',kind='mysqldump')+[
        '--init-command=SET SESSION max_execution_time=15000',
        '--single-transaction','--no-create-info','--complete-insert','--hex-blob',
        '--skip-add-locks','--skip-triggers','--skip-disable-keys',
        '--set-gtid-purged=OFF','--no-tablespaces','--column-statistics=0','crypto_market_info']
    fd = os.open(dump, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, 'wb') as output:
        for table, predicates in sorted(filters.items()):
            for predicate in predicates:
                result = subprocess.run(command + [table, "--where="+predicate], env=client_env, stdout=output,
                                        stderr=subprocess.PIPE, timeout=40)
                if result.returncode:
                    raise RuntimeError(result.stderr.decode()[:400])
    # Refuse global durability and binary-log toggles, DDL, or database selection.
    with dump.open('rb') as f:
        for line in f:
            upper = line.upper()
            assert not any(x in upper for x in [b'SQL_LOG_BIN', b'FLUSH_LOG', b'INNODB_FLUSH_LOG',
                                                b'CREATE DATABASE', b'DROP ', b'USE `', b'SET GLOBAL'])
    verify_local('isolated')
    with dump.open('rb') as source, (OUT/'bench-metadata-import-errors.log').open('xb') as errors:
        proc = subprocess.Popen(local_command('isolated',database='crypto_market_info'),
                                env=client_env,stdin=source,stdout=subprocess.DEVNULL,stderr=errors)
        try:
            code=proc.wait(timeout=300)
        except subprocess.TimeoutExpired:
            proc.terminate();proc.wait(timeout=10);raise
        if code:raise RuntimeError(f'isolated metadata import failed ({code}); inspect private error log')
    digest = hashlib.sha256()
    with dump.open('rb') as source:
        while chunk := source.read(1 << 20): digest.update(chunk)
    (OUT / 'bench-metadata-clone.json').write_text(json.dumps({
        'tables': sorted(filters), 'bytes': dump.stat().st_size, 'sha256': digest.hexdigest(),
        'dump_file': dump.name,
        'target': BENCH, 'production_writes': 0, 'minute_facts_copied': 0}, indent=2) + '\n')
    print('immutable metadata clone complete', flush=True)

if __name__ == '__main__':
    run()
