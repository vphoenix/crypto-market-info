#!/usr/bin/env python3
"""Verify the authorized compression job after every table has completed."""
import datetime
import importlib.util
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / 'var/storage/mysql-compression/20261016'
REPORT = ROOT / 'research/2026-10-16-mysql-compression'
spec = importlib.util.spec_from_file_location('compress', ROOT / 'tools/storage/compress-mysql.py')
compress = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compress)
mysql = compress.mysql


def main():
    # This finisher never starts/restarts a job or a collection service.
    started = time.monotonic()
    while True:
        state = json.loads((BASE / 'status.json').read_text())
        if state['phase'] == 'complete':
            break
        if state['phase'] == 'paused':
            raise RuntimeError('compression paused; inspect status before explicitly resuming')
        if time.monotonic() - started > 24 * 3600:
            raise RuntimeError('compression verification wait exceeded 24 hours')
        time.sleep(15)
    compress.guard()
    manifest = json.loads((BASE / 'manifest.json').read_text())
    assert len(manifest['tables']) == len(state['tables']) == 137
    for table in manifest['tables']:
        db, name = table['database'], table['table']
        entry = state['tables'][db + '.' + name]
        assert entry['verified'] and compress.compressed(db, name)
        assert entry['before']['rows'] == entry['after']['rows']
        assert entry['before']['sha256'] == entry['after']['sha256']
    dbs = sorted({t['database'] for t in manifest['tables']})
    where = 'TABLE_SCHEMA IN (' + ','.join("'" + d + "'" for d in dbs) + ')'
    queries = {
        'columns': "SELECT TABLE_SCHEMA,TABLE_NAME,ORDINAL_POSITION,COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COALESCE(HEX(COLUMN_DEFAULT),'NULL'),EXTRA FROM information_schema.columns WHERE " + where + ' ORDER BY TABLE_SCHEMA,TABLE_NAME,ORDINAL_POSITION',
        'indexes': "SELECT TABLE_SCHEMA,TABLE_NAME,INDEX_NAME,NON_UNIQUE,SEQ_IN_INDEX,COLUMN_NAME,COALESCE(SUB_PART,0),IS_VISIBLE,COLLATION FROM information_schema.statistics WHERE " + where + ' ORDER BY TABLE_SCHEMA,TABLE_NAME,INDEX_NAME,SEQ_IN_INDEX',
        'partitions': "SELECT TABLE_SCHEMA,TABLE_NAME,COALESCE(PARTITION_NAME,''),COALESCE(PARTITION_METHOD,''),COALESCE(PARTITION_EXPRESSION,''),COALESCE(PARTITION_DESCRIPTION,'') FROM information_schema.partitions WHERE " + where + ' ORDER BY TABLE_SCHEMA,TABLE_NAME,PARTITION_ORDINAL_POSITION',
        'schema-version': 'SELECT database_name,table_name,version,HEX(ddl_hash) FROM cmi_storage.schema_version ORDER BY database_name,table_name',
    }
    for label, query in queries.items():
        actual = mysql.sql('production', query, limit=30) + '\n'
        (BASE / (label + '.after.tsv')).write_text(actual)
        assert actual == (BASE / (label + '.before.tsv')).read_text(), label + ' changed'
    history = ROOT / 'var/storage/history-migration/20261016'
    run = json.loads((history / 'identity.json').read_text())['run'].replace('-', '')
    query = "SELECT source_database,database_name,table_name,state,HEX(upper_key),HEX(last_key),source_empty,COALESCE(error,'') FROM cmi_storage.migration_table WHERE run_id=UNHEX('" + run + "') ORDER BY database_name,table_name; SELECT HEX(chunk_id),database_name,table_name,source_count,state,HEX(source_hash),HEX(target_hash) FROM cmi_storage.migration_chunk WHERE run_id=UNHEX('" + run + "') ORDER BY chunk_id;"
    checkpoints = mysql.sql('production', query, limit=30) + '\n'
    (BASE / 'history-checkpoints.after.tsv').write_text(checkpoints)
    assert checkpoints == (history / 'binlog-disabled/checkpoints.after.tsv').read_text()
    assert json.loads((history / 'status.json').read_text())['paused_by_user']
    before = sum(t['allocated_before'] for t in state['tables'].values())
    after = sum(t['allocated_after'] for t in state['tables'].values())
    result = {
        'verified_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'state': 'complete', 'tables': 137, 'business_tables': 125, 'control_tables': 12,
        'profile': 'ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8',
        'allocated_before_bytes': before, 'allocated_after_bytes': after,
        'saved_bytes': before - after, 'saved_percent': 100 * (1 - after / before),
        'all_physical_partitions_verified': True,
        'exact_counts_and_bounded_full_column_samples_equal': True,
        'columns_indexes_partition_boundaries_unchanged': True,
        'schema_and_migration_identities_unchanged': True,
        'history_migration_remains_paused': True, 'binlog_enabled': False,
    }
    compress.save(BASE / 'result.json', result)
    compress.save(REPORT / 'result.json', result)
    if 'error' in state:
        state['previous_attempt_error'] = state.pop('error')
        compress.save(BASE / 'status.json', state)
    with (REPORT / 'report.md').open('a') as f:
        f.write('\n## 完成核验\n\n')
        f.write('137 张表全部完成原生压缩，实际分区压缩页、精确行数及有界完整列样本核验通过；列定义、索引、分区边界、schema_version 和历史迁移断点保持。\n\n')
        f.write(f'同一数据集实际分配空间从 {before / 2**30:.2f} GiB 降至 {after / 2**30:.2f} GiB，节省 {(before-after) / 2**30:.2f} GiB（{result["saved_percent"]:.1f}%）。此比例不是稳定每日增长或吞吐验收。binlog 保持关闭，历史迁移和采集保持停止。\n')
    operations = ROOT / 'docs/runtime-operations.md'
    content = operations.read_text()
    entry = f'2026-10-16（UTC+8）压缩转换完成：137 张项目 MySQL 表（125 业务表、12 控制表）已验证为原生 `COMPRESSED`、8 KiB 压缩页；实际分配空间 {before/2**30:.2f} → {after/2**30:.2f} GiB。列、索引、分区、schema_version 和历史迁移断点保持，采集与历史迁移继续停止，binlog 保持关闭。详见[压缩执行记录](../research/2026-10-16-mysql-compression/report.md)。\n\n'
    title = '# 当前部署与运行说明\n\n'
    assert content.startswith(title)
    operations.write_text(title + entry + content[len(title):])
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
