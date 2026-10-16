"""Generate reviewed per-table mappings, never execute DDL against a server.

Input is the explicit project allowlist snapshot. Generated SQL and mapping are
checked in for review; live collectors do not infer schema from ClickHouse.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
ALLOWED = {
    'crypto_market_info', 'crypto_market_info_across',
    'crypto_market_info_across_livecheck', 'crypto_market_info_justlend_keeper',
    'crypto_market_info_lst', 'crypto_market_info_perp_soak',
    'crypto_market_info_perp_soak_20260928', 'crypto_market_info_perp_validation',
    'crypto_market_info_reserve', 'crypto_market_info_reserve_history',
    'crypto_market_info_reserve_scope', 'crypto_market_info_reserve_simtest',
}
IDENT = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
HOT_TIME = {'order_book_minute', 'derivative_book_minute',
            'derivative_book_quality_minute', 'options_catalog_quality_evidence_minute',
            'options_index_minute'}
HOT_ID = {'order_book_second_delta', 'derivative_book_second_delta'}


def sql_type(typ):
    nullable = typ.startswith('Nullable(')
    if nullable:
        typ = typ[9:-1]
    scalar = {'UInt8': 'TINYINT UNSIGNED', 'UInt16': 'SMALLINT UNSIGNED',
              'UInt32': 'INT UNSIGNED', 'UInt64': 'BIGINT UNSIGNED',
              'Int32': 'INT', 'Int64': 'BIGINT', 'Bool': 'TINYINT UNSIGNED',
              'UInt256': 'BINARY(32)', 'UUID': 'BINARY(16)',
              'String': 'MEDIUMBLOB', 'LowCardinality(String)': 'MEDIUMBLOB',
              'Decimal(38, 18)': 'DECIMAL(38,18)'}
    result = scalar.get(typ)
    if typ.startswith('FixedString('):
        n = int(typ[12:-1])
        if n > 255:
            raise ValueError('BINARY column exceeds supported size')
        result = 'BINARY(%d)' % n
    elif typ.startswith('Array('):
        result = 'MEDIUMBLOB'
    elif typ.startswith('Enum8('):
        result = 'TINYINT'
    elif typ == "DateTime('UTC')" or typ == "DateTime64(0, 'UTC')":
        result = 'DATETIME'
    elif typ in ("DateTime64(3, 'UTC')", "DateTime64(6, 'UTC')"):
        result = 'DATETIME(%s)' % typ[11]
    if result is None:
        raise ValueError('unsupported type ' + typ)
    return result, nullable


def generate(directory):
    inv = json.loads((directory / 'source-inventory.json').read_text())
    columns = json.loads((directory / 'source-columns.json').read_text())
    ddl, mappings = [], []
    for table in inv['tables']:
        db, name = table['database'], table['name']
        if db not in ALLOWED or not IDENT.fullmatch(name):
            raise ValueError('table outside explicit allowlist')
        fields = [c for c in columns if c['database'] == db and c['table'] == name]
        physical = [c for c in fields if c['default_kind'] != 'ALIAS']
        keys = [s.strip() for s in table['sorting_key'].split(',')]
        kinds = {c['name']: c['type'] for c in physical}
        if any(k not in kinds for k in keys):
            raise ValueError('sorting key has expression or alias: ' + name)
        for c in physical:
            if not IDENT.fullmatch(c['name']):
                raise ValueError('invalid field')
        engine = table['engine']
        if engine not in ('MergeTree', 'ReplacingMergeTree'):
            raise ValueError('unsupported source engine')
        match = re.match(r'ReplacingMergeTree\(([^)]*)\)', table['engine_full'])
        revision = match.group(1) if match else ''
        if revision and revision not in kinds:
            raise ValueError('version column missing')
        hashed = any(sql_type(kinds[k])[0] == 'MEDIUMBLOB' for k in keys)
        parts = []
        for c in physical:
            target, nullable = sql_type(c['type'])
            parts.append('  `%s` %s %s' % (c['name'], target, 'NULL' if nullable else 'NOT NULL'))
            if c['type'] in ('Bool', 'Nullable(Bool)'):
                parts.append('  CHECK (`%s` IN (0,1))' % c['name'])
        target_keys = keys.copy()
        if hashed or engine == 'MergeTree':
            parts.append('  `_key_hash` BINARY(32) NOT NULL')
            target_keys = ['_key_hash']
            if name in HOT_TIME:
                target_keys.append('minute_time')
            elif name in HOT_ID:
                target_keys.append('minute_id')
        if engine == 'MergeTree':
            parts.append('  `_occurrence` BIGINT UNSIGNED NOT NULL')
            target_keys.append('_occurrence')
        parts.append('  PRIMARY KEY (%s)' % ','.join('`%s`' % k for k in target_keys))
        if hashed:
            lookup = [('`%s`(160)' % k if sql_type(kinds[k])[0] == 'MEDIUMBLOB' else '`%s`' % k) for k in keys]
            parts.append('  KEY `business_lookup` (%s)' % ','.join(lookup))
        if name in HOT_TIME and ('compact_batch_id' in kinds or 'batch_id' in kinds):
            batch_col = 'compact_batch_id' if 'compact_batch_id' in kinds else 'batch_id'
            parts.append('  KEY `batch_lookup` (`minute_time`,`%s`)' % batch_col)
        if 'capture_id' in kinds and target_keys[0] != 'capture_id':
            parts.append('  KEY `capture_lookup` (`capture_id`)')
        # These are direct query keys, not price-history indexes.
        if name == 'order_book_minute':
            parts.append('  UNIQUE KEY `minute_identity` (`id`,`minute_time`)')
        elif name == 'instrument':
            parts.append('  `_identity_hash` BINARY(32) NOT NULL')
            parts.append('  UNIQUE KEY `instrument_identity` (`_identity_hash`)')
        sql = 'CREATE TABLE IF NOT EXISTS `%s`.`%s` (\n%s\n) ENGINE=InnoDB ROW_FORMAT=DYNAMIC' % (db, name, ',\n'.join(parts))
        partition = ''
        if name in HOT_TIME:
            partition = 'RANGE COLUMNS (`minute_time`) (' + ', '.join('PARTITION `p_2026%02d` VALUES LESS THAN (\'2026-%02d-01\')' % (m, m + 1) for m in range(1, 11)) + ', PARTITION `p_future` VALUES LESS THAN (MAXVALUE))'
        elif name in HOT_ID:
            # 2026-11-01 UTC minutes, shifted by 32 as defined by MinuteID.
            import datetime
            boundary = int(datetime.datetime(2026, 11, 1, tzinfo=datetime.timezone.utc).timestamp()) // 60 << 32
            partition = 'RANGE (`minute_id`) (' + ', '.join('PARTITION `p_2026%02d` VALUES LESS THAN (%d)' % (m, int(datetime.datetime(2026, m + 1, 1, tzinfo=datetime.timezone.utc).timestamp()) // 60 << 32) for m in range(1, 11)) + ', PARTITION `p_future` VALUES LESS THAN (MAXVALUE))'
        if partition:
            sql += '\nPARTITION BY ' + partition
        # Keep the historical mapping digest stable for frozen migrations;
        # generated installation SQL follows the current physical policy.
        ddl.append(sql.replace('ENGINE=InnoDB ROW_FORMAT=DYNAMIC',
                               'ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8', 1) + ';')
        mappings.append(dict(database=db, table=name, source_engine=engine,
                             source_ddl_sha256=hashlib.sha256(table['create_table_query'].encode()).hexdigest(),
                             source_key=keys, target_key=target_keys, hashed_key=hashed or engine == 'MergeTree',
                             version_column=revision, preserve_multiplicity=engine == 'MergeTree',
                             columns=[dict(name=c['name'], source_type=c['type'], mysql_type=sql_type(c['type'])[0], nullable=sql_type(c['type'])[1]) for c in physical],
                             aliases=[dict(name=c['name'], source_type=c['type'], expression=c['default_expression']) for c in fields if c['default_kind'] == 'ALIAS'],
                             ddl=sql))
    out = ROOT / 'internal/storage/mysql'
    (out / 'schema.sql').write_text('-- Generated by tools/storage/generate_schema.py; schema version 1.\n\n' + '\n\n'.join(ddl) + '\n')
    (out / 'mapping.json').write_text(json.dumps(dict(version=1, tables=mappings), indent=2) + '\n')
    print('Generated explicit mappings for %d allowlisted tables' % len(mappings))

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--snapshot', type=Path, default=ROOT / 'docs/redis-mysql-migration')
    generate(p.parse_args().snapshot)
