"""Bounded, read-only queries; exact decimals and auditable query/result hashes."""
import datetime as dt
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from decimal import Decimal, getcontext

getcontext().prec = 70
ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / 'tools/storage/backlog-validation'))
import mysql_client as mysql

CH = '/home/ubuntu/.local/share/crypto-market-info-clickhouse-bin/clickhouse'
MAPPING = json.loads((ROOT / 'internal/storage/mysql/mapping.json').read_text())['tables']

def save(name, sql, raw, started, error=None):
    (OUT / (name + '.sql')).write_text(sql + '\n')
    (OUT / (name + '.jsonl')).write_bytes(raw)
    (OUT / (name + '.meta.json')).write_text(json.dumps({
        'started_utc': started, 'finished_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
        'sha256': hashlib.sha256(raw).hexdigest(), 'rows': len(raw.splitlines()),
        'read_only': True, 'error': error,
    }, indent=2) + '\n')

def query(name, sql, engine='mysql', seconds=18):
    started = dt.datetime.now(dt.timezone.utc).isoformat()
    if engine == 'mysql':
        args = mysql.command('production') + ['--raw', '-e',
            f'SET SESSION max_execution_time={seconds * 1000}; START TRANSACTION READ ONLY; ' + sql + '; ROLLBACK;']
        env = mysql.environment()
    else:
        args = [CH, 'client', '--host', '127.0.0.1', '--port', '9000',
                '--connect_timeout', '5', '--receive_timeout', str(seconds + 10),
                '--readonly', '1', '--max_threads', '1', '--max_execution_time', str(seconds),
                '--max_memory_usage', '700000000', '--output_format_json_quote_decimals', '1',
                '--query', sql + ' FORMAT JSONEachRow']
        env = None
    try:
        r = subprocess.run(args, env=env, capture_output=True, timeout=seconds + 15)
        if r.returncode:
            error = r.stderr.decode(errors='replace')[:2400]
            save(name, sql, b'', started, error)
            raise RuntimeError(error)
        save(name, sql, r.stdout, started)
        rows = [json.loads(s, parse_float=Decimal) for s in r.stdout.splitlines()]
        print(name, len(rows), flush=True)
        return rows
    except subprocess.TimeoutExpired:
        save(name, sql, b'', started, 'client timeout')
        raise

def obj(fields):
    return 'JSON_OBJECT(' + ','.join("'" + k + "'," + v for k, v in fields.items()) + ')'

def projection(db, table, columns=None, alias=''):
    tablemap = next(t for t in MAPPING if t['database'] == db and t['table'] == table)
    cols = tablemap['columns']
    if columns is not None:
        cols = [c for c in cols if c['name'] in columns]
    fields = {}
    for c in cols:
        n, t = c['name'], c['source_type']
        ref = (alias + '.' if alias else '') + '`' + n + '`'
        if 'Array(' in t or 'Tuple(' in t or t == 'UInt256':
            fields[n] = 'HEX(' + ref + ')'
        elif 'String' in t or t.startswith('Enum'):
            fields[n] = 'CAST(' + ref + ' AS CHAR)'
        elif 'Decimal' in t:
            fields[n] = 'CAST(' + ref + ' AS CHAR)'
        else:
            fields[n] = ref
    return obj(fields)

def array(hexstr, kind):
    raw = bytes.fromhex(hexstr)
    assert raw[0] == 1
    pos = 17
    n = int.from_bytes(raw[pos:pos+4], 'big'); pos += 4
    result = []
    for _ in range(n):
        if kind == 'str':
            size = int.from_bytes(raw[pos:pos+4], 'big'); pos += 4
            result.append(raw[pos:pos+size].decode()); pos += size
        elif kind == 'decimal':
            result.append(Decimal(int.from_bytes(raw[pos:pos+16], 'big', signed=True)) / 10**18); pos += 16
        elif kind == 'uint32':
            result.append(int.from_bytes(raw[pos:pos+4], 'big')); pos += 4
        else:
            raise ValueError(kind)
    assert pos == len(raw)
    return result

def safe(name, sql, engine='mysql', seconds=18):
    try:
        return query(name, sql, engine, seconds)
    except Exception as exc:
        print(name, 'ERROR', str(exc)[:250], flush=True)
        return []

if __name__ == '__main__':
    mode = sys.argv[1]
    if mode == 'current':
        db = 'crypto_market_info'
        safe('mysql-instruments', 'SELECT ' + projection(db, 'instrument') + ' FROM ' + db + '.instrument')
        safe('mysql-catalog', 'SELECT ' + projection(db, 'okx_paired_catalog') + ' FROM ' + db + '.okx_paired_catalog ORDER BY effective_minute DESC,observed_at DESC LIMIT 1')
        safe('mysql-loan', 'SELECT ' + projection(db, 'okx_public_loan_policy') + ' FROM ' + db + '.okx_public_loan_policy ORDER BY observed_at DESC LIMIT 1')
        for table, field in [('funding_forecast_observation','observed_ms'), ('cex_fee_schedule_rule','observed_ms')]:
            limit = int(dt.datetime.now(dt.timezone.utc).timestamp()) * 1000 - (600000 if 'forecast' in table else 86400000)
            sql = ('SELECT ' + projection(db, table, alias='f') + ' FROM ' + db + '.' + table + ' f INNER JOIN ' + db + ".source_batch_status s ON s.source_id=f.observation_id AND s.source_kind='" + table + "' WHERE s.complete=1 AND f." + field + '>=' + str(limit))
            safe('mysql-' + table, sql)
        safe('mysql-yield-routes', 'SELECT ' + projection(db,'yield_route') + ' FROM ' + db + '.yield_route')
        safe('mysql-yields', 'SELECT ' + projection(db,'yield_observation', alias='o') + ' FROM ' + db + '.yield_observation o INNER JOIN (SELECT yield_route_id,MAX(observation_time) t FROM ' + db + '.yield_observation GROUP BY yield_route_id) x ON x.yield_route_id=o.yield_route_id AND x.t=o.observation_time')
        safe('mysql-migrations', 'SELECT ' + obj({'run_id':'HEX(run_id)','started_at':'started_at','state':'state'}) + ' FROM cmi_storage.migration_run ORDER BY started_at DESC LIMIT 5')
    elif mode == 'funding':
        for db, label in [('crypto_market_info','main'),('crypto_market_info_perp_soak','soak-old'),('crypto_market_info_perp_soak_20260928','soak-new')]:
            for engine in ['clickhouse','mysql']:
                if engine == 'clickhouse':
                    safe(engine+'-'+label+'-instruments', f'SELECT * FROM {db}.instrument FINAL', engine)
                    safe(engine+'-'+label+'-funding', f'SELECT * FROM {db}.funding_rate_hourly FINAL ORDER BY instrument_id,hour_time', engine, 35)
                else:
                    safe(engine+'-'+label+'-funding', 'SELECT ' + obj({'instrument_id':'instrument_id','hour_time':'hour_time','funding_time':'funding_time','rate':'CAST(rate AS CHAR)','is_actual':'is_actual'}) + f' FROM {db}.funding_rate_hourly',engine,25)
    elif mode == 'coverage':
        for engine in ['mysql','clickhouse']:
            for db,table,timecol in [('crypto_market_info','order_book_minute','minute_time'),('crypto_market_info','funding_rate_hourly','hour_time'),('crypto_market_info','yield_observation','observation_time'),('crypto_market_info','dex_block','block_time'),('crypto_market_info_lst','lst_capture','started_at'),('crypto_market_info_across','across_capture','started_at'),('crypto_market_info_reserve','reserve_capture','available_at'),('crypto_market_info_justlend_keeper','jl_keeper_capture','capture_started_at')]:
                fields = {'first':'MIN('+timecol+')','latest':'MAX('+timecol+')','rows':'COUNT(*)'}
                sql = 'SELECT ' + (obj(fields) if engine=='mysql' else ','.join(v+' AS '+k for k,v in fields.items())) + ' FROM '+db+'.'+table + (' FINAL' if engine=='clickhouse' else '')
                safe(engine+'-'+table+'-coverage',sql,engine,20)
