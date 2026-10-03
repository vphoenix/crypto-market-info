#!/usr/bin/env python3
"""Read-only ClickHouse yield inventory; Decimal calculations, no account access."""
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal, getcontext
from pathlib import Path
import argparse
import hashlib
import json
import urllib.request

getcontext().prec = 50
ROOT = Path(__file__).resolve().parent
D = Decimal

def dump(path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str) + "\n")

def query(name, sql):
    sql += " SETTINGS output_format_json_quote_decimals=1 FORMAT JSONEachRow"
    (ROOT / (name + ".sql")).write_text(sql + "\n")
    with urllib.request.urlopen(urllib.request.Request("http://127.0.0.1:8123/", data=sql.encode()), timeout=120) as response:
        raw = response.read()
    (ROOT / (name + ".jsonl")).write_bytes(raw)
    return {"file": name + ".jsonl", "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}

def read(name):
    return [json.loads(line, parse_float=D) for line in (ROOT / (name + ".jsonl")).read_text().splitlines()]

def dt(value):
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)

def stats(values):
    values = sorted(values)
    if not values:
        return None
    return {"n": len(values), "min": values[0], "median": values[(len(values)-1)//2], "max": values[-1], "mean": sum(values) / D(len(values)), "above_3pct_n": sum(v > D('.03') for v in values)}

def analyze():
    routes = {row['yield_route_id']: row for row in read('routes')}
    observations = read('observations')
    cutoff = dt(read('cutoff')[0]['cutoff'])
    groups = defaultdict(list)
    for row in observations:
        groups[row['yield_route_id']].append(row)
    summary = []
    for key, rows in sorted(groups.items()):
        rows.sort(key=lambda row: (row['observation_time'],row['tier_no']))
        latest = rows[-1]
        s = dict(routes[key])
        s.update({"n": len(rows), "first": rows[0]['observation_time'], "last": latest['observation_time'], "age_hours": D(str((cutoff-dt(latest['observation_time'])).total_seconds()))/3600,
                  "days": len({row['observation_time'][:10] for row in rows}), "latest": latest,
                  "rate_kinds": sorted({row['rate_kind'] for row in rows}),
                  "availability_counts": {v:sum(row['availability']==v for row in rows) for v in sorted({row['availability'] for row in rows})},
                  "missing_payload_hash_n":sum(not row['source_payload_hash'] for row in rows)})
        for days in (7,30,90):
            win = [row for row in rows if dt(row['observation_time']) >= cutoff-timedelta(days=days)]
            # Daily medians prevent frequent collectors dominating a source's history.
            daily = defaultdict(list)
            for row in win:
                if row['rate'] is not None:
                    daily[row['observation_time'][:10]].append(D(row['rate']))
            medians = {day: sorted(values)[(len(values)-1)//2] for day,values in daily.items()}
            s[str(days)+'d'] = {"n": len(win), "days": len({row['observation_time'][:10] for row in win}), "daily_rate_stats": stats(list(medians.values())), "daily_medians": medians}
            ratios = [row for row in win if row['exposure_ratio'] is not None]
            if len(ratios)>1:
                first,last = ratios[0],ratios[-1]
                elapsed = D(str((dt(last['observation_time'])-dt(first['observation_time'])).total_seconds()))
                if elapsed:
                    gain = D(last['exposure_ratio']) / D(first['exposure_ratio']) - 1
                    s[str(days)+'d']['ratio_return'] = {"first": first['observation_time'], "last": last['observation_time'], "first_ratio":first['exposure_ratio'], "last_ratio":last['exposure_ratio'], "elapsed_seconds":elapsed, "gain":gain, "simple_apr":gain*D(365*86400)/elapsed}
        summary.append(s)
    dump(ROOT/'summary.json', {"cutoff_utc":str(cutoff), "observation_count":len(observations), "route_count":len(routes), "routes":summary})
    print('cutoff',cutoff,'rows',len(observations),'routes',len(routes))
    for s in summary:
        if s['provider'] == 'TRON':
            continue
        print(s['yield_route_id'], s['provider'], s['product_code'], s['n'], s['first'], s['last'], 'rate',s['latest']['rate'],s['latest']['rate_kind'],'availability',s['latest']['availability'], '30d',s['30d']['daily_rate_stats'], 'ratio',s['30d'].get('ratio_return'))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--fetch', action='store_true')
    args = parser.parse_args()
    if args.fetch:
        start = datetime.now(timezone.utc)
        manifest = [query('cutoff', "SELECT now64(3,'UTC') AS cutoff"),
                    query('routes', 'SELECT * FROM crypto_market_info.yield_route FINAL ORDER BY yield_route_id'),
                    query('observations', 'SELECT * FROM crypto_market_info.yield_observation FINAL ORDER BY yield_route_id,observation_time,tier_no'),
                    query('instruments', 'SELECT * FROM crypto_market_info.instrument FINAL ORDER BY instrument_id')]
        dump(ROOT/'manifest.json', {'started_at_utc':start,'finished_at_utc':datetime.now(timezone.utc),'files':manifest})
    analyze()
