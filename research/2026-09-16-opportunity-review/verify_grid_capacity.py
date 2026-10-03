#!/usr/bin/env python3
"""Small read-only public-market aggregation, avoiding wide committed views."""
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal as D
import subprocess, json, hashlib

ROOT=Path(__file__).resolve().parent
CLIENT='/home/ubuntu/.local/share/crypto-market-info-clickhouse-bin/clickhouse'
SQL="""
SELECT symbol, tupleElement(v,1) AS taker_side, tupleElement(v,4) AS rule_id,
       count() AS trade_count, toString(sum(tupleElement(v,2))) AS base_volume,
       toString(sum(tupleElement(v,2)*tupleElement(v,3))) AS quote_volume_in_ticks,
       min(tupleElement(v,5)) AS first_received_utc, max(tupleElement(v,5)) AS last_received_utc
FROM (
 SELECT symbol,trade_id,
        argMin(tuple(taker_side,quantity,price_tick,rule_id,receive_time),tuple(receive_time,batch_id)) AS v
 FROM crypto_grid_trading.trades_raw
 WHERE exchange='binance' AND symbol IN ('USDCUSDT','USD1USDT','FDUSDUSDT','UUSDT')
   AND receive_time >= '2026-09-15 00:00:00' AND receive_time < '2026-09-16 00:00:00'
   AND batch_id IN (SELECT batch_id FROM crypto_grid_trading.batch_commits WHERE committed_at >= '2026-09-15 00:00:00')
 GROUP BY symbol,trade_id
)
GROUP BY symbol,taker_side,rule_id ORDER BY symbol,taker_side,rule_id
"""
RULESQL="""
SELECT DISTINCT rule_id,symbol,toString(price_tick_size) AS price_tick_size
FROM crypto_grid_trading.rule_versions_raw
WHERE exchange='binance' AND symbol IN ('USDCUSDT','USD1USDT','FDUSDUSDT','UUSDT')
"""

def query(sql):
    command=[CLIENT,'client','--host','127.0.0.1','--port','9000','--readonly','1',
        '--max_threads','1','--max_block_size','8192','--max_memory_usage','800000000','--max_execution_time','40',
        '--query',sql,'--format','JSONEachRow']
    p=subprocess.run(command,text=True,capture_output=True)
    if p.returncode:
        raise RuntimeError(p.stderr)
    return [json.loads(line,parse_float=D) for line in p.stdout.splitlines()]

rules=query(RULESQL)
rows=[]
for symbol in ('USD1USDT','FDUSDUSDT','UUSDT','USDCUSDT'):
    rows.extend(query(SQL.replace("symbol IN ('USDCUSDT','USD1USDT','FDUSDUSDT','UUSDT')", "symbol = '"+symbol+"'")))
ticks={x['rule_id']:D(x['price_tick_size']) for x in rules}
agg={}
for row in rows:
    row['observed_quote_volume']=D(row['quote_volume_in_ticks'])*ticks[row['rule_id']]
    side=agg.setdefault(row['symbol'],{})
    side[row['taker_side']]=side.get(row['taker_side'],D(0))+row['observed_quote_volume']
thresholds=[]
for symbol,sides in agg.items():
    weak=min(sides.values())
    tick=next(D(r['price_tick_size']) for r in rules if r['symbol']==symbol)
    for capital in map(D,('100000','1000000')):
        for apr in map(D,('.03','.05')):
            required_one_side=capital*apr/D(365)/tick
            thresholds.append(dict(symbol=symbol,total_capital=capital,target_apr_pct=apr*100,
                assumed_paired_gap=tick,required_daily_matched_one_side_quote=required_one_side,
                observed_weak_side_quote_volume=weak,necessary_weak_side_share_pct=required_one_side/weak*100))
output=dict(generated_utc=datetime.now(timezone.utc).isoformat(),sql=SQL,rule_sql=RULESQL,rows=rows,
            rules=rules,thresholds=thresholds,
            caveats='Public raw events restricted to committed batches then deduplicated by Binance symbol/trade ID; 2026-09-15 UTC only. No private order/account tables. Observed market volume, not queue-fill replay; quality-gap volumes not invented. Near-par one-tick zero-fee capacity necessary condition only; all capital including idle cash counted. Larger gaps reduce required flow but are not guaranteed to fill.')
(ROOT/'grid-public-capacity.json').write_text(json.dumps(output,ensure_ascii=False,indent=2,default=str)+'\n')
print(json.dumps(dict(volumes=agg,thresholds=thresholds),indent=2,default=str))
