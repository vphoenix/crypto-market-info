import json, urllib.request, pathlib
ROOT=pathlib.Path(__file__).resolve().parent
QUERIES={
'now': "SELECT now64(6,'UTC') AS now_utc, timezone() AS server_timezone FORMAT JSONEachRow",
'schemas': "SELECT table,name,type FROM system.columns WHERE database='crypto_market_info' AND table IN ('instrument','derivative_contract_spec','derivative_trading_rule','derivative_book_minute','derivative_book_second_delta','derivative_book_quality_minute','options_live_minute_commit','options_index_minute') ORDER BY table,position FORMAT JSONEachRow",
'runs': 'SELECT * FROM options_live_run FINAL ORDER BY started_at DESC FORMAT JSONEachRow',
'commits_summary': 'SELECT run_id,count() AS minutes,min(minute_time) AS first_minute,max(minute_time) AS last_minute,sum(anchor_count) AS anchors FROM options_live_minute_commit FINAL GROUP BY run_id ORDER BY last_minute DESC FORMAT JSONEachRow',
'latest_commit': 'SELECT * FROM options_live_minute_commit FINAL ORDER BY minute_time DESC LIMIT 1 FORMAT JSONEachRow',
'instruments': "SELECT * FROM instrument FINAL WHERE exchange='Deribit' ORDER BY instrument_id FORMAT JSONEachRow",
'specs':'SELECT * FROM derivative_contract_spec FINAL ORDER BY instrument_id FORMAT JSONEachRow',
}
for name,query in QUERIES.items():
    (ROOT/(name+'.sql')).write_text(query+'\n')
    req=urllib.request.Request('http://127.0.0.1:8123/?database=crypto_market_info',data=query.encode())
    try:
        with urllib.request.urlopen(req,timeout=60) as resp: raw=resp.read()
        (ROOT/(name+'.jsonl')).write_bytes(raw)
        print(name,raw.decode()[:1500])
    except Exception as e: print(name,str(e))
