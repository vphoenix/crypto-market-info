"""Export full Deribit option/futures second-0 books for one-hour windows, PK-range per instrument.

Reads derivative_book_minute by (instrument_id, minute_time) ranges in chunks of 200 instruments,
without the index table (about 70 random-lookup rows per minute;
every check uses same-expiry futures instead). Pauses whenever the production writer has backlog.
"""
import datetime as dt
import gzip
import json
import sys
import time
from pathlib import Path

import dbq

OUT = Path(__file__).resolve().parent
WINDOWS = sys.argv[1:]  # 'YYYY-MM-DD HH'
inst = {r['instrument_id']: r for r in map(json.loads, gzip.open(OUT / 'meta-instruments.jsonl.gz'))}
spec = {r['instrument_id']: r for r in map(json.loads, gzip.open(OUT / 'meta-specs.jsonl.gz'))}


def active(start, end):
    ids = []
    for i, r in inst.items():
        s = spec.get(i)
        if r['market_type'] not in ('option', 'delivery') or not r['expiry_time'] or s is None:
            continue
        if r['expiry_time'][:19] > start and (s['creation_time'] or '')[:19] <= end:
            ids.append(i)
    return sorted(ids)


for w in WINDOWS:
    start = w + ':00:00'
    end = w + ':59:00'
    d = OUT / 'windows' / w.replace('-', '').replace(' ', 'T')
    d.mkdir(parents=True, exist_ok=True)
    ids = active(start, end)
    t0 = time.monotonic()
    for k in range(0, len(ids), 200):
        name = d / f'books-{k // 200:02d}.jsonl.gz'
        if name.with_suffix('.meta.json').exists():
            continue
        chunk = ids[k:k + 200]
        sql = ("SELECT JSON_OBJECT('id',instrument_id,'m',minute_time,'v',valid_bitmap & 1,'d',stored_depth,'bp',HEX(bid_prices),"
               "'bq',HEX(bid_qtys),'ap',HEX(ask_prices),'aq',HEX(ask_qtys)) FROM crypto_market_info.derivative_book_minute "
               f"WHERE instrument_id IN ({','.join(map(str, chunk))}) AND minute_time BETWEEN '{start}' AND '{end}'")
        waited = dbq.guard()
        for attempt in range(3):
            try:
                raw, el = dbq.run(sql, seconds=90)
                break
            except RuntimeError as e:
                print(w, k, 'retry', attempt, str(e)[:120], flush=True)
                time.sleep(30)
        else:
            raise SystemExit(f'{w} chunk {k} failed')
        m = dbq.save(name, sql, raw, el)
        print(w, f'chunk {k // 200}', m['rows'], 'rows', m['elapsed_s'], 's', 'waited', waited, 'backlog', dbq.writer_backlog()[:2], flush=True)
        time.sleep(3)
    (d / 'instruments.json').write_text(json.dumps(ids) + '\n')
    print(w, 'done', len(ids), 'instruments', round(time.monotonic() - t0), 's', flush=True)
