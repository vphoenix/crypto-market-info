"""Export complete saved depth and deltas around screened minute anchors."""
import json
from datetime import datetime, timedelta
from pathlib import Path
from export import query

ROOT = Path(__file__).resolve().parent
spikes = json.loads((ROOT / 'spikes.json').read_text(), parse_float=str)
stamps = set()
for row in spikes:
    stamp = datetime.fromisoformat(row['time'])
    for offset in range(-2, 3):
        stamps.add(str(stamp + timedelta(minutes=offset)))
assert len(stamps) <= 500
if stamps:
    where = 'instrument_id IN (1,3) AND minute_time IN (' + ','.join(
        "toDateTime('" + stamp + "','UTC')" for stamp in sorted(stamps)) + ')'
    query('spike-minutes', 'SELECT * FROM crypto_market_info.order_book_minute FINAL WHERE '
          + where + ' ORDER BY minute_time,instrument_id')
    query('spike-deltas', 'SELECT * FROM crypto_market_info.order_book_second_delta FINAL '
          'WHERE minute_id IN (SELECT id FROM crypto_market_info.order_book_minute FINAL WHERE '
          + where + ') ORDER BY minute_id,second_offset')
