"""Verify complete settled funding windows and calculate exact historical rates."""
import json
from datetime import datetime, timezone
from decimal import Decimal as D
from pathlib import Path

P = Path(__file__).resolve().parent
PERIOD_MS = 8 * 3_600_000
END_MS = 1_789_228_800_000  # 2026-09-12 16:00 UTC
assert datetime.fromtimestamp(END_MS // 1000, timezone.utc).isoformat() == '2026-09-12T16:00:00+00:00'
out = {}
for symbol in ('BTC', 'SOL', 'AVAX', 'TRX'):
    rows = json.loads((P / ('public-funding-' + symbol + 'USDT.json')).read_text(), parse_float=str)
    rows.sort(key=lambda r: r['fundingTime'])
    offsets = [r['fundingTime'] % PERIOD_MS for r in rows]
    assert max(offsets) < 1000
    assert len({r['fundingTime'] // PERIOD_MS for r in rows}) == len(rows)
    stats = {}
    for days in (7, 30):
        start = END_MS - days * 86_400_000
        selected = [r for r in rows if start < (r['fundingTime'] // PERIOD_MS) * PERIOD_MS <= END_MS]
        assert len(selected) == days * 3
        rates = [D(r['fundingRate']) for r in selected]
        stats[str(days)] = {
            'n': len(selected), 'sum': sum(rates),
            'annualized_nominal': sum(rates) * 365 / D(days),
            'negative_count': sum(x < 0 for x in rates),
            'min': min(rates), 'max': max(rates),
            'observed_max_settlement_ms_offset': max(offsets),
        }
    out[symbol] = stats
(P / 'public-funding-analysis.json').write_text(json.dumps(out, indent=2, default=str) + '\n')
print('Validated 21/90 complete funding slots for BTC, SOL, AVAX, TRX.')
