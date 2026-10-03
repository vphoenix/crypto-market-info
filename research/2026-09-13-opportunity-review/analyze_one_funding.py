"""Read-only ONE funding comparison using the archived official API responses."""
from datetime import datetime, timezone
from decimal import Decimal as D, getcontext
from hashlib import sha256
from pathlib import Path
import json

getcontext().prec = 40
P = Path(__file__).parent
names = ['public-funding-binance-ONEUSDT.json','public-funding-okx-ONE-USDT-SWAP.json',
         'market-anomaly-followup.json']
inputs = {n:json.loads((P/n).read_text()) for n in names}
b = inputs[names[0]]['data']
o = inputs[names[1]]['data']['data']
end = int(datetime(2026,9,12,16,tzinfo=timezone.utc).timestamp())*1000
records = []
for days in (1,3,7):
    row = {'days':days,'start_exclusive_utc':datetime.fromtimestamp((end-days*86400000)//1000,timezone.utc).isoformat(),
           'end_inclusive_utc':'2026-09-12T16:00:00+00:00'}
    sums = {}
    for label,data,field,interval in [('binance',b,'fundingRate',8),('okx',o,'realizedRate',4)]:
        selected = sorted([r for r in data if end-days*86400000<int(r['fundingTime'])//1000*1000<=end],
                          key=lambda r:int(r['fundingTime']))
        times = [int(r['fundingTime'])//1000*1000 for r in selected]
        assert len(selected) == days*24//interval
        assert len(set(times)) == len(times)
        assert all(y-x==interval*3600000 for x,y in zip(times,times[1:]))
        sums[label] = sum((D(r[field]) for r in selected),D(0))
        row[label] = {'rows':len(selected),'sum_pct':str(sums[label]*100),'interval_hours':interval,
                      'min_pct':str(min(D(r[field]) for r in selected)*100),
                      'max_pct':str(max(D(r[field]) for r in selected)*100)}
    net = sums['binance']-sums['okx']
    row['long_okx_short_binance_net_pct_each_leg_notional'] = str(net*100)
    row['constant_5000_notional_each_leg_funding_usdt'] = str(net*5000)
    records.append(row)
current = {}
for x in inputs[names[2]]:
    if '/funding-rate?' in x['url']:
        current['okx'] = x
    elif '/premiumIndex?symbol=ONEUSDT' in x['url']:
        current['binance'] = x
f_b = D(current['binance']['body']['lastFundingRate'])
f_o = D(current['okx']['body']['data'][0]['fundingRate'])
net_daily = f_b*3-f_o*6
summary = {'method':'Fixed 5,000 USDT notional per venue is a funding sensitivity, not a realized constant-coin hedge backtest.',
           'timestamp_normalization':'Binance fundingTime milliseconds floored to seconds; UTC aligned windows are start-exclusive/end-inclusive.',
           'input_file_sha256':{n:sha256((P/n).read_bytes()).hexdigest() for n in names},
           'realized_windows':records,'current_api_observations':current,
           'current_if_rates_unchanged_net_daily_pct_leg_notional':str(net_daily*100),
           'current_if_rates_unchanged_net_daily_usdt_5000_each_leg':str(net_daily*5000)}
(P/'one-funding-analysis.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps({'realized_windows':records,'current_net_daily_5000_each':str(net_daily*5000)},indent=2))
