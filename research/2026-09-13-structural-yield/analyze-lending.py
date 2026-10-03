"""Decimal-only lending rate screening; FRR is a benchmark, not account P&L."""
import datetime, json
from decimal import Decimal as D, getcontext, ROUND_CEILING
from pathlib import Path
getcontext().prec=40
P=Path(__file__).resolve().parent
def read(name):return json.loads((P/(name+'.json')).read_text(),parse_float=str)
def quantile(values,p):
    v=sorted(values)
    return v[min(len(v)-1,int(D(len(v)-1)*D(p)))]
out={}
for asset in ('fUSD','fUST'):
    raw=read('bitfinex-'+asset+'-90d')['data']
    assert len(raw)==2161
    assert len({r[0] for r in raw})==len(raw)
    gaps=[b[0]-a[0] for a,b in zip(raw,raw[1:])]
    assert all(g==3600000 for g in gaps)
    end=raw[-1][0]
    stats={}
    for days in (7,30,90):
        rows=[r for r in raw if end-days*86400000<=r[0]<end]
        assert len(rows)==days*24
        # The funding stats endpoint encodes FRR as daily lending rate / 365.
        gross=[D(str(r[3]))*D(365)*D(365) for r in rows]
        net=[r*D('0.85') for r in gross]
        stats[str(days)]={'hours':len(rows),'mean_frr_gross_apr':sum(gross)/len(gross),
            'mean_after_15pct_interest_fee_apr':sum(net)/len(net),'min_net_apr':min(net),
            'p10_net_apr':quantile(net,'0.1'),'median_net_apr':quantile(net,'0.5'),'max_net_apr':max(net),
            'fraction_hours_net_above_3_5pct':D(sum(x>D('0.035') for x in net))/len(net),
            'hypothetical_period_simple_return_at_frr':sum(net)/len(net)*D(days)/365,
            'note':'FRR benchmark, not an achievable fill or realised lender return; no idle-capital, transfer, tax or principal losses.'}
    ticker=read('bitfinex-'+asset+'-ticker')['data']
    bids=sorted([r for r in read('bitfinex-'+asset+'-book')['data'] if D(str(r[3]))<0 and r[2]>0],key=lambda r:D(str(r[0])),reverse=True)
    capacity=[]
    for amount in (D(10000),D(100000)):
        left=amount;daily_interest=D(0);used=[]
        for r in bids:
            take=min(left,-D(str(r[3])))
            if take<=0:continue
            daily_interest+=take*D(str(r[0]));left-=take
            used.append({'amount':take,'max_days':r[1],'daily_rate':r[0]})
            if left==0:break
        capacity.append({'amount':amount,'unfilled':left,'matched_daily_gross_interest':daily_interest,
            'annualized_after_interest_fee_if_fully_deployed':daily_interest*365*D('0.85')/amount,'used_borrow_bids':used,
            'note':'Snapshot executable-side estimate; borrower may repay early, no locked yield for full displayed duration.'})
    out[asset]={'first_utc':datetime.datetime.fromtimestamp(raw[0][0]//1000,datetime.timezone.utc).isoformat(),
        'last_utc':datetime.datetime.fromtimestamp(end//1000,datetime.timezone.utc).isoformat(),'history':stats,
        'current_ticker_frr_gross_apr':D(str(ticker[0]))*365,
        'current_ticker_frr_net_apr':D(str(ticker[0]))*365*D('0.85'),
        'visible_borrow_bid_total':sum((-D(str(r[3])) for r in bids),D(0)), 'capacity':capacity}
(P/'lending-analysis.json').write_text(json.dumps(out,indent=2,default=str)+'\n')
print(json.dumps(out,default=str))
