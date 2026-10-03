"""Reproducible Decimal-only public rate comparisons and labelled scenarios."""
import datetime, hashlib, json
from decimal import Decimal as D, getcontext
from pathlib import Path
getcontext().prec=40
P=Path(__file__).resolve().parent
out={'okx':{},'illustrative_scenarios':{}}
for asset in ('usdt','usdc'):
    f=P/('okx-'+asset+'-history.json');o=json.loads(f.read_text(),parse_float=str)
    assert o['http_status']==200 and o['data']['code']=='0'
    assert hashlib.sha256((P/o['raw_file']).read_bytes()).hexdigest()==o['sha256']
    rows=sorted(o['data']['data'],key=lambda r:int(r['ts']))
    assert len(rows)==100
    assert all(int(b['ts'])-int(a['ts'])==3600000 for a,b in zip(rows,rows[1:]))
    last=rows[-1];r24=rows[-24:]
    out['okx'][asset]={
        'source_url':o['url'],'retrieved_at_utc':o['retrieved_at_utc'],
        'latest_observation_utc':datetime.datetime.fromtimestamp(int(last['ts'])//1000,datetime.timezone.utc).isoformat(),
        'latest_borrow_apr':D(last['rate']),'latest_lending_apr_reported':D(last['lendingRate']),
        'last24_observation_mean_borrow_apr':sum(D(r['rate']) for r in r24)/24,
        'last24_observation_mean_lending_apr':sum(D(r['lendingRate']) for r in r24)/24,
        'note':'Official public rate fields; not personal account return, executable borrow allowance or approved collateral LTV. No additional 15% deducted from the published lendingRate.'}
out['illustrative_scenarios']['borrow_short_term']={
    'borrowed':'10000','gross_apr':'0.10','days':'7',
    'interest_cost':D('10000')*D('.10')*7/365,
    'note':'Cost illustration only; excludes fees and loan eligibility.'}
out['illustrative_scenarios']['collateral_gap']={
    'initial_collateral_value':'1000','loan_principal':'900','sale_value_after_15pct_drop':'850',
    'shortfall':D('900')-D('850'),'loss_fraction_of_loan':(D('900')-D('850'))/D('900'),
    'note':'Hypothetical failed/late liquidation, ignores interest/fees; not actual default probability or normal liquidation trigger.'}
out['illustrative_scenarios']['borrow_low_lend_high']={
    'own_collateral_capital':'100000','loan_ratio_assumed':'0.70','loan':'70000',
    'borrow_apr_assumed':'0.035','lend_apr_after_platform_fee_assumed':'0.082875',
    'annual_spread_income':D('70000')*(D('.082875')-D('.035')),
    'own_capital_return':D('.7')*(D('.082875')-D('.035')),
    'note':'Not an available product quote. Collateral yield assumed zero solely for arithmetic; all fees, changing rates, liquidation and platform losses excluded.'}
out['illustrative_scenarios']['loss_vs_interest']={
    'one_off_loss_fraction':'0.20','annual_interest_assumed':'0.082875',
    'years_of_simple_interest_erased':D('.20')/D('.082875'),
    'note':'Stress example, not an estimate of event likelihood or expected loss.'}
(P/'comparator-analysis.json').write_text(json.dumps(out,indent=2,default=str)+'\n')
print(json.dumps(out,default=str))
