from pathlib import Path
from decimal import Decimal as D, getcontext
import collections, datetime, itertools, json, sys
getcontext().prec=60
ROOT=Path(__file__).resolve().parent
ZERO=D(0)
instruments={int(r['instrument_id']):r for r in [json.loads(x) for x in (ROOT/'mysql-instruments.jsonl').read_text().splitlines()]}
specs={int(r['instrument_id']):r for r in [json.loads(x) for x in (ROOT/'mysql-option-specs.jsonl').read_text().splitlines()]}
stored=json.loads(Path(sys.argv[1]).read_text(),parse_float=D)
def option_screen(rows, label):
    groups = collections.defaultdict(list)
    futures = {}
    for r in rows:
        i, s = instruments[r['id']], specs[r['id']]
        key = (i['base_asset'], i['settle_asset'], s['index_id'], i['expiry_time'], s['settlement_semantics_id'])
        if i['market_type'] == 'delivery':
            assert s['settlement_semantics_id'] in ['deribit-delivery-linear-v1', 'deribit-delivery-reversed-v1']
            futures[key[:4]] = r
        else:
            groups[key].append(r)
    counts = collections.Counter()
    positive = []
    financing = []
    box_lending = []
    def fee(r, price):
        i = instruments[r['id']]
        if i['settle_asset'] == i['base_asset']:
            return min(D('.0003'), price * D('.125'))
        return min(D('.0003') * r['index'], price * D('.125'))
    def record(kind, legs, gross):
        counts[kind] += 1
        charges = sum((abs(weight) * fee(r, r['ask'] if weight > 0 else r['bid']) for r, weight in legs), ZERO)
        net = gross - charges
        if net > 0:
            positive.append({'kind': kind, 'legs': [(instruments[r['id']]['exchange_symbol'], str(weight)) for r, weight in legs], 'gross_per_base_unit': gross, 'entry_fees_per_base_unit': charges, 'after_entry_fees_per_base_unit': net, 'currency': instruments[legs[0][0]['id']]['settle_asset'], 'status': 'candidate_only; expiry fees/margin and simultaneous executable depth not certified'})
    for key, members in groups.items():
        by_type = {}
        for typ in ['call', 'put']:
            curve = sorted([r for r in members if specs[r['id']]['option_type'] == typ], key=lambda r: D(specs[r['id']]['strike']))
            by_type[typ] = {D(specs[r['id']]['strike']): r for r in curve}
            for lower, higher in itertools.combinations(curve, 2):
                kl, kh = D(specs[lower['id']]['strike']), D(specs[higher['id']]['strike'])
                if typ == 'call':
                    record('call_monotonic_credit', [(lower, D(1)), (higher, D(-1))], higher['bid'] - lower['ask'])
                    # Inverse call vertical maximum in base is width/high strike.
                    bound = (kh - kl) / kh if key[1] == key[0] else kh - kl
                    record('call_vertical_overpay', [(lower, D(-1)), (higher, D(1))], lower['bid'] - higher['ask'] - bound)
                else:
                    record('put_monotonic_credit', [(higher, D(1)), (lower, D(-1))], lower['bid'] - higher['ask'])
                    if key[1] != key[0]:
                        record('linear_put_vertical_overpay', [(higher, D(-1)), (lower, D(1))], higher['bid'] - lower['ask'] - (kh - kl))
            # Adjacent strikes only; this is explicitly not exhaustive multi-leg arbitrage.
            for a, b, c in zip(curve, curve[1:], curve[2:]):
                ka, kb, kc = [D(specs[r['id']]['strike']) for r in [a, b, c]]
                wa, wc = (kc - kb) / (kc - ka), (kb - ka) / (kc - ka)
                record(typ + '_adjacent_butterfly_credit', [(a, wa), (b, D(-1)), (c, wc)], b['bid'] - wa * a['ask'] - wc * c['ask'])
        # Only linear put/call parity is screened here. Inverse requires native-currency financing accounting.
        if key[1] == key[0] or key[:4] not in futures:
            continue
        assert key[4] == 'deribit-option-linear-v1'
        future = futures[key[:4]]
        tf = future.get('taker_fee', D('.00035'))
        for strike in by_type['call'].keys() & by_type['put'].keys():
            c, p = by_type['call'][strike], by_type['put'][strike]
            for direction in ['conversion', 'reversal']:
                if direction == 'conversion':
                    premium = c['ask'] - p['bid']
                    payoff = future['bid'] - strike
                    entry_fee = fee(c, c['ask']) + fee(p, p['bid']) + future['bid'] * tf
                else:
                    premium = p['ask'] - c['bid']
                    payoff = strike - future['ask']
                    entry_fee = fee(p, p['ask']) + fee(c, c['bid']) + future['ask'] * tf
                counts['linear_' + direction] += 1
                if payoff - premium - entry_fee > 0:
                    financing.append({'kind': direction, 'call': instruments[c['id']]['exchange_symbol'], 'put': instruments[p['id']]['exchange_symbol'], 'future': instruments[future['id']]['exchange_symbol'], 'entry_premium_per_base': premium, 'fixed_payoff_per_base': payoff, 'entry_fee_per_base': entry_fee, 'surplus_before_financing_and_delivery_per_base': payoff - premium - entry_fee, 'currency': key[1], 'status': 'financing_candidate; capital/margin and expiry costs missing'})
        paired_strikes = sorted(by_type['call'].keys() & by_type['put'].keys())
        expiry = datetime.datetime.fromisoformat(key[3]).replace(tzinfo=datetime.timezone.utc)
        sample_time = datetime.datetime.fromisoformat(stored['sample_at_utc'].replace('Z', '+00:00'))
        seconds = D(str((expiry - sample_time).total_seconds()))
        if seconds <= 0:
            continue
        for kl, kh in itertools.combinations(paired_strikes, 2):
            cl, ch = by_type['call'][kl], by_type['call'][kh]
            pl, ph = by_type['put'][kl], by_type['put'][kh]
            counts['linear_box_pair'] += 1
            width = kh - kl
            long_debit = cl['ask'] - ch['bid'] + ph['ask'] - pl['bid']
            long_fee = fee(cl, cl['ask']) + fee(ch, ch['bid']) + fee(ph, ph['ask']) + fee(pl, pl['bid'])
            invested = long_debit + long_fee
            if invested > 0 and width > invested:
                box_lending.append({'call_low': instruments[cl['id']]['exchange_symbol'], 'call_high': instruments[ch['id']]['exchange_symbol'], 'put_low': instruments[pl['id']]['exchange_symbol'], 'put_high': instruments[ph['id']]['exchange_symbol'], 'expiry_utc': expiry.isoformat(), 'entry_debit_and_fee_per_base': invested, 'fixed_payoff_per_base': width, 'surplus_before_expiry_and_capital_cost_per_base': width - invested, 'indicative_simple_annual_rate': (width - invested) / invested * D(365 * 86400) / seconds, 'currency': key[1], 'status': 'loan_like_payoff; executable lot ratios, depth, margin and expiry costs unverified'})
            short_credit = cl['bid'] - ch['ask'] + ph['bid'] - pl['ask']
            short_fee = fee(cl, cl['bid']) + fee(ch, ch['ask']) + fee(ph, ph['bid']) + fee(pl, pl['ask'])
            if short_credit - short_fee > width:
                positive.append({'kind': 'linear_short_box_excess_credit', 'gross_per_base_unit': short_credit - width, 'entry_fees_per_base_unit': short_fee, 'after_entry_fees_per_base_unit': short_credit - short_fee - width, 'currency': key[1], 'legs': [instruments[r['id']]['exchange_symbol'] for r in [cl, ch, ph, pl]], 'status': 'candidate_only; depth/margin and expiry costs not certified'})
    box_lending.sort(key=lambda r: r['indicative_simple_annual_rate'], reverse=True)
    return {'source': label, 'option_count': sum(len(x) for x in groups.values()), 'future_count': len(futures), 'expiry_identity_groups': len(groups), 'comparisons': dict(counts), 'static_credit_candidates': positive, 'linear_financing_candidates': financing, 'linear_box_lending_positive_observations': len(box_lending), 'linear_box_lending_top': box_lending[:10], 'limitations': ['Only positive two-sided quotes; no mark price fills.', 'Adjacent butterflies only; inverse parity, arbitrary portfolios and combo books not certified.', 'Positive values are a screen, not a net profit or executable transaction certificate.']}

rows=[]
reasons=collections.Counter()
for r in stored['derivative_books']:
    if 'error' in r:continue
    i=instruments[int(r['instrument_id'])];result=r['result'];q=result['Quality'];b=result['Snapshot']
    reasons[str(q['Reason'])]+=1
    if not q['ReplayValid'] or not q['MarketOpen'] or not b['Bids'] or not b['Asks']:continue
    if i['base_asset'] not in ['BTC','ETH'] or i['market_type'] not in ['option','delivery']:continue
    bid=D(b['Bids'][0]['PriceTick'])*D(i['price_tick_size']);ask=D(b['Asks'][0]['PriceTick'])*D(i['price_tick_size'])
    if bid<=0 or ask<=bid:continue
    index=stored['indexes'].get(specs[int(r['instrument_id'])]['index_id'])
    if not index or D(index['Price'])<=0:continue
    rows.append({'id':int(r['instrument_id']),'bid':bid,'ask':ask,'index':D(index['Price'])})
result=option_screen(rows,'committed same-second native replay, 10-level options and 5-level futures')
result['sample_at_utc']=stored['sample_at_utc']
result['expected_books']=stored['expected_books'];result['valid_books']=stored['valid_books'];result['missing_books']=stored['missing_books'];result['quality_reasons']=dict(reasons)
Path(sys.argv[2]).write_text(json.dumps(result,default=str,indent=2)+'\n')
print(json.dumps({k:v for k,v in result.items() if k not in ['static_credit_candidates','linear_financing_candidates','linear_box_lending_top']},default=str))
print('credit candidates',len(result['static_credit_candidates']),'financing candidates',len(result['linear_financing_candidates']),'box top',result['linear_box_lending_top'][:2])
