#!/usr/bin/env python3
"""Read-only minute-boundary executable Deribit opportunity screen, Decimal throughout."""
import urllib.request, json, pathlib, datetime, decimal, itertools, collections, csv, gzip
from decimal import Decimal as D
from fractions import Fraction
from math import lcm, gcd
P=pathlib.Path(__file__).parent
DB='crypto_market_info'

def query(sql):
    req=urllib.request.Request('http://127.0.0.1:8123/',data=(sql+' FORMAT JSONEachRow').encode())
    with urllib.request.urlopen(req,timeout=120) as r: raw=r.read()
    return [json.loads(x,parse_float=D) for x in raw.splitlines()]
def save(name, rows):
    payload=json.dumps(rows,default=str,ensure_ascii=False,indent=2)
    if name.startswith('raw_'):
        (P/(name+'.json.gz')).write_bytes(gzip.compress(payload.encode()))
    else:(P/(name+'.json')).write_text(payload)
def fetch():
    cutoff=query(f'SELECT max(minute_time) AS cutoff FROM {DB}.options_live_minute_commit FINAL')[0]['cutoff']
    tables={
      'instruments':f'SELECT * FROM {DB}.instrument FINAL WHERE instrument_id IN (SELECT instrument_id FROM {DB}.derivative_contract_spec)',
      'specs':f'SELECT * FROM {DB}.derivative_contract_spec FINAL',
      'runs':f'SELECT * FROM {DB}.options_live_run FINAL',
      'commits':f"SELECT run_id,minute_time,batch_id,run_hash,prepared_at,instrument_ids,member_hashes,anchor_count,delta_count,index_ids,index_hashes FROM {DB}.options_live_minute_commit FINAL WHERE minute_time <= '{cutoff}'",
      'rules':f'SELECT * FROM {DB}.derivative_trading_rule FINAL',
      'books':f"SELECT * FROM {DB}.derivative_book_minute FINAL WHERE minute_time <= '{cutoff}'",
      'quality':f"SELECT instrument_id,minute_time,batch_id,row_hash,replay_valid_bitmap,stream_valid_bitmap,market_known_bitmap,market_open_bitmap,source_times[1] AS source_time,received_times[1] AS received_time,trading_rule_ids[1] AS trading_rule_id,reasons[1] AS reason,bid_level_counts[1] AS bid_levels,ask_level_counts[1] AS ask_levels FROM {DB}.derivative_book_quality_minute FINAL WHERE minute_time <= '{cutoff}'",
      'indices':f"SELECT index_id,minute_time,batch_id,prices[1] AS price,source_times[1] AS source_time,states[1] AS state FROM {DB}.options_index_minute FINAL WHERE minute_time <= '{cutoff}'",
    }
    result={}
    for name,sql in tables.items():
        try: result[name]=query(sql)
        except Exception as e:
            if name=='indices':
                result[name]=query(sql.replace('states[1]','state[1]'))
            else: raise
        save('raw_'+name,result[name]);print(name,len(result[name]),flush=True)
    save('capture',{'cutoff':cutoff,'captured_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source':'local ClickHouse public market history; committed realtime batches only'})
    return result
if __name__=='__main__':
    import sys
    if '--fetch' in sys.argv: fetch()

def analyze():
    data={n:json.load(gzip.open(P/('raw_'+n+'.json.gz'),'rt'),parse_float=D) for n in ['instruments','specs','runs','commits','rules','books','quality','indices']}
    ins={x['instrument_id']:x for x in data['instruments']}; specs={x['instrument_id']:x for x in data['specs']}
    rules={x['trading_rule_id']:x for x in data['rules']}
    dt=lambda s:datetime.datetime.fromisoformat(s).replace(tzinfo=datetime.timezone.utc)
    commits={c['batch_id']:c for c in data['commits']}
    quals={(q['batch_id'],q['instrument_id']):q for q in data['quality']}
    idx={(x['batch_id'],x['index_id']):x for x in data['indices']}
    groups=collections.defaultdict(dict); reasons=collections.Counter(); ages=[]
    for b in data['books']:
        iid=b['instrument_id']; bid=b['batch_id']; c=commits.get(bid); q=quals.get((bid,iid))
        if not c or not q or iid not in c['instrument_ids']: reasons['uncommitted_or_missing_quality']+=1; continue
        if q['row_hash']!=c['member_hashes'][c['instrument_ids'].index(iid)]: raise ValueError('Member content hash does not match commit')
        if not all(int(q[x])&1 for x in ['replay_valid_bitmap','stream_valid_bitmap','market_known_bitmap','market_open_bitmap']) or not int(b['valid_bitmap'])&1 or q['reason']!=0:
            reasons['invalid_quality_at_minute_boundary']+=1;continue
        if not b['bid_prices'] or not b['ask_prices']:
            reasons['one_or_both_book_sides_empty']+=1;continue
        if q['trading_rule_id'] not in rules:raise ValueError('Rule missing')
        r=rules[q['trading_rule_id']]; age=D(str((dt(b['minute_time'])-dt(q['source_time'])).total_seconds()))
        if age<0:raise ValueError('Future source time')
        ages.append(age)
        inst=ins[iid];s=specs[iid];ix=idx[(bid,s['index_id'])]
        groups[bid][iid]={'bid':D(b['bid_prices'][0])*D(inst['price_tick_size']),'ask':D(b['ask_prices'][0])*D(inst['price_tick_size']),
                         'bid_qty':D(b['bid_qtys'][0])*D(inst['quantity_step_size']),'ask_qty':D(b['ask_qtys'][0])*D(inst['quantity_step_size']),
                         'age':age,'rule':r,'index':None if ix['price'] is None else D(ix['price']),
                         'index_age':None if ix['source_time'] is None else D(str((dt(b['minute_time'])-dt(ix['source_time'])).total_seconds()))}
    families={}
    for iid,s in specs.items():
        family=s['index_id'];f=families.setdefault(family,{'options':collections.defaultdict(dict)})
        if s['option_type'] is None:f['future']=iid
        else:f['options'][D(s['strike'])][s['option_type']]=iid
    observations=[]
    fee_opt=lambda price,index,linear:min(D('.0003')*(index if linear else D(1)),D('.125')*price)
    def executable_quantity(legs):
        # Each (book, side, native units per base) contributes its own minimum and step.
        steps=[Fraction(D(b['rule']['amount_step']))/Fraction(units) for b,_,units in legs]
        common=Fraction(lcm(*(s.numerator for s in steps)),gcd(*(s.denominator for s in steps)))
        maximum=min(Fraction(b[side+'_qty'])/Fraction(units) for b,side,units in legs)
        units=(maximum//common)*common
        if any(units*Fraction(per_base)<Fraction(D(b['rule']['min_trade_amount'])) for b,_,per_base in legs): return D(0)
        return D(units.numerator)/D(units.denominator)
    def row(c,family,strategy,strike,profit,fee,capital,legs,qty,ages_,detail):
        t=dt(c['minute_time']);expires=dt(ins[families[family]['future']]['expiry_time'])
        secs=D(int((expires-t).total_seconds()));net=profit-fee
        maxage=max(ages_)
        observations.append({'minute':c['minute_time'],'family':family,'strategy':strategy,'strike':str(strike),'gross_profit':profit,'opening_fees':fee,'net_before_delivery':net,
           'capital_denominator':capital,'annualized_before_delivery':net/capital*D(365*86400)/secs if capital>0 else None,
           'max_source_age_s':maxage,'bbo_capacity_base':qty,'legs':','.join(ins[x]['exchange_symbol'] for x in legs),'detail':detail})
    for bid,c in commits.items():
      books=groups.get(bid,{})
      for family, fam in families.items():
        linear=family.endswith('usdc'); fid=fam['future'];f=books.get(fid)
        if not f:continue
        index=f['index']
        if index is None or f['index_age']>60:continue
        for strike, cp in fam['options'].items():
          cid,pid=cp['call'],cp['put'];call,put=books.get(cid),books.get(pid)
          if not call or not put:continue
          for direction in ['long_synthetic_short_future','short_synthetic_long_future']:
            forward=direction=='long_synthetic_short_future'
            cv,pv,fv=(call['ask'],put['bid'],f['bid']) if forward else (call['bid'],put['ask'],f['ask'])
            optfees=fee_opt(cv,index,linear)+fee_opt(pv,index,linear)
            if linear:
                gross=pv-cv+fv-strike if forward else cv-pv-fv+strike
                futurefee=fv*D('.00035');capital=index;future_units=D(1)
            else:
                gross=D(1)-strike/fv-cv+pv if forward else strike/fv-D(1)+cv-pv
                futurefee=strike/fv*D('.00035');capital=D(1);future_units=strike
            capacity=executable_quantity([(call,'ask' if forward else 'bid',D(1)),(put,'bid' if forward else 'ask',D(1)),(f,'bid' if forward else 'ask',future_units)])
            if capacity<=0:continue
            row(c,family,direction,strike,gross,optfees+futurefee,capital,[cid,pid,fid],capacity,[call['age'],put['age'],f['age']],{'call':cv,'put':pv,'future':fv,'index':index,'denominator':'1 base unit equivalent, not verified account margin'})
        for k1,k2 in itertools.combinations(sorted(fam['options']),2):
          ids=[fam['options'][k1]['call'],fam['options'][k1]['put'],fam['options'][k2]['call'],fam['options'][k2]['put']]
          if not all(i in books for i in ids):continue
          cs,ps,ch,ph=[books[i] for i in ids];w=k2-k1
          debit=cs['ask']-ps['bid']-ch['bid']+ph['ask']
          fees=sum(fee_opt(v,index,linear) for v in [cs['ask'],ps['bid'],ch['bid'],ph['ask']])
          boxlegs=[(cs,'ask',D(1)),(ps,'bid',D(1)),(ch,'bid',D(1)),(ph,'ask',D(1))]
          qty=executable_quantity(boxlegs+([] if linear else [(f,'ask',w)])); ag=[books[i]['age'] for i in ids]
          if linear:
            payout=w;profit=payout-debit;capital=debit+fees; strategy='long_box'
          else:
            payout=w/f['ask'];fees+=w/f['ask']*D('.00035');profit=payout-debit;capital=debit+fees;strategy='long_box_hedged_inverse_future'
            ag.append(f['age'])
          if qty>0:
            row(c,family,strategy,f'{k1}-{k2}',profit,fees,capital,ids+([] if linear else [fid]),qty,ag,{'debit':debit,'payout':payout,'index':index,'denominator':'premium plus opening fees; additional account margin omitted; this is not an account return'})
          credit=cs['bid']-ps['ask']-ch['ask']+ph['bid']
          fees=sum(fee_opt(v,index,linear) for v in [cs['bid'],ps['ask'],ch['ask'],ph['bid']])
          boxlegs=[(cs,'bid',D(1)),(ps,'ask',D(1)),(ch,'ask',D(1)),(ph,'bid',D(1))]
          qty=executable_quantity(boxlegs+([] if linear else [(f,'bid',w)]))
          if qty<=0:continue
          liability=w if linear else w/f['bid']
          if not linear:fees+=liability*D('.00035')
          row(c,family,'short_box' if linear else 'short_box_hedged_inverse_future',f'{k1}-{k2}',credit-liability,fees,liability,ids+([] if linear else [fid]),qty,ag,{'credit':credit,'liability':liability,'index':index,'denominator':'fully reserved terminal liability; excludes account margin and any external reinvestment'})
    def summary(rows):
      valid=[r for r in rows if r['annualized_before_delivery'] is not None]
      best=max(valid,key=lambda r:r['annualized_before_delivery']) if valid else None
      over=[r for r in valid if r['annualized_before_delivery']>=D('.03')]
      bestnet=max(valid,key=lambda r:r['net_before_delivery']) if valid else None
      return {'samples':len(valid),'positive_gross':sum(r['gross_profit']>0 for r in valid),'positive_after_opening_fees':sum(r['net_before_delivery']>0 for r in valid),'at_least_3pct':len(over),'best_annualized':best,'best_net':bestnet}
    result={'capture':json.load(open(P/'capture.json')),'minutes_committed':len(commits),'raw_books':len(data['books']),'eligible_two_sided_books':sum(map(len,groups.values())),
            'exclusions':dict(reasons),'scope':'minute boundaries only; held quotes retained with exact source age; no delta replay; public BBO and minimum order quantity; no mark/mid',
            'fees':'Standard taker 0.00035 future; min(0.0003*index, 0.125*premium) linear option or min(0.0003, 0.125*premium) coin option. Delivery/financing/account margin not deducted: all net results are optimistic.',
            'by_freshness':{}}
    for freshness in [10,60,300,3600,None]:
      rows=[r for r in observations if freshness is None or r['max_source_age_s']<=freshness]
      by={}
      for key in sorted(set((r['family'],r['strategy']) for r in rows)):
        by['/'.join(key)]=summary([r for r in rows if (r['family'],r['strategy'])==key])
      result['by_freshness'][str(freshness)]=by
    save('summary',result);save('positive_candidates',[r for r in observations if r['annualized_before_delivery'] and r['annualized_before_delivery']>=D('.03')])
    with gzip.open(P/'observations.csv.gz','wt') as ff:
      writer=csv.DictWriter(ff,fieldnames=list(observations[0]));writer.writeheader();writer.writerows(observations)
    print(json.dumps({'minutes':len(commits),'observations':len(observations),'fresh60':result['by_freshness']['60']},default=str,indent=2))
if __name__=='__main__':
    analyze()
