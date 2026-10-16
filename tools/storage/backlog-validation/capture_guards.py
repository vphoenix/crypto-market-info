"""Require ten observed minutes of full source scope before arming a fixture."""
import datetime as dt
from metrics_guards import metrics_current

UTC=dt.timezone.utc

def stamp(value):
    result=dt.datetime.fromisoformat(value)
    if result.tzinfo is None:raise ValueError('timezone required')
    return result.timestamp()

def stable_full_scope(history,plans,now,seconds=600):
    end=now.timestamp();start=end-seconds
    rows=sorted((r for r in history if start-90<=stamp(r['utc'])<=end),key=lambda r:stamp(r['utc']))
    prior=[n for n,r in enumerate(rows)if stamp(r['utc'])<=start]
    if not prior:return False
    rows=rows[prior[-1]:]
    if len(rows)<10 or end-stamp(rows[-1]['utc'])>90:return False
    if any(stamp(b['utc'])-stamp(a['utc'])>90 for a,b in zip(rows,rows[1:])):return False
    writers={r.get('services',{}).get('storage-writer',{}).get('MainPID')for r in rows}
    collectors={r.get('services',{}).get('collector',{}).get('MainPID')for r in rows}
    if len(writers)!=1 or None in writers or len(collectors)!=1 or None in collectors:return False
    ordered=sorted(plans,key=lambda p:stamp(p['effective']))
    for r in rows:
        at=stamp(r['utc']);w=r.get('writer')or{}
        if not metrics_current(r) or not r.get('writer_metrics_fresh')or not w.get('spool_index_complete')or not w.get('redis_index_complete')or w.get('blocked')!=0:return False
        if not -5<=at-stamp(w['time'])<=45:return False
        active=[p for p in ordered if stamp(p['effective'])<=at]
        if not active or active[-1]['books']<2806:return False
        options=[h for h in r.get('source_health',[])if h.get('msg')=='options catalog runtime health']
        paired=[h for h in r.get('source_health',[])if h.get('msg')=='OKX paired runtime health']
        if not options or not paired:return False
        o,p=options[-1],paired[-1]
        if not -5<=at-stamp(o['time'])<=150 or not -5<=at-stamp(p['time'])<=150:return False
        if o.get('entries',0)<2806 or o.get('uncertain_scopes',0)>0 or o.get('protocol_scopes',0)>0:return False
        b=p.get('books')or{}
        if b.get('ReadyInstruments',0)<332 or b.get('InvalidInstruments',1)!=0:return False
    return True
