"""Bounded DEX coverage and exact atom summaries; latest revision precedes filtering."""
from pathlib import Path
import sys, hashlib
sys.path.insert(0,str(Path(__file__).resolve().parent.parent/'options'))
from capture import query

ROOT=Path(__file__).resolve().parent
MANIFEST=hashlib.sha256((ROOT.parents[2]/'internal/dex/ethereum/manifest.json').read_bytes()).hexdigest()
BASE=f"chain_id=1 AND manifest_hash=unhex('{MANIFEST}')"

if __name__=='__main__':
    latest=query('latest',f"SELECT block_number,hex(block_hash) AS block_hash_hex,hex(manifest_hash) AS manifest_hash_hex,hex(batch_id) AS batch_id_hex,hex(quote_members) AS quote_members_hex,block_time,received_at,available_at,capture_mode,canonical,finality,quote_coverage,expected_quotes,actual_quotes,committed FROM dex_block FINAL WHERE {BASE} AND block_time >= '2026-10-01 00:00:00' ORDER BY block_number DESC LIMIT 24",ROOT)
    query('coverage_recent',f"SELECT toDate(block_time) AS day,finality,capture_mode,quote_coverage,canonical,committed,count() AS blocks,min(block_time) AS first_utc,max(block_time) AS latest_utc,min(block_number) AS first_height,max(block_number) AS last_height FROM dex_block FINAL WHERE {BASE} AND block_time>='2026-09-28 00:00:00' GROUP BY day,finality,capture_mode,quote_coverage,canonical,committed ORDER BY day,finality,capture_mode,quote_coverage",ROOT)
    windows=[('2026-09-28 00:00:00','2026-09-28 02:00:00'),('2026-09-29 06:00:00','2026-09-29 08:00:00'),('2026-09-30 12:00:00','2026-09-30 14:00:00'),('2026-10-01 15:00:00','2026-10-01 17:00:00')]
    for n,(start,end) in enumerate(windows):
        times=f"block_time>='{start}' AND block_time<'{end}'"
        blocks=query(f'window_{n}_blocks',f"SELECT block_number,hex(block_hash) AS block_hash_hex,hex(manifest_hash) AS manifest_hash_hex,hex(batch_id) AS batch_id_hex,hex(quote_members) AS quote_members_hex,block_time,capture_mode,canonical,finality,quote_coverage,expected_quotes,actual_quotes,committed FROM dex_block FINAL WHERE {BASE} AND {times} ORDER BY block_number",ROOT)
        if not blocks:continue
        lo,hi=min(int(b['block_number']) for b in blocks),max(int(b['block_number']) for b in blocks)
        query(f'window_{n}_summary',f"SELECT q.route_id,toString(q.requested_amount_raw) AS amount_atoms,count() AS observations,countIf(q.amount_out_raw>q.requested_amount_raw) AS positive,toString(max(toInt256(q.amount_out_raw)-toInt256(q.requested_amount_raw))) AS best_gross_atoms,argMax(q.block_time,toInt256(q.amount_out_raw)-toInt256(q.requested_amount_raw)) AS best_utc,min(q.block_time) AS first_utc,max(q.block_time) AS last_utc FROM (SELECT * FROM dex_route_quote FINAL WHERE {BASE} AND block_number BETWEEN {lo} AND {hi}) AS q INNER JOIN (SELECT batch_id,block_hash,manifest_hash FROM dex_block FINAL WHERE {BASE} AND {times} AND canonical AND committed AND finality='finalized' AND capture_mode='live' AND quote_coverage='complete' AND actual_quotes=expected_quotes) AS b ON q.batch_id=b.batch_id AND q.block_hash=b.block_hash AND q.manifest_hash=b.manifest_hash WHERE q.quote_role='strategy' AND q.status='ok' AND q.amount_in_raw=q.requested_amount_raw GROUP BY q.route_id,q.requested_amount_raw ORDER BY q.route_id,q.requested_amount_raw",ROOT)
