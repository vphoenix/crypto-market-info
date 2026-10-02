"""Current finalized Sky capacity and existing exact-size cost references."""
from pathlib import Path
import sys,hashlib
sys.path.insert(0,str(Path(__file__).resolve().parent.parent/'options'))
from capture import query
ROOT=Path(__file__).resolve().parent
manifest=hashlib.sha256((ROOT.parents[2]/'internal/dex/ethereum/manifest.json').read_bytes()).hexdigest()
base=f"chain_id=1 AND manifest_hash=unhex('{manifest}')"
b=query('exit_reference_block',f"SELECT block_number,hex(block_hash) AS block_hash_hex,hex(batch_id) AS batch_id_hex,block_time,finality,canonical,committed,quote_coverage,expected_quotes,actual_quotes FROM dex_block FINAL WHERE {base} AND block_time >= '2026-10-01 12:00:00' AND finality='finalized' AND canonical AND committed AND quote_coverage='complete' ORDER BY block_number DESC LIMIT 1",ROOT)[0]
where=f"{base} AND block_number={b['block_number']} AND block_hash=unhex('{b['block_hash_hex']}') AND batch_id=unhex('{b['batch_id_hex']}')"
query('exit_reference_sky',f"SELECT block_number,block_time,hex(block_hash) AS block_hash_hex,hex(payload_hash) AS payload_hash_hex,module_id,toString(tin) AS tin_atoms,toString(tout) AS tout_atoms,toString(buf) AS buf_atoms,toString(dai_cash) AS dai_cash_atoms,toString(usdc_pocket_cash) AS usdc_cash_atoms,toString(pocket_allowance) AS allowance_atoms,vat_live,dai_join_live,dai_join_ward,usds_join_ward,identity_ok,state_complete,reason FROM dex_sky_state FINAL WHERE {where}",ROOT)
query('exit_reference_quotes',f"SELECT block_number,block_time,hex(block_hash) AS block_hash_hex,hex(payload_hash) AS payload_hash_hex,quote_role,route_id,quote_mode,hex(token_in) AS token_in_hex,hex(token_out) AS token_out_hex,toString(requested_amount_raw) AS requested_atoms,toString(amount_in_raw) AS input_atoms,toString(amount_out_raw) AS output_atoms,status,reason FROM dex_route_quote FINAL WHERE {where} AND (quote_role='cost' OR requested_amount_raw=1000000000000) ORDER BY quote_role,route_id",ROOT)
