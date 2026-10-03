"""Read-only ClickHouse evidence capture; all cash amounts remain integers/Decimal."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import urllib.request
import urllib.error

ROOT = Path(__file__).resolve().parent
QUERIES = {
    "coverage": """SELECT finality,capture_mode,quote_coverage,canonical,committed,
        count() AS blocks, min(block_time) AS first_utc,max(block_time) AS latest_utc,
        min(block_number) AS first_height,max(block_number) AS last_height
        FROM dex_block FINAL GROUP BY finality,capture_mode,quote_coverage,canonical,committed
        ORDER BY finality,capture_mode,quote_coverage""",
    "latest": """SELECT block_number,hex(block_hash) AS block_hash,block_time,received_at,available_at,
        capture_mode,canonical,finality,quote_coverage,expected_quotes,actual_quotes,committed
        FROM dex_block FINAL ORDER BY block_number DESC LIMIT 12""",
    "size_summary": """SELECT q.route_id,toString(q.requested_amount_raw) AS amount_atoms,
        count() AS observations,countIf(q.amount_out_raw>q.requested_amount_raw) AS positive,
        toString(max(toInt256(q.amount_out_raw)-toInt256(q.requested_amount_raw))) AS best_gross_atoms,
        argMax(q.block_time,toInt256(q.amount_out_raw)-toInt256(q.requested_amount_raw)) AS best_utc,
        min(q.block_time) AS first_utc,max(q.block_time) AS last_utc
        FROM dex_route_quote AS q FINAL INNER JOIN
        (SELECT batch_id,block_hash,manifest_hash FROM dex_block FINAL
         WHERE canonical AND committed AND finality='finalized' AND capture_mode='live'
         AND quote_coverage='complete' AND actual_quotes=expected_quotes) AS b
        ON q.batch_id=b.batch_id AND q.block_hash=b.block_hash AND q.manifest_hash=b.manifest_hash
        WHERE q.quote_role='strategy' AND q.status='ok' AND q.amount_in_raw=q.requested_amount_raw
        GROUP BY q.route_id,q.requested_amount_raw ORDER BY q.route_id,q.requested_amount_raw""",
    "latest_sizes": """SELECT route_id,toString(requested_amount_raw) AS amount_atoms,
        toString(amount_out_raw) AS out_atoms,
        toString(toInt256(amount_out_raw)-toInt256(requested_amount_raw)) AS gross_atoms,
        status,reason,block_number,block_time,hex(block_hash) AS block_hash
        FROM dex_route_quote FINAL WHERE batch_id=(SELECT batch_id FROM dex_block FINAL
        WHERE canonical AND committed AND finality='finalized' AND capture_mode='live'
        AND quote_coverage='complete' ORDER BY block_number DESC LIMIT 1)
        AND quote_role='strategy' ORDER BY route_id,requested_amount_raw""",
}

if __name__ == "__main__":
    manifest=[]
    for name, sql in QUERIES.items():
        sql += " FORMAT JSON"
        (ROOT / (name + ".sql")).write_text(sql + "\n")
        req=urllib.request.Request("http://127.0.0.1:8123/?database=crypto_market_info&readonly=1&max_execution_time=90&max_threads=2",data=sql.encode())
        captured=datetime.now(timezone.utc).isoformat()
        try:
            with urllib.request.urlopen(req,timeout=100) as response:
                raw=response.read();status=response.status
        except urllib.error.HTTPError as exc:
            raw=exc.read();status=exc.code
        (ROOT / (name + ".json")).write_bytes(raw)
        manifest.append({"name":name,"captured_at_utc":captured,"http_status":status,"sha256":hashlib.sha256(raw).hexdigest()})
        print(name,status,raw[:600].decode(errors="replace"))
    (ROOT / "capture.json").write_text(json.dumps(manifest,indent=2)+"\n")
