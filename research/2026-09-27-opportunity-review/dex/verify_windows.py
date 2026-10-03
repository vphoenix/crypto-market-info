"""Bound report queries to two-hour slices to avoid ClickHouse query-size limits."""
from pathlib import Path
from datetime import datetime,timedelta,timezone
import subprocess,json

root=Path(__file__).resolve().parent
start=datetime(2026,9,25,0,tzinfo=timezone.utc)
end=datetime(2026,9,26,16,26,tzinfo=timezone.utc)
summary=[]
while start<end:
    stop=min(start+timedelta(hours=2),end)
    name=start.strftime('%Y%m%dT%H%M')
    out=root/'verified-windows'/name
    args=['/tmp/crypto-review-dex-check','--from',start.isoformat(),'--to',stop.isoformat(),'--report-dir',str(out)]
    result=subprocess.run(args,capture_output=True,text=True,timeout=180)
    out.mkdir(parents=True,exist_ok=True)
    (out/'stdout.txt').write_text(result.stdout+result.stderr)
    summary.append({'from':start.isoformat(),'to':stop.isoformat(),'returncode':result.returncode,'command':args})
    print(name,result.returncode,result.stdout.splitlines()[:2],flush=True)
    if result.returncode:
        print(result.stderr,flush=True)
        break
    start=stop
(root/'verify_manifest.json').write_text(json.dumps(summary,indent=2)+'\n')
