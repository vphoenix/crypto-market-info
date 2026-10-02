from pathlib import Path
import subprocess,json
ROOT=Path(__file__).resolve().parent
windows=[('2026-09-28T00:00:00Z','2026-09-28T02:00:00Z'),('2026-09-29T06:00:00Z','2026-09-29T08:00:00Z'),('2026-09-30T12:00:00Z','2026-09-30T14:00:00Z'),('2026-10-01T15:00:00Z','2026-10-01T17:00:00Z')]
manifest=[]
for n,(start,end) in enumerate(windows):
    out=ROOT/'verified_windows'/str(n);out.mkdir(parents=True,exist_ok=True)
    cmd=[str(ROOT/'dex-check'),'--from',start,'--to',end,'--capital-usdt','1000000','--report-dir',str(out)]
    r=subprocess.run(cmd,capture_output=True,text=True,timeout=180)
    (out/'stdout.txt').write_text(r.stdout+r.stderr)
    manifest.append({'window':n,'command':cmd,'returncode':r.returncode})
    print(n,r.returncode,r.stdout[:200],r.stderr[:200],flush=True)
    if r.returncode:break
(ROOT/'check_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
