from pathlib import Path
import subprocess,json,datetime
ROOT=Path(__file__).resolve().parent
manifest=[]
for out in [ROOT]+[ROOT/'windows'/str(n) for n in range(4)]:
    c=json.loads((out/'latest_commit.jsonl').read_text())
    at=datetime.datetime.fromisoformat(c['minute_time']).replace(tzinfo=datetime.timezone.utc)+datetime.timedelta(seconds=59)
    cmd=[str(ROOT/'options-check'),'--database','crypto_market_info','--profile','live','--run',c['run_id'],'--at',at.isoformat()]
    r=subprocess.run(cmd,capture_output=True,text=True,timeout=40)
    (out/'verified_minute.json').write_text(r.stdout)
    (out/'verification.stderr').write_text(r.stderr)
    manifest.append({'directory':str(out.relative_to(ROOT)),'command':cmd,'returncode':r.returncode})
    print(out.name,r.returncode,r.stderr[:200],flush=True)
(ROOT/'verification_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
