"""Rebuild existing read-only checkers with research query budgets only.

Production source and configuration are never edited. Generated copies and
binaries stay inside the two research directories and can be removed after use.
"""
from pathlib import Path
import shutil, subprocess, hashlib, json, os

ROOT=Path(__file__).resolve().parent
REPO=ROOT.parents[2]
DEST=ROOT/'generated_check_store'
DEST.mkdir(exist_ok=True)
manifest=[]
for src in (REPO/'internal/storage/clickhouse').glob('*.go'):
    if src.name.endswith('_test.go'): continue
    original=src.read_text()
    changed=original.replace('ch.Settings{"readonly": 1}', 'ch.Settings{"readonly": 1, "max_threads": 2, "max_execution_time": 60}')
    changed=changed.replace('ch.Settings{"readonly": 2, "max_execution_time": 120}', 'ch.Settings{"readonly": 1, "max_threads": 2, "max_execution_time": 60}')
    (DEST/src.name).write_text(changed)
    manifest.append({'source':str(src.relative_to(REPO)),'sha256':hashlib.sha256(original.encode()).hexdigest(),'budget_only_change':original!=changed})
for command,output in [('options-check',ROOT),('dex-check',ROOT.parent/'dex')]:
    source=(REPO/'cmd'/command/'main.go').read_text()
    source=source.replace('github.com/vphoenix/crypto-market-info/internal/storage/clickhouse','github.com/vphoenix/crypto-market-info/research/2026-10-02-opportunity-review/options/generated_check_store')
    if command=='dex-check':
        source=source.replace('chstore.OpenDEXReader(ctx, cfg.ClickHouse)','chstore.OpenDEXReader(ctx, chstore.Config(cfg.ClickHouse))')
    cmd=output/'generated_command'
    cmd.mkdir(exist_ok=True)
    (cmd/'main.go').write_text(source)
    env=os.environ.copy()
    env['GOCACHE']=str(ROOT/'go-build-cache')
    subprocess.run(['go','build','-o',str(output/command),str(cmd/'main.go')],cwd=REPO,env=env,check=True)
(ROOT/'checker_build_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
