"""Read-only exports for opportunity research; no account or trading APIs."""
import json
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent

def query(sql):
    req = urllib.request.Request(
        'http://127.0.0.1:8123/?readonly=1&max_execution_time=45&max_threads=2',
        data=(sql.rstrip(';') + ' FORMAT JSONEachRow').encode(),
    )
    with urllib.request.urlopen(req, timeout=60) as res:
        return [json.loads(line, parse_float=str) for line in res if line.strip()]

if __name__ == '__main__':
    name, sql_file = sys.argv[1:]
    rows = query(Path(sql_file).read_text())
    (ROOT / (name + '.json')).write_text(json.dumps(rows, indent=2, ensure_ascii=False) + '\n')
    print(json.dumps(rows, ensure_ascii=False))
