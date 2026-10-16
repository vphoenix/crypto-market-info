#!/usr/bin/env python3
"""Inspect Keeper reads; cancel only revalidated reads of a stopped collector."""
import argparse
import datetime
import json
from pathlib import Path
import subprocess
import time

ROOT = Path('/home/ubuntu/crypto-market-info')


def service_state():
    result = subprocess.run(['systemctl', '--user', 'show',
        'crypto-market-info-justlend-keeper.service', '-p', 'MainPID', '-p', 'ActiveState'],
        check=True, capture_output=True, text=True, timeout=15)
    return dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)


def mysql(query):
    command = ['docker', 'exec', '-i', 'crypto-market-info-mysql', 'sh', '-c',
        'read -r CMI_QUERY_MAINT_PASSWORD\nexport MYSQL_PWD="$CMI_QUERY_MAINT_PASSWORD"\n'
        'exec mysql --user=root --socket=/run/cmi/mysql.sock --batch --raw --skip-column-names -e "$1"',
        'sh', query]
    secret = Path('/home/ubuntu/.config/crypto-market-info-storage/mysql-root-password').read_bytes().strip() + b'\n'
    result = subprocess.run(command, input=secret, capture_output=True, timeout=20)
    if result.returncode:
        raise RuntimeError('bounded maintenance SQL failed: ' + result.stderr.decode(errors='replace')[:300])
    return [json.loads(line) for line in result.stdout.decode().splitlines() if line.startswith('{')]


def candidates():
    return mysql("""SELECT /*+ MAX_EXECUTION_TIME(5000) */ JSON_OBJECT(
        'database',p.DB,'account',p.USER,'connection',p.ID,'thread',t.THREAD_ID,
        'statement_hash',SHA2(p.INFO,256),'summary',LEFT(p.INFO,200),'seconds',p.TIME,
        'transaction',x.TRX_ID,'rows_modified',COALESCE(x.TRX_ROWS_MODIFIED,0),
        'readonly',COALESCE(x.TRX_IS_READ_ONLY,1))
        FROM information_schema.PROCESSLIST p
        JOIN performance_schema.threads t ON t.PROCESSLIST_ID=p.ID
        LEFT JOIN information_schema.INNODB_TRX x ON x.TRX_MYSQL_THREAD_ID=p.ID
        WHERE p.DB='crypto_market_info_justlend_keeper' AND p.USER='cmi_live'
        AND p.COMMAND IN ('Query','Execute') AND p.ID<>CONNECTION_ID()
        AND UPPER(LTRIM(p.INFO)) LIKE 'SELECT %'
        AND p.INFO LIKE '%jl_keeper_%'""")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cancel-abandoned', action='store_true')
    parser.add_argument('--evidence', type=Path, required=True)
    args = parser.parse_args()
    report = {'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'caller': service_state(), 'candidates': candidates(), 'cancelled': [], 'retained': []}
    args.evidence.parent.mkdir(parents=True, exist_ok=True)
    args.evidence.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2), flush=True)
    if not args.cancel_abandoned:
        return
    for old in report['candidates']:
        state = service_state()
        if state.get('MainPID') != '0' or state.get('ActiveState') != 'inactive':
            raise RuntimeError('Keeper must be confirmed stopped; active callers are never cancelled')
        fresh = next((row for row in candidates() if row['connection'] == old['connection']), None)
        if fresh is None:
            continue
        identity = ['database', 'account', 'connection', 'thread', 'statement_hash', 'transaction']
        if any(fresh[k] != old[k] for k in identity) or fresh['rows_modified'] or not fresh['readonly']:
            report['retained'].append({'connection': old['connection'], 'reason': 'identity changed or non-readonly transaction'})
            continue
        mysql('KILL QUERY ' + str(int(fresh['connection'])))
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if not any(r['connection'] == fresh['connection'] and r['statement_hash'] == fresh['statement_hash'] for r in candidates()):
                break
            time.sleep(0.2)
        else:
            raise RuntimeError('cancelled read did not disappear within the verification bound')
        # A stopped caller cannot roll back a lingering transaction itself.
        # Close only this same still-owned thread if a transaction remains.
        lingering = mysql("SELECT JSON_OBJECT('thread',t.THREAD_ID,'transaction',x.TRX_ID) FROM performance_schema.threads t JOIN information_schema.INNODB_TRX x ON x.TRX_MYSQL_THREAD_ID=t.PROCESSLIST_ID WHERE t.PROCESSLIST_ID=" + str(int(fresh['connection'])))
        if lingering:
            if lingering[0]['thread'] != fresh['thread'] or service_state().get('MainPID') != '0':
                raise RuntimeError('connection ownership changed before transaction cleanup')
            mysql('KILL CONNECTION ' + str(int(fresh['connection'])))
            remaining = mysql("SELECT JSON_OBJECT('transaction',TRX_ID) FROM information_schema.INNODB_TRX WHERE TRX_MYSQL_THREAD_ID=" + str(int(fresh['connection'])))
            if remaining:
                raise RuntimeError('connection rollback still pending; retain evidence and stop cleanup')
        report['cancelled'].append(fresh['connection'])
        args.evidence.write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
