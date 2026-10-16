#!/usr/bin/env python3
"""Bounded, reversible resource trial on the two pinned local MySQL instances."""
import argparse
import datetime as dt
import hashlib
import json
import os
os.environ.setdefault('DOCKER_API_VERSION', '1.45')
from pathlib import Path
import shutil
import subprocess

ROOT = Path('/home/ubuntu/crypto-market-info')
OUT = ROOT / 'var/storage/write-backlog-improvement/mysql-capacity-1g'
INSTANCES = {
    'crypto-market-info-mysql': 'b820454a-c654-11f1-80fa-a04da03d4fd6',
    'cmi-backlog-validation-mysql': 'a93e84b2-c7c3-11f1-bf43-7ab521822a4d',
}
CONFIG_FILES = [ROOT / 'deploy/storage/mysql.cnf',
                ROOT / 'deploy/systemd/crypto-market-info-mysql.service',
                Path('/home/ubuntu/.config/systemd/user/crypto-market-info-mysql.service')]


def replace_config(path, text):
    staged = path.with_name(path.name + '.cmi-new')
    shutil.copy2(path, staged)
    staged.write_text(text)
    with staged.open('rb') as stream:
        os.fsync(stream.fileno())
    os.replace(staged, path)


LOCAL_CLIENT = None

def sql(name, statement):
    # Docker inspect still verifies container/data-directory identity; direct
    # official clients avoid the daemon's currently stalled exec endpoint.
    global LOCAL_CLIENT
    if LOCAL_CLIENT is None:
        import importlib.util
        spec=importlib.util.spec_from_file_location('capacity_local_mysql_client', Path(__file__).with_name('mysql_client.py'))
        LOCAL_CLIENT=importlib.util.module_from_spec(spec);spec.loader.exec_module(LOCAL_CLIENT)
    target='production' if name=='crypto-market-info-mysql' else 'isolated'
    assert name in INSTANCES
    return LOCAL_CLIENT.sql(target,statement)


LOCAL_DOCKER = None

def docker_control():
    global LOCAL_DOCKER
    if LOCAL_DOCKER is None:
        import importlib.util
        spec=importlib.util.spec_from_file_location('capacity_local_docker', Path(__file__).with_name('docker_control.py'))
        LOCAL_DOCKER=importlib.util.module_from_spec(spec);spec.loader.exec_module(LOCAL_DOCKER)
    return LOCAL_DOCKER

def update_limits(name,memory,swap):
    assert name in INSTANCES
    return docker_control().update_limits(name,memory,swap)


def inspect(name):
    d = docker_control().inspect(name)
    assert d['HostConfig']['NetworkMode'] == 'none' and d['State']['Running']
    data = ROOT / ('var/storage/mysql/data' if name == 'crypto-market-info-mysql'
                   else 'var/storage/write-backlog-improvement/bench-mysql/data')
    assert any(m['Destination'] == '/var/lib/mysql' and Path(m['Source']) == data for m in d['Mounts'])
    values = sql(name, 'SELECT @@server_uuid,@@innodb_buffer_pool_size,@@innodb_flush_log_at_trx_commit,@@sync_binlog,@@innodb_doublewrite').split('\t')
    assert values[0] == INSTANCES[name] and values[2:] == ['1', '1', 'ON']
    return dict(memory=d['HostConfig']['Memory'], swap=d['HostConfig']['MemorySwap'],
                buffer=int(values[1]), uuid=values[0])


def save(name, value):
    target = OUT / name
    target.write_text(json.dumps(value, indent=2) + '\n')
    target.chmod(0o600)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['start', 'keep', 'rollback', 'restore-limits'])
    action = parser.parse_args().action
    if action == 'start':
        OUT.mkdir(mode=0o700, exist_ok=False)
        available = int(next(x.split()[1] for x in Path('/proc/meminfo').read_text().splitlines() if x.startswith('MemAvailable:'))) * 1024
        assert available >= 2500 << 20, 'insufficient host headroom for the trial'
        before = {name: inspect(name) for name in INSTANCES}
        assert all(v['buffer'] == 512 << 20 for v in before.values())
        save('before.json', before)
        save('started.json', {'utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'host_available': available})
        for name in INSTANCES:
            update_limits(name,2560<<20,3072<<20)
            sql(name, 'SET GLOBAL innodb_buffer_pool_size=1073741824')
            save(name + '.json', inspect(name))
    elif action == 'keep':
        current = {name: inspect(name) for name in INSTANCES}
        assert all(v['buffer'] == 1024 << 20 and v['memory'] == 2560 << 20 for v in current.values())
        assert not (OUT / 'config-prepared.json').exists()
        prepared = []
        for number, path in enumerate(CONFIG_FILES):
            shutil.copy2(path, OUT / f'config-before-{number}')
            text = path.read_text()
            if path.suffix == '.cnf':
                assert text.count('innodb-buffer-pool-size=512M') == 1
                text = text.replace('innodb-buffer-pool-size=512M', 'innodb-buffer-pool-size=1024M')
            else:
                assert text.count('--memory 1536m --memory-swap 2048m') == 1
                text = text.replace('--memory 1536m --memory-swap 2048m', '--memory 2560m --memory-swap 3072m')
            prepared.append({'path': str(path), 'before': hashlib.sha256(path.read_bytes()).hexdigest(),
                             'after': hashlib.sha256(text.encode()).hexdigest(), 'text': text})
        save('config-prepared.json', prepared)
        for item in prepared:
            replace_config(Path(item['path']), item['text'])
        subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True, timeout=15)
        save('kept.json', {'utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'current': current,
                          'database_restart': False})
    elif action == 'rollback':
        before = json.loads((OUT / 'before.json').read_text())
        for name, old in before.items():
            inspect(name)
            sql(name, f"SET GLOBAL innodb_buffer_pool_size={old['buffer']}")
            # Keep headroom until the asynchronous buffer shrink has completed.
            save(name + '-rollback-buffer.json', inspect(name))
        if (OUT / 'config-prepared.json').exists():
            prepared = json.loads((OUT / 'config-prepared.json').read_text())
            for number, item in enumerate(prepared):
                path = Path(item['path'])
                assert path in CONFIG_FILES
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
                assert digest in (item['before'], item['after']), 'configuration changed since trial'
                if digest == item['after']:
                    replace_config(path, (OUT / f'config-before-{number}').read_text())
            subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True, timeout=15)
        save('rollback.json', {'utc': dt.datetime.now(dt.timezone.utc).isoformat(),
                              'container_limit_restore_pending_resize': True})
    else:
        before = json.loads((OUT / 'before.json').read_text())
        for name, old in before.items():
            current = inspect(name)
            assert current['buffer'] == old['buffer']
            resize = sql(name, "SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_resize_status'")
            assert 'Completed resizing buffer pool' in resize
            subprocess.run(['docker', 'update', '--memory', str(old['memory']), '--memory-swap', str(old['swap']), name],
                           check=True, capture_output=True, timeout=15)
        save('limits-restored.json', {'utc': dt.datetime.now(dt.timezone.utc).isoformat()})


if __name__ == '__main__':
    main()
