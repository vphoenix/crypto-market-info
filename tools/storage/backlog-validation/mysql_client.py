"""Pinned local MySQL clients copied from the project's existing server image.

Credentials stay in the child environment, never in argv, reports or SQL files.
"""
import functools
import hashlib
import json
import os
from pathlib import Path
import subprocess

ROOT = Path('/home/ubuntu/crypto-market-info')
BASE = ROOT / 'var/storage/write-backlog-improvement/mysql-client-8.4.11'
EXPECTED = {
    'mysql': '8206bf76fe1f72ac7deca68c4f8dcca796473f4945af72a96479aadb658f69c8',
    'mysqldump': '457f01d8a3615261b109ef5df5fa4c14fe23e01144638f6f2f76eb2f4788bbcc',
}
SERVERS = {
    'production': (ROOT / 'var/storage/mysql/run/mysql.sock', 'b820454a-c654-11f1-80fa-a04da03d4fd6'),
    'isolated': (ROOT / 'var/storage/write-backlog-improvement/bench-mysql/run/mysql.sock', 'a93e84b2-c7c3-11f1-bf43-7ab521822a4d'),
}

@functools.lru_cache(maxsize=2)
def binary(kind):
    assert kind in EXPECTED
    path = BASE / kind
    assert not path.is_symlink()
    with path.open('rb') as stream:
        assert hashlib.file_digest(stream, 'sha256').hexdigest() == EXPECTED[kind], 'client hash changed'
    return str(path)

def environment():
    env = os.environ.copy()
    env['MYSQL_PWD'] = Path('/home/ubuntu/.config/crypto-market-info-storage/mysql-root-password').read_text().strip()
    return env

def command(name, *, kind='mysql', database=None):
    socket, _ = SERVERS[name]
    args = [binary(kind), '--no-defaults', '--no-login-paths', '--protocol=SOCKET', '--user=root', '--socket=' + str(socket)]
    if kind == 'mysql':
        args += ['--connect-timeout=3', '--skip-auto-rehash']
    if kind == 'mysql':
        args += ['--batch', '--skip-column-names']
    if database is not None:
        assert database == 'crypto_market_info'
        args += ['--database=' + database]
    return args

def verify(name):
    result = subprocess.run(command(name) + ['-e', 'SELECT @@server_uuid'], env=environment(), capture_output=True, timeout=10)
    if result.returncode or result.stdout.decode().strip() != SERVERS[name][1]:
        raise RuntimeError('local database identity check failed')

def sql(name, statement, *, limit=20):
    verify(name)
    result = subprocess.run(command(name) + ['-e', statement], env=environment(), capture_output=True, timeout=limit)
    if result.returncode:
        raise RuntimeError(f'bounded local database operation failed ({result.returncode})')
    return result.stdout.decode().strip()
