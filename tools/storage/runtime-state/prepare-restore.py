#!/usr/bin/env python3
"""Invalidate bound runtime checkpoints before an explicit database restore.

This tool rotates local recovery epochs only. It never restores a database,
starts/stops a service, changes a producer, or removes a checkpoint/pending file.
The inventory, exact binary hash and stopped service states are recorded.
"""
import argparse
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import uuid


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def atomic_json(path, value):
    descriptor, name = tempfile.mkstemp(prefix='.restore-fence-', dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    descriptor = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def stopped(unit):
    if not re.fullmatch(r'crypto-market-info(?:-[a-z0-9-]+)?\.service', unit):
        raise ValueError('invalid project service name')
    result = subprocess.run(['systemctl', '--user', 'show', unit,
                             '-p', 'LoadState', '-p', 'ActiveState', '-p', 'MainPID'],
                            check=True, capture_output=True, text=True, timeout=10)
    state = dict(line.split('=', 1) for line in result.stdout.splitlines())
    if state.get('LoadState') != 'loaded' or state.get('ActiveState') not in ('inactive', 'failed') or state.get('MainPID') != '0':
        raise RuntimeError('restore requires stopped service: ' + unit)
    return state


def epoch(tool, operation, item, *extra):
    result = subprocess.run([str(tool), operation, '--epoch-file', item['epoch_file'],
                             '--target', item['target'], *extra],
                            check=True, capture_output=True, text=True, timeout=20)
    value = json.loads(result.stdout)
    if value.get('Target') != item['target'] or not value.get('ID'):
        raise RuntimeError('recovery tool returned a different target')
    return value


def _prepare(inventory_path, tool, tool_sha256, output, reason):
    if not re.fullmatch(r'[0-9a-f]{64}', tool_sha256) or digest(tool) != tool_sha256:
        raise RuntimeError('runtime-state binary hash mismatch')
    if not reason.strip():
        raise ValueError('restore reason is required')
    inventory = json.loads(inventory_path.read_text())
    if inventory.get('version') != 1 or not inventory.get('targets'):
        raise ValueError('explicit versioned target inventory required')
    seen = set()
    for item in inventory['targets']:
        if not re.fullmatch(r'[0-9a-f-]{36}/[A-Za-z0-9_]+', item['target']):
            raise ValueError('target must be server UUID/database')
        if not item.get('expected_epoch') or not item.get('services') or not item.get('state_directories'):
            raise ValueError('target requires current epoch, services and state directories')
        path = Path(item['epoch_file'])
        if not path.is_absolute() or str(path) in seen:
            raise ValueError('epoch paths must be absolute and distinct')
        seen.add(str(path))
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    record_path = output / 'restore-fence.json'
    original_hash = digest(inventory_path)
    if record_path.exists():
        record = json.loads(record_path.read_text())
        if record['inventory_sha256'] != original_hash or record['runtime_tool_sha256'] != tool_sha256 or record['reason'] != reason:
            raise RuntimeError('existing restore job has different inputs')
    else:
        record = {'version': 1, 'job_id': str(uuid.uuid4()), 'created_at': now(),
                  'inventory_sha256': original_hash, 'runtime_tool_sha256': tool_sha256,
                  'reason': reason, 'targets': {}, 'database_restored': False}
        atomic_json(record_path, record)
    locks = []
    try:
        # Hold every runtime ownership lock before rotating the first target.
        for item in inventory['targets']:
            for directory in item['state_directories']:
                path = Path(directory)
                if not path.is_absolute() or not path.is_dir():
                    raise ValueError('existing absolute runtime directory required')
                descriptor = os.open(path / 'runtime.lock', os.O_RDWR)
                locks.append(descriptor)
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            for unit in item['services']:
                stopped(unit)
        marker = reason + ' [restore-job:' + record['job_id'] + ']'
        # Verify ALL epochs before changing any. A partial rotation is retained
        # and resumable; neither an epoch nor a producer is rolled backwards.
        current = {}
        for item in inventory['targets']:
            value = epoch(tool, 'epoch-show', item)
            resumed = value.get('Previous') == item['expected_epoch'] and value.get('Reason') == marker
            if value['ID'] != item['expected_epoch'] and not resumed:
                raise RuntimeError('epoch changed outside this restore job')
            current[item['target']] = (value, resumed)
        for item in inventory['targets']:
            state = {unit: stopped(unit) for unit in item['services']}
            value, resumed = current[item['target']]
            if not resumed:
                value = epoch(tool, 'epoch-rotate', item,
                              '--expected-epoch', item['expected_epoch'], '--reason', marker)
            record['targets'][item['target']] = {
                'previous_epoch': item['expected_epoch'], 'new_epoch': value['ID'],
                'epoch_file': item['epoch_file'], 'checked_at': now(), 'services': state,
                'state_directories_preserved': item['state_directories']}
            atomic_json(record_path, record)
        record['prepared_at'] = now()
        record['status'] = 'checkpoints invalidated; database restore and explicit reconstruction still required'
        atomic_json(record_path, record)
        return record
    finally:
        for descriptor in reversed(locks):
            os.close(descriptor)


def prepare(inventory_path, tool, tool_sha256, output, reason):
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(output / 'restore-job.lock', os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return _prepare(inventory_path, tool, tool_sha256, output, reason)
    finally:
        os.close(descriptor)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', type=Path, required=True)
    parser.add_argument('--runtime-tool', type=Path, required=True)
    parser.add_argument('--runtime-tool-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--reason', required=True)
    args = parser.parse_args()
    report = prepare(args.inventory, args.runtime_tool, args.runtime_tool_sha256,
                     args.output, args.reason)
    print(json.dumps({'status': report['status'], 'record': str(args.output / 'restore-fence.json')}, ensure_ascii=False))
