"""Read-only Redis/spool observation for the pinned local deployment."""
import datetime, os, socket, struct
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]

def redis(*args):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(6)
        s.connect(str(ROOT / 'var/storage/redis/run/redis.sock'))
        parts = [str(v).encode() for v in args]
        s.sendall(b'*' + str(len(parts)).encode() + b'\r\n' + b''.join(b'$' + str(len(p)).encode() + b'\r\n' + p + b'\r\n' for p in parts))
        f = s.makefile('rb')
        def read():
            line = f.readline(); kind = line[:1]; body = line[1:-2]
            if kind == b'-': raise RuntimeError(body.decode())
            if kind == b':': return int(body)
            if kind == b'+': return body.decode()
            if kind == b'$':
                n = int(body)
                if n < 0: return None
                data = f.read(n); f.read(2); return data.decode(errors='replace')
            if kind == b'*': return [read() for _ in range(int(body))]
            raise RuntimeError('invalid Redis response')
        return read()

def spool_header(path):
    with open(path, 'rb') as f:
        if f.read(5) not in (b'CMIS\x01', b'CMIS\x02'): raise ValueError('header')
        result = {}
        for name in ['database', 'kind', 'id', 'service']:
            n = struct.unpack('>H', f.read(2))[0]
            result[name] = f.read(n).decode()
        gen, minute, codec, members, size = struct.unpack('>QqHII', f.read(26))
        result.update(generation=gen, minute_utc=datetime.datetime.fromtimestamp(minute, datetime.timezone.utc).isoformat(), members=members, payload_bytes=size)
        return result

def queues():
    files = []
    for ent in os.scandir(ROOT / 'var/storage/spool'):
        if ent.name.endswith('.batch'):
            try:
                st = ent.stat(); files.append((st.st_mtime, st.st_size, ent.path))
            except FileNotFoundError: pass
    files.sort()
    info = dict(line.split(':', 1) for line in redis('INFO', 'memory').splitlines() if ':' in line)
    key = 'cmi:v1:ready:crypto_market_info'
    result = {'time_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'redis_ready': redis('ZCARD', key), 'redis_oldest': redis('ZRANGE', key, 0, 0, 'WITHSCORES'),
              'redis_used_bytes': int(info['used_memory']), 'spool_files': len(files), 'spool_bytes': sum(x[1] for x in files)}
    result['spool_oldest_headers'] = []
    for _, _, path in files[:12]:
        try: result['spool_oldest_headers'].append(spool_header(path))
        except FileNotFoundError: pass
    return result
