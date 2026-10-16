"""Bounded Docker HTTP over the local Unix socket, scoped to test/project MySQL."""
import http.client
import json
from pathlib import Path
import socket
import urllib.parse

ROOT=Path('/home/ubuntu/crypto-market-info')
DATA={
 'crypto-market-info-mysql':ROOT/'var/storage/mysql/data',
 'cmi-backlog-validation-mysql':ROOT/'var/storage/write-backlog-improvement/bench-mysql/data',
}
class UnixHTTP(http.client.HTTPConnection):
    def connect(self):
        self.sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect('/var/run/docker.sock')

def request(name,method,action='',body=None,timeout=10):
    assert name in DATA
    path='/v1.45/containers/'+urllib.parse.quote(name,safe='')+action
    conn=UnixHTTP('localhost',timeout=timeout)
    try:
        raw=None if body is None else json.dumps(body).encode()
        conn.request(method,path,body=raw,headers={'Content-Type':'application/json','Connection':'close'})
        response=conn.getresponse();data=response.read(1<<20)
        if response.status not in [200,204]:raise RuntimeError(f'project Docker {method} {action} failed ({response.status})')
        return json.loads(data)if data else None
    finally:conn.close()

def inspect(name):
    value=request(name,'GET','/json')
    assert value['Name']=='/'+name
    assert value['HostConfig']['NetworkMode']=='none'
    assert any(Path(m['Source'])==DATA[name]and m['Destination']=='/var/lib/mysql'for m in value['Mounts'])
    return value

def update_limits(name,memory,swap):
    inspect(name)
    assert 0<memory<=swap<=6<<30
    return request(name,'POST','/update',{'Memory':memory,'MemorySwap':swap})

def stop_isolated():
    name='cmi-backlog-validation-mysql';value=inspect(name)
    assert value['HostConfig']['AutoRemove'] is False, 'preserve the isolated container definition before stopping'
    # The HTTP deadline is separate from MySQL's shutdown grace. A timeout
    # requires readback; never turn a slow durable flush into a 30-second kill.
    return request(name,'POST','/stop?t=600',timeout=10)

def start_isolated():
    name='cmi-backlog-validation-mysql';inspect(name)
    return request(name,'POST','/start',timeout=30)
