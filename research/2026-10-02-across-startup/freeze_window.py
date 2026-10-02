"""One-off public RPC audit: freeze a 30-day finalized backfill window."""
import concurrent.futures
import datetime
import hashlib
import json
import pathlib
import time
import urllib.request

ROOT = pathlib.Path(__file__).parent
CHAINS = [(8453, "https://mainnet.base.org"), (42161, "https://arb1.arbitrum.io/rpc")]

def freeze(item):
    chain, url = item
    calls = []
    def rpc(method, params):
        request = {"jsonrpc": "2.0", "id": len(calls) + 1, "method": method, "params": params}
        for attempt in range(3):
            started = datetime.datetime.now(datetime.timezone.utc).isoformat()
            try:
                req = urllib.request.Request(url, data=json.dumps(request).encode(), headers={"Content-Type": "application/json", "User-Agent": "crypto-market-info/1.0"})
                with urllib.request.urlopen(req, timeout=10) as response:
                    raw = response.read()
                parsed = json.loads(raw)
                calls.append({"requested_at": started, "request": request, "response": parsed, "payload_sha256": hashlib.sha256(raw).hexdigest()})
                if parsed.get("error") or parsed.get("id") != request["id"] or parsed.get("result") is None:
                    raise ValueError("invalid_rpc_response")
                return parsed["result"]
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(1)
    assert int(rpc("eth_chainId", []), 16) == chain
    def header(number):
        return rpc("eth_getBlockByNumber", [hex(number) if isinstance(number, int) else number, False])
    end = header("finalized")
    target = int(end["timestamp"], 16) - 30 * 86400
    lo, hi = 0, int(end["number"], 16)
    while lo < hi:
        mid = (lo + hi) // 2
        if int(header(mid)["timestamp"], 16) < target:
            lo = mid + 1
        else:
            hi = mid
    first = header(lo)
    previous = header(lo - 1)
    assert int(previous["timestamp"], 16) < target <= int(first["timestamp"], 16)
    result = {"chain_id": chain, "rpc": url, "from_block": lo, "to_block": int(end["number"], 16), "target_from_utc": datetime.datetime.fromtimestamp(target, datetime.timezone.utc).isoformat(), "from_header": first, "previous_header": previous, "finalized_to_header": end, "calls": calls}
    (ROOT / f"window-{chain}.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k: result[k] for k in ("chain_id", "from_block", "to_block", "target_from_utc")}), flush=True)

if __name__ == "__main__":
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(freeze, CHAINS))
