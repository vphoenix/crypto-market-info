"""Read-only bounded Swap-event activity probe, not a profitability backtest."""
from collections import Counter
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parent / "pool-evidence"
RPC = "https://ethereum-rpc.publicnode.com"
discovery = json.loads((ROOT / "summary.json").read_text())


def batch(label, calls):
    payload = [{"jsonrpc": "2.0", "id": i, "method": m, "params": p}
               for i, (m, p) in enumerate(calls, 1)]
    started = datetime.now(timezone.utc).isoformat()
    response = requests.post(RPC, json=payload, timeout=45)
    (ROOT / f"{label}.raw.json").write_bytes(response.content)
    (ROOT / f"{label}.meta.json").write_text(json.dumps({
        "endpoint": RPC, "observed_at_utc": started,
        "http_status": response.status_code, "sha256": sha256(response.content).hexdigest(),
        "requests": payload,
    }, indent=2) + "\n")
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list):
        raise RuntimeError(data)
    by_id = {x["id"]: x for x in data}
    assert set(by_id) == set(range(1, len(calls) + 1)), "Incomplete batch"
    for item in data:
        if "error" in item:
            raise RuntimeError(item["error"])
    return [by_id[i]["result"] for i in range(1, len(calls) + 1)]


end = discovery["block_number"]
start = end - 7199
sig = "Swap(address,address,int256,int256,uint160,uint128,int24)"
topic, first_block, last_block = batch("activity-anchor", [
    ("web3_sha3", ["0x" + sig.encode().hex()]),
    ("eth_getBlockByNumber", [hex(start), False]),
    ("eth_getBlockByNumber", [hex(end), False]),
])
assert last_block["hash"] == discovery["block_hash"]
pools = [p for p in discovery["pools"] if p.get("exists") and p["purpose"] == "strategy"]
addresses = [p["address"] for p in pools]
queries = []
for lower in range(start, end + 1, 900):
    upper = min(lower + 899, end)
    queries.append(("eth_getLogs", [{"fromBlock": hex(lower), "toBlock": hex(upper),
                                    "address": addresses, "topics": [topic]}]))
responses = batch("activity-swaps", queries)
logs = [log for part in responses for log in part]
identities = {(x["blockHash"], x["transactionHash"], x["logIndex"]) for x in logs}
assert len(identities) == len(logs), "Duplicate logs"
assert all(not x.get("removed", False) for x in logs), "Removed log in scan"
assert all(start <= int(x["blockNumber"], 16) <= end for x in logs)
counts = Counter(x["address"].lower() for x in logs)
for pool in pools:
    pool["swap_event_count"] = counts[pool["address"].lower()]
    pool["unique_transaction_count"] = len({x["transactionHash"] for x in logs
        if x["address"].lower() == pool["address"].lower()})
recheck = batch("activity-recheck", [("eth_getBlockByNumber", [hex(end), False])])[0]
assert recheck["hash"] == discovery["block_hash"], "Anchor changed"
summary = {"chain_id": 1, "from_block": start, "to_block": end, "end_hash": discovery["block_hash"],
           "from_time_utc": datetime.fromtimestamp(int(first_block["timestamp"], 16), timezone.utc).isoformat(),
           "to_time_utc": discovery["block_time_utc"], "rpc_chunks": len(queries), "swap_events": len(logs),
           "scope": "Bounded Swap log activity only; not profit, latency, or full state verification", "pools": pools}
(ROOT / "activity-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps({k: v for k, v in summary.items() if k != "pools"}, indent=2))
for pool in pools:
    print("/".join(pool["pair"]), pool["fee_pips"], pool["address"],
          "swaps=", pool["swap_event_count"], "transactions=", pool["unique_transaction_count"])
