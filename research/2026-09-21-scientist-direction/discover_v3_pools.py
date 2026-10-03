"""Bounded, read-only mainnet discovery; no keys, signing, or transaction submission."""
from datetime import datetime, timezone
from hashlib import sha256
from itertools import combinations
import json
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parent / "pool-evidence"
ROOT.mkdir(exist_ok=True)
RPC = "https://ethereum-rpc.publicnode.com"
FACTORY = "0x1f98431c8ad98523631ae4a59f267346ea31f984"
TOKENS = {
    "USDC": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "DAI": "0x6b175474e89094c44da98b954eedeac495271d0f",
    "USDS": "0xdc035d45d973e3ec169d2276ddab16f1e407384f",
    "USDT": "0xdac17f958d2ee523a2206206994597c13d831ec7",
    "WETH": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
}


def call_batch(label, calls):
    payload = [{"jsonrpc": "2.0", "id": i, "method": method, "params": params}
               for i, (method, params) in enumerate(calls, start=1)]
    observed = datetime.now(timezone.utc).isoformat()
    response = requests.post(RPC, json=payload, timeout=30)
    raw = response.content
    (ROOT / f"{label}.raw.json").write_bytes(raw)
    (ROOT / f"{label}.meta.json").write_text(json.dumps({
        "endpoint": RPC, "observed_at_utc": observed, "http_status": response.status_code,
        "sha256": sha256(raw).hexdigest(), "requests": payload,
    }, indent=2) + "\n")
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list):
        raise RuntimeError(f"Expected batch, got {data!r}")
    by_id = {r["id"]: r for r in data}
    if set(by_id) != set(range(1, len(payload) + 1)):
        raise RuntimeError("Incomplete response batch")
    return [by_id[i] for i in range(1, len(payload) + 1)]


def result(item):
    if "error" in item:
        raise RuntimeError(item["error"])
    return item["result"]


def word_address(address):
    return address[2:].lower().zfill(64)


initial = call_batch("anchor", [("eth_chainId", []), ("eth_getBlockByNumber", ["latest", False])])
assert int(result(initial[0]), 16) == 1
block = result(initial[1])
anchor = {"blockHash": block["hash"], "requireCanonical": True}
pairs = [(a, b, fee, "strategy") for a, b in combinations(["USDC", "DAI", "USDS"], 2)
         for fee in [100, 500, 3000, 10000]]
pairs += [("USDC", "USDT", fee, "settlement_reference") for fee in [100, 500]]
pairs += [("USDC", "WETH", 500, "gas_reference")]
queries = []
for a, b, fee, _ in pairs:
    data = "0x1698ee82" + word_address(TOKENS[a]) + word_address(TOKENS[b]) + f"{fee:064x}"
    queries.append(("eth_call", [{"to": FACTORY, "data": data}, anchor]))
found = call_batch("factory-get-pool", queries)
pools = []
for spec, item in zip(pairs, found):
    a, b, fee, purpose = spec
    if "error" in item:
        pools.append({"pair": [a, b], "fee_pips": fee, "purpose": purpose, "error": item["error"]})
        continue
    value = result(item)
    address = "0x" + value[-40:]
    pools.append({"pair": [a, b], "fee_pips": fee, "purpose": purpose,
                  "address": address, "exists": int(value, 16) != 0})
live = [p for p in pools if p.get("exists")]
state_calls = []
methods = [("token0", "0x0dfe1681"), ("token1", "0xd21220a7"),
           ("fee", "0xddca3f43"), ("tickSpacing", "0xd0c93a7c"),
           ("slot0", "0x3850c7bd"), ("liquidity", "0x1a686502")]
for pool in live:
    for _, selector in methods:
        state_calls.append(("eth_call", [{"to": pool["address"], "data": selector}, anchor]))
states = call_batch("pool-state", state_calls) if state_calls else []
for i, pool in enumerate(live):
    fields = {}
    for j, (name, _) in enumerate(methods):
        item = states[i * len(methods) + j]
        if "error" in item:
            fields[name] = {"error": item["error"]}
            continue
        value = result(item)
        if name.startswith("token"):
            fields[name] = "0x" + value[-40:]
        elif name == "slot0":
            words = [int(value[k:k+64], 16) for k in range(2, len(value), 64)]
            tick = words[1] if words[1] < 2**255 else words[1] - 2**256
            fields[name] = {"sqrt_price_x96": str(words[0]), "tick": tick, "raw_words": [str(w) for w in words]}
        else:
            fields[name] = str(int(value, 16))
    pool["state"] = fields
confirm = result(call_batch("anchor-recheck", [("eth_getBlockByNumber", [block["number"], False])])[0])
assert confirm["hash"] == block["hash"], "Anchor reorged during discovery"
summary = {"chain_id": 1, "block_number": int(block["number"], 16), "block_hash": block["hash"],
           "block_time_utc": datetime.fromtimestamp(int(block["timestamp"], 16), timezone.utc).isoformat(),
           "finality_at_request": "latest/unfinalized", "factory": FACTORY, "tokens": TOKENS, "pools": pools}
(ROOT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
