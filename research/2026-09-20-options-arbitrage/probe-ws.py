"""40-second unauthenticated Deribit feed probe; raw evidence, no trade signals.

Uses the existing REST census to choose ATM BTC/ETH call-put-future triples.
Requires the already-installed websockets package. No production state is touched.
"""
import asyncio
import collections
import datetime as dt
from decimal import Decimal as D
import hashlib
import json
from pathlib import Path
import time
import websockets

ROOT = Path(__file__).resolve().parent


def read(name):
    return json.loads((ROOT / (name + ".raw.json")).read_text(), parse_float=D)["result"]


def select():
    now_ms = time.time_ns() // 1_000_000
    names = []
    for currency, bases in (("BTC", ("BTC",)), ("ETH", ("ETH",)), ("USDC", ("BTC", "ETH"))):
        meta = read(f"deribit-{currency}-option-instruments")
        summaries = {x["instrument_name"]: x for x in read(f"deribit-{currency}-option-summaries")}
        futures = {x["instrument_name"] for x in read(f"deribit-{currency}-future-instruments")}
        for base in bases:
            chain = [x for x in meta if x["base_currency"] == base and
                     2 * 86400000 < x["expiration_timestamp"] - now_ms < 15 * 86400000]
            expiry = min(x["expiration_timestamp"] for x in chain)
            chain = [x for x in chain if x["expiration_timestamp"] == expiry]
            reference = D(summaries[chain[0]["instrument_name"]]["underlying_price"])
            strike = min({D(x["strike"]) for x in chain}, key=lambda k: abs(k - reference))
            pair = [x["instrument_name"] for x in chain if D(x["strike"]) == strike]
            if len(pair) != 2:
                raise ValueError("expected complete call/put pair")
            future = summaries[pair[0]]["underlying_index"]
            if future not in futures:
                raise ValueError("summary underlying is not a listed future")
            names.extend(pair + [future])
    return names


async def main():
    instruments = select()
    channels = [f"book.{name}.100ms" for name in instruments]
    quote_channel = f"quote.{instruments[0]}"
    channels.append(quote_channel)
    summary = {"url": "wss://www.deribit.com/ws/api/v2", "duration_requested_seconds": 40,
               "started_at": dt.datetime.now(dt.timezone.utc).isoformat(), "channels": channels,
               "subscription_ack": False, "errors": [], "books": {}}
    previous = {}
    state = {}
    counters = collections.defaultdict(collections.Counter)
    output = ROOT / "deribit-ws-40s.jsonl"
    with output.open("w") as log:
        async with websockets.connect(summary["url"], open_timeout=15, close_timeout=3,
                                      max_size=8 * 1024 * 1024) as ws:
            await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "public/subscribe",
                                      "params": {"channels": channels}}))
            end = time.monotonic_ns() + 40_000_000_000
            while time.monotonic_ns() < end:
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=max(0.001, (end-time.monotonic_ns())/1e9))
                except asyncio.TimeoutError:
                    break
                # Preserve the exact market payload as a JSON string; no float round trip.
                log.write(json.dumps({"received_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                                      "received_monotonic_ns": time.monotonic_ns(), "payload": message}) + "\n")
                event = json.loads(message, parse_float=D)
                if "error" in event:
                    summary["errors"].append(event)
                if event.get("id") == 1 and "result" in event:
                    summary["subscription_ack"] = set(event["result"]) == set(channels)
                if event.get("method") != "subscription":
                    continue
                channel, data = event["params"]["channel"], event["params"]["data"]
                counters[channel]["messages"] += 1
                if not channel.startswith("book."):
                    continue
                if data["type"] == "snapshot":
                    counters[channel]["snapshots"] += 1
                    state[channel] = {"bids": {}, "asks": {}}
                elif data.get("prev_change_id") != previous.get(channel):
                    counters[channel]["sequence_gaps"] += 1
                    state.pop(channel, None)
                previous[channel] = data["change_id"]
                if channel not in state:
                    continue
                for side in ("bids", "asks"):
                    for operation, price, amount in data[side]:
                        if operation == "delete" or amount == 0:
                            state[channel][side].pop(price, None)
                        else:
                            state[channel][side][price] = amount
                book = state[channel]
                if book["bids"] and book["asks"] and max(book["bids"]) >= min(book["asks"]):
                    counters[channel]["crossed_states"] += 1
    summary["ended_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    summary["counts"] = dict(counters)
    for channel, book in state.items():
        summary["books"][channel] = {"bid_levels": len(book["bids"]), "ask_levels": len(book["asks"]),
                                     "best_bid": max(book["bids"], default=None),
                                     "best_ask": min(book["asks"], default=None)}
    summary["raw_sha256"] = hashlib.sha256(output.read_bytes()).hexdigest()
    summary["raw_bytes"] = output.stat().st_size
    (ROOT / "ws-summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(main())
