"""Bounded public REST census, not an executable-arbitrage scanner.

Run explicitly with --fetch; otherwise validate/analyse the saved byte payloads.
All market decimals are decoded directly into Decimal. No credentials are used.
"""
import argparse
import collections
import datetime as dt
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parent
D = Decimal


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def endpoints():
    for currency in ("BTC", "ETH", "USDC"):
        for kind in ("option", "future"):
            for method, label in (("get_instruments", "instruments"),
                                  ("get_book_summary_by_currency", "summaries")):
                query = urllib.parse.urlencode({"currency": currency, "kind": kind})
                yield f"deribit-{currency}-{kind}-{label}", (
                    f"https://www.deribit.com/api/v2/public/{method}?{query}")
    for asset in ("BTC", "ETH"):
        query = urllib.parse.urlencode({"instType": "OPTION", "uly": f"{asset}-USD"})
        for path, label in (("public/instruments", "instruments"), ("market/tickers", "summaries")):
            yield f"okx-{asset}-option-{label}", f"https://www.okx.com/api/v5/{path}?{query}"


def fetch():
    manifest = []
    for name, url in endpoints():
        entry = {"name": name, "url": url, "request_started_at": utc_now()}
        start = time.monotonic_ns()
        payload = None
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "public-options-research/1.0"})
            with urllib.request.urlopen(request, timeout=20) as response:
                payload = response.read()
                entry.update(http_status=response.status, server_date=response.headers.get("Date"))
        except urllib.error.HTTPError as error:
            entry.update(http_status=error.code, error=str(error))
            payload = error.read()
        except (urllib.error.URLError, TimeoutError) as error:
            entry["error"] = str(error)
        entry.update(response_received_at=utc_now(), elapsed_ns=time.monotonic_ns() - start)
        if payload is not None:
            path = ROOT / f"{name}.raw.json"
            path.write_bytes(payload)
            entry.update(path=path.name, sha256=hashlib.sha256(payload).hexdigest(), bytes=len(payload))
        manifest.append(entry)
        print(json.dumps(entry), flush=True)
        # Seed calls stay comfortably below the catalog's 1 request/sec budget.
        time.sleep(2)
    (ROOT / "probe-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def analyse():
    manifest = json.loads((ROOT / "probe-manifest.json").read_text())
    data, errors = {}, []
    for entry in manifest:
        if entry.get("http_status") != 200:
            errors.append(entry)
            continue
        payload = (ROOT / entry["path"]).read_bytes()
        if hashlib.sha256(payload).hexdigest() != entry["sha256"]:
            raise ValueError(f"payload hash mismatch: {entry['name']}")
        response = json.loads(payload, parse_float=D)
        if response.get("error") or response.get("code", "0") != "0":
            errors.append({"name": entry["name"], "response": response})
            continue
        data[entry["name"]] = response.get("result", response.get("data"))
    result = {"scope": "Independent REST responses; not synchronized executable quotes", "errors": errors, "groups": []}
    for entry in manifest:
        name = entry["name"]
        if "option-instruments" not in name or name not in data:
            continue
        summaries = data.get(name.replace("instruments", "summaries"))
        if summaries is None:
            continue
        deribit = name.startswith("deribit-")
        key = "instrument_name" if deribit else "instId"
        meta = {item[key]: item for item in data[name]}
        rows = {item[key]: item for item in summaries}
        now = int(dt.datetime.fromisoformat(entry["response_received_at"]).timestamp()) * 1000
        groups = collections.defaultdict(lambda: collections.Counter())
        for symbol, instrument in meta.items():
            base = instrument["base_currency"] if deribit else instrument["uly"].split("-")[0]
            group = groups[base]
            group["listed"] += 1
            row = rows.get(symbol)
            if row is None:
                group["missing_summary"] += 1
                continue
            group["summary_present"] += 1
            bid = D(str(row.get("bid_price" if deribit else "bidPx") or "0"))
            ask = D(str(row.get("ask_price" if deribit else "askPx") or "0"))
            if bid > 0 and ask > bid:
                group["positive_uncrossed_two_sided_price"] += 1
                if not deribit and (D(row["bidSz"]) <= 0 or D(row["askSz"]) <= 0):
                    group["two_sided_price_but_zero_size"] += 1
                expiry = int(instrument["expiration_timestamp" if deribit else "expTime"])
                days = D(expiry - now) / D(86400000)
                bucket = "expired" if days <= 0 else "0_2d" if days < 2 else "2_45d" if days <= 45 else "over_45d"
                group[f"two_sided_{bucket}"] += 1
            elif bid > 0 and ask > 0:
                group["locked_or_crossed"] += 1
            else:
                group["missing_positive_side"] += 1
        result["groups"].append({"source": name, "assets": dict(groups),
                                  "unmatched_summary_symbols": len(rows.keys() - meta.keys()),
                                  "note": "Deribit summary has no quoted sizes; price presence is not liquidity proof." if deribit else "Ticker size is contracts; normalize contract value."})
    (ROOT / "coverage-summary.json").write_text(json.dumps(result, indent=2, default=str) + "\n")
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fetch", action="store_true")
    args = parser.parse_args()
    if args.fetch:
        fetch()
    analyse()
