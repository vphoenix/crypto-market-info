"""Bounded unauthenticated public combo census (3 catalogs and at most 2 books).

Run only with --fetch. Does not create combos or RFQs. Raw response bytes,
timestamps and hashes are retained; Decimal preserves financial numbers.
"""
import argparse
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


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def fetch():
    manifest, summary, selected = [], {"currencies": {}, "books": []}, []

    def get(name, method, params):
        url = "https://www.deribit.com/api/v2/public/" + method + "?" + urllib.parse.urlencode(params)
        entry = {"name": name, "url": url, "request_started_at": now()}
        started = time.monotonic_ns()
        payload = None
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "public-options-research/1.0"})
            with urllib.request.urlopen(req, timeout=20) as response:
                payload = response.read()
                entry.update(http_status=response.status, server_date=response.headers.get("Date"))
        except urllib.error.HTTPError as exc:
            payload = exc.read()
            entry.update(http_status=exc.code, error=str(exc))
        except (urllib.error.URLError, TimeoutError) as exc:
            entry["error"] = str(exc)
        entry.update(response_received_at=now(), elapsed_ns=time.monotonic_ns() - started)
        result = None
        if payload is not None:
            filename = name + ".raw.json"
            (ROOT / filename).write_bytes(payload)
            entry.update(path=filename, sha256=hashlib.sha256(payload).hexdigest(), bytes=len(payload))
            if entry.get("http_status") == 200:
                parsed = json.loads(payload, parse_float=Decimal)
                result = parsed.get("result")
                if "error" in parsed:
                    entry["api_error"] = parsed["error"]
        manifest.append(entry)
        print(json.dumps(entry), flush=True)
        time.sleep(2)
        return result

    for currency in ("BTC", "ETH", "USDC"):
        rows = get("deribit-" + currency + "-combos", "get_combos", {"currency": currency})
        if rows is None:
            summary["currencies"][currency] = {"available": False}
            continue
        option_combos = [row for row in rows if any(
            leg["instrument_name"].endswith(("-C", "-P")) for leg in row.get("legs", []))]
        summary["currencies"][currency] = {
            "available": True, "total": len(rows), "option_combos": len(option_combos),
            "option_combo_ids": [row["id"] for row in option_combos],
            "note": "Catalog is active combos only; count does not imply quotes or depth.",
        }
        if option_combos and len(selected) < 2:
            selected.append(option_combos[0]["id"])
    for index, combo in enumerate(selected):
        book = get("deribit-combo-book-" + str(index + 1), "get_order_book", {"instrument_name": combo, "depth": 10})
        if book is None:
            continue
        summary["books"].append({
            "instrument_name": combo, "source_timestamp": book.get("timestamp"),
            "bid_levels": len(book.get("bids", [])), "ask_levels": len(book.get("asks", [])),
            "best_bid_price": book.get("best_bid_price"), "best_bid_amount": book.get("best_bid_amount"),
            "best_ask_price": book.get("best_ask_price"), "best_ask_amount": book.get("best_ask_amount"),
            "note": "First listed option combo per available currency; selection not representative; independent REST snapshot.",
        })
    (ROOT / "combo-probe-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (ROOT / "combo-probe-summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fetch", action="store_true", required=True)
    parser.parse_args()
    fetch()
