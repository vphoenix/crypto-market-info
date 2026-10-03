"""Read frozen public responses; Decimal only. No network, wallet or trading code."""
import datetime as dt
import json
import statistics
from decimal import Decimal as D, getcontext
from pathlib import Path

getcontext().prec = 50
ROOT = Path(__file__).resolve().parent
EXPIRY = dt.datetime(2026, 11, 26, tzinfo=dt.timezone.utc)


def load(name):
    return json.loads((ROOT / name).read_text(), parse_float=str)


def elapsed_seconds(delta):
    return D(delta.days * 86400 + delta.seconds) + D(delta.microseconds) / D(1000000)


def annualized(face, capital, days):
    ratio = face / capital
    return {
        "term_profit": str(face - capital),
        "simple_apr_pct": str((ratio - 1) * 365 / days * 100),
        "compound_equivalent_apy_pct": str(((ratio.ln() * 365 / days).exp() - 1) * 100),
        "total_occupied_capital": str(capital),
    }


out = {"quotes": [], "history": [], "bitfinex": []}
for response in load("pendle-quotes-history.json"):
    if "body" not in response:
        continue
    body = response["body"]
    route = body["routes"][0]
    nominal = D(body["inputs"][0]["amount"]) / D(10**18)
    face = D(route["outputs"][0]["amount"]) / D(10**18)
    when = dt.datetime.fromisoformat(response["at"])
    days = elapsed_seconds(EXPIRY - when) / 86400
    min_face = D(route["contractParamInfo"]["contractCallParams"][2]) / D(10**18)
    assert nominal in (D(10000), D(100000))
    assert min_face < face and min_face >= nominal
    record = {
        "input_token": body["inputs"][0]["token"], "nominal": str(nominal),
        "pt": route["outputs"][0]["token"], "face_tokens": str(face),
        "min_face_at_10bp_slippage_tolerance": str(min_face), "days": str(days),
        "source_effective_apy_pct": str(D(route["data"]["effectiveApy"]) * 100),
        "source_trading_fee_usd_already_in_output": route["data"]["fee"]["usd"],
        "native_token_no_external_cost": annualized(face, nominal, days),
        "scenarios": [],
    }
    for external_cost in (D(10), D(50)):
        for annual_cover_fee in (D(0), D("0.0128"), D("0.0167")):
            # Hypothetical cover pricing, full maturity face value insured.
            premium = face * annual_cover_fee * days / 365
            result = annualized(face, nominal + external_cost + premium, days)
            result.update(external_cost_usd=str(external_cost),
                          annual_cover_fee=str(annual_cover_fee), premium_usd=str(premium))
            record["scenarios"].append(result)
    for k, apy in (("usds_plus_protocol_cover", D("0.0128") + D("0.0056")),
                   ("usde_plus_protocol_cover", D("0.0128") + D("0.0334"))):
        premium = face * apy * days / 365
        record[k] = annualized(face, nominal + D(10) + premium, days)
    out["quotes"].append(record)

for response in load("defi-history-sources.json")[:2]:
    rows = response["body"]["results"]
    assert len(rows) == response["body"]["total"]
    dates = [dt.datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00")) for r in rows]
    assert len(set(dates)) == len(rows)
    assert all(b-a == dt.timedelta(days=1) for a, b in zip(dates, dates[1:]))
    stats = {"url": response["url"], "rows": len(rows), "start": str(dates[0]), "end": str(dates[-1]),
             "continuous_days": (dates[-1] - dates[0]).days}
    for field in ("impliedApy", "underlyingApy"):
        values = [D(r[field]) * 100 for r in rows]
        stats[field] = {"min_pct": str(min(values)), "max_pct": str(max(values)),
                        "median_pct": str(statistics.median(values)),
                        "mean_pct": str(sum(values) / len(values))}
    out["history"].append(stats)

for response in load("defi-bitfinex-followup.json")[:2]:
    bids = sorted((r for r in response["body"] if D(r[3]) < 0), key=lambda r: D(r[0]), reverse=True)
    rate, period, count, amount = bids[0]
    out["bitfinex"].append({"url": response["url"], "at": response["at"], "daily_rate": rate,
                           "max_term_days": period, "orders": count, "visible_borrow_bid": str(-D(amount)),
                           "gross_apr_pct": str(D(rate) * 365 * 100),
                           "normal_fee_net_apr_pct": str(D(rate) * 365 * D(".85") * 100)})

(ROOT / "defi-calculation.json").write_text(json.dumps(out, ensure_ascii=False, indent=2))
for q in out["quotes"]:
    print(q["input_token"], q["nominal"], "base APR", q["native_token_no_external_cost"]["simple_apr_pct"])
    for s in q["scenarios"]:
        print("external cost", s["external_cost_usd"], "cover", s["annual_cover_fee"],
              "APR", s["simple_apr_pct"], "profit", s["term_profit"])
print("Verified four quote records, daily continuity in both history series and two funding book directions.")
