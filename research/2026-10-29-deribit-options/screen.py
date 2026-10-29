"""Deribit option-chain static arbitrage screen over exported one-hour windows (second-0 books).

All edges are exact integers: prices/quantities are the stored 1e-8 raw units, strikes are scaled to
integers. Every leg trades at its real side (buy at ask, sell at bid). Futures are the same-expiry,
same-family Deribit futures (inverse futures hedge K USD of notional per coin as in 09-21).

Checks per family (BTC/ETH inverse, BTC/ETH USDC linear) and expiry:
  bound      single option vs. its intrinsic value through the same-expiry future (2 legs)
  parity     conversion/reversal: call, put, future (3 legs)
  vertical   all strike pairs: monotonic credit and vertical-overpay bounds (2 legs)
  butterfly  every strike triple with wings at most 8 listed strikes apart (3 legs)
  calendar   inverse calls, same strike, near bid > far ask while far future >= near future (2 legs; not locked)
  box        all strike pairs, every 5th minute: best executable lending/borrowing rate among boxes whose
             top-of-book capacity meets the minimum order (4 option legs [+ future])
  cross      inverse vs USDC option, same base/expiry/strike/type (2 option legs + inverse future)

Fees (project notes 09-20/09-21; live page refused automated access on 2026-10-29): option taker
min(3 bp of underlying, 12.5% of premium) per leg, futures taker 5 bp of notional. Delivery fees,
margin, financing and slippage beyond the top level are not deducted; they only make results worse.
"""
import collections
import datetime as dt
import gzip
import json
import sys
from decimal import Decimal as D, getcontext
from fractions import Fraction as Q
from pathlib import Path

getcontext().prec = 50
OUT = Path(__file__).resolve().parent
E8 = 10 ** 8
SK = 100  # strike scale
OPT_FEE, OPT_CAP, FUT_FEE = Q(3, 10000), Q(1, 8), Q(5, 10000)
MIN_SIZE = {'BTC': Q(1, 10), 'ETH': Q(1)}  # Deribit option minimum trade amount, coin
WING = 8

inst = {r['instrument_id']: r for r in map(json.loads, gzip.open(OUT / 'meta-instruments.jsonl.gz'))}
spec = {r['instrument_id']: r for r in map(json.loads, gzip.open(OUT / 'meta-specs.jsonl.gz'))}


def ints(h, signed):
    raw = bytes.fromhex(h)
    assert raw[0] == 1
    n = int.from_bytes(raw[17:21], 'big')
    return [int.from_bytes(raw[21 + 8 * j:29 + 8 * j], 'big', signed=signed) for j in range(n)]


def load_window(d):
    """{minute: {id: (bids[(p,q)], asks[(p,q)])}} for rows whose second 0 is valid; duplicates must agree."""
    books, stats = collections.defaultdict(dict), collections.Counter()
    seen = {}
    for f in sorted(d.glob('books-*.jsonl.gz')):
        for line in gzip.open(f):
            r = json.loads(line)
            stats['rows'] += 1
            key = (r['m'], r['id'])
            sig = (r['v'], r['bp'], r['bq'], r['ap'], r['aq'])
            if key in seen:
                stats['duplicate_rows'] += 1
                if seen[key] != sig:
                    stats['conflicting_duplicates'] += 1
                    books[r['m']].pop(r['id'], None)
                    seen[key] = None
                continue
            seen[key] = sig
            if not r['v']:
                stats['second0_invalid'] += 1
                continue
            bids = list(zip(ints(r['bp'], True), ints(r['bq'], False)))
            asks = list(zip(ints(r['ap'], True), ints(r['aq'], False)))
            assert all(a[0] > b[0] for a, b in zip(bids, bids[1:])) and all(a[0] < b[0] for a, b in zip(asks, asks[1:]))
            if bids and asks and bids[0][0] >= asks[0][0]:
                stats['crossed_or_locked'] += 1
                continue
            books[r['m']][r['id']] = (bids, asks)
            stats['valid_books'] += 1
    return books, stats


def family(i):
    r = inst[i]
    return (r['base_asset'], 'inverse' if r['settle_asset'] == r['base_asset'] else 'linear')


def strike_int(i):
    k = Q(spec[i]['strike']) * SK
    assert k.denominator == 1
    return int(k)


def ofee(fam, price_raw, under_raw):
    """Option taker fee per coin, raw 1e-8 units of the premium currency (exact Fraction)."""
    if fam[1] == 'inverse':
        return min(OPT_FEE * E8, OPT_CAP * price_raw)
    return min(OPT_FEE * under_raw, OPT_CAP * price_raw)


class Screen:
    def __init__(self):
        self.count = collections.Counter()
        self.gross_pos = collections.Counter()
        self.net_pos = collections.Counter()
        self.best = {}
        self.cands = []
        self.box = collections.defaultdict(list)
        self.gross_list = []
        self.under = {}

    def note(self, kind, minute, fam, exp, gross, scale, fees, legs, cap):
        """gross/scale in raw premium units per coin (exact ints); fees and cap are callables, evaluated only if gross > 0."""
        self.count[kind] += 1
        b = self.best.get(kind)
        if b is None or gross * b[1] > b[0] * scale:
            self.best[kind] = (gross, scale, minute, fam, exp, legs())
        if gross <= 0:
            return
        self.gross_pos[kind] += 1
        g, f = Q(gross, scale), fees()
        net = g - f
        c = cap()
        if 'box' not in kind:
            # Zero-fee upper bound: every gross-positive top-of-book observation, valued in USD(C).
            usd = Q(1) if fam[1] == 'linear' else Q(self.under[fam[0]], E8)
            self.gross_list.append({'kind': kind, 'minute': minute, 'family': fam, 'expiry': exp, 'legs': legs(),
                                    'gross_usd_per_coin': D((g * usd / E8).numerator) / (g * usd / E8).denominator,
                                    'entry_fees_usd_per_coin': D((f * usd / E8).numerator) / (f * usd / E8).denominator,
                                    'top_level_capacity_coin': D(c.numerator) / c.denominator})
        if net <= 0:
            return
        self.net_pos[kind] += 1
        self.cands.append({'kind': kind, 'minute': minute, 'family': fam, 'expiry': exp, 'legs': legs(),
                           'gross_per_coin': D(g.numerator) / g.denominator / E8,
                           'entry_fees_per_coin': D(f.numerator) / f.denominator / E8,
                           'net_per_coin': D(net.numerator) / net.denominator / E8,
                           'premium_currency': fam[0] if fam[1] == 'inverse' else 'USD(C)',
                           'top_level_capacity_coin': None if c is None else D(c.numerator) / c.denominator,
                           'meets_min_size': None if c is None else c >= MIN_SIZE[fam[0]]})


def sym(i):
    return inst[i]['exchange_symbol']


def run_minute(sc, minute, books, box_minute):
    t = dt.datetime.fromisoformat(minute)
    chains = collections.defaultdict(lambda: {'C': {}, 'P': {}})
    futs = {}
    for i, (bids, asks) in books.items():
        r = inst.get(i)
        if r is None or i not in spec or r['exchange'] != 'Deribit':
            continue
        key = (family(i), r['expiry_time'][:19])
        if r['market_type'] == 'delivery':
            if bids and asks:
                futs[key] = (bids, asks, i)
        elif r['market_type'] == 'option':
            chains[key]['C' if spec[i]['option_type'] == 'call' else 'P'][strike_int(i)] = (bids, asks, i)
    # Underlying proxy for linear fee caps: the nearest-expiry future mid of the same base.
    under = {}
    for (fam, exp), (b, a, _) in sorted(futs.items(), key=lambda x: x[0][1]):
        under.setdefault(fam[0], (b[0][0] + a[0][0]) // 2)
    sc.under = under
    fee_cache = {}

    def fee(fam, i, side, price, u):
        key = (i, side)
        if key not in fee_cache:
            fee_cache[key] = ofee(fam, price, u)
        return fee_cache[key]

    def q(level):
        return Q(level[1], E8)

    for (fam, exp), ch in chains.items():
        u = under.get(fam[0])
        if u is None:
            continue
        fut = futs.get((fam, exp))
        lin = fam[1] == 'linear'
        years = Q(int((dt.datetime.fromisoformat(exp) - t).total_seconds()), 365 * 86400)
        if years <= 0:
            continue
        # ---- single-option bounds and parity, via the same-expiry future
        if fut:
            fb, fa, fid = fut[0][0][0], fut[1][0][0], fut[2]  # noqa: F841 (fb reused below)
            fbq, faq = fut[0][0][1], fut[1][0][1]
            for k, (cb, ca, ci) in ch['C'].items():
                k8 = k * E8 // SK
                if ca:  # buy call, short future: linear payoff >= F_bid-K; inverse >= 1-K/F_bid coin
                    if lin:
                        sc.note('bound_call_below_intrinsic', minute, fam, exp, fb - k8 - ca[0][0], 1,
                                lambda: fee(fam, ci, 'a', ca[0][0], u) + FUT_FEE * fb, lambda: [sym(ci), sym(fid)],
                                lambda: min(q(ca[0]), Q(fbq, E8)))
                    else:
                        sc.note('bound_call_below_intrinsic', minute, fam, exp, (fb - k8) * E8 - ca[0][0] * fb, fb,
                                lambda: fee(fam, ci, 'a', ca[0][0], u) + FUT_FEE * Q(k8 * E8, fb), lambda: [sym(ci), sym(fid)],
                                lambda: min(q(ca[0]), Q(fbq, k8)))
                if not lin and cb:
                    sc.note('bound_inverse_call_above_one', minute, fam, exp, cb[0][0] - E8, 1,
                            lambda: fee(fam, ci, 'b', cb[0][0], u), lambda: [sym(ci)], lambda: q(cb[0]))
            for k, (pb, pa, pi) in ch['P'].items():
                k8 = k * E8 // SK
                if pa:  # buy put, long future
                    if lin:
                        sc.note('bound_put_below_intrinsic', minute, fam, exp, k8 - fa - pa[0][0], 1,
                                lambda: fee(fam, pi, 'a', pa[0][0], u) + FUT_FEE * fa, lambda: [sym(pi), sym(fid)],
                                lambda: min(q(pa[0]), Q(faq, E8)))
                    else:
                        sc.note('bound_put_below_intrinsic', minute, fam, exp, (k8 - fa) * E8 - pa[0][0] * fa, fa,
                                lambda: fee(fam, pi, 'a', pa[0][0], u) + FUT_FEE * Q(k8 * E8, fa), lambda: [sym(pi), sym(fid)],
                                lambda: min(q(pa[0]), Q(faq, k8)))
            for k in ch['C'].keys() & ch['P'].keys():
                (cb, ca, ci), (pb, pa, pi) = ch['C'][k], ch['P'][k]
                k8 = k * E8 // SK
                legs = lambda: [sym(ci), sym(pi), sym(fid)]
                if ca and pb:  # conversion: buy call, sell put, short future
                    if lin:
                        sc.note('parity_conversion', minute, fam, exp, fb - k8 - ca[0][0] + pb[0][0], 1,
                                lambda: fee(fam, ci, 'a', ca[0][0], u) + fee(fam, pi, 'b', pb[0][0], u) + FUT_FEE * fb, legs,
                                lambda: min(q(ca[0]), q(pb[0]), Q(fbq, E8)))
                    else:
                        sc.note('parity_conversion', minute, fam, exp, (fb - k8) * E8 - (ca[0][0] - pb[0][0]) * fb, fb,
                                lambda: fee(fam, ci, 'a', ca[0][0], u) + fee(fam, pi, 'b', pb[0][0], u) + FUT_FEE * Q(k8 * E8, fb), legs,
                                lambda: min(q(ca[0]), q(pb[0]), Q(fbq, k8)))
                if cb and pa:  # reversal: sell call, buy put, long future
                    if lin:
                        sc.note('parity_reversal', minute, fam, exp, k8 - fa + cb[0][0] - pa[0][0], 1,
                                lambda: fee(fam, ci, 'b', cb[0][0], u) + fee(fam, pi, 'a', pa[0][0], u) + FUT_FEE * fa, legs,
                                lambda: min(q(cb[0]), q(pa[0]), Q(faq, E8)))
                    else:
                        sc.note('parity_reversal', minute, fam, exp, (k8 - fa) * E8 + (cb[0][0] - pa[0][0]) * fa, fa,
                                lambda: fee(fam, ci, 'b', cb[0][0], u) + fee(fam, pi, 'a', pa[0][0], u) + FUT_FEE * Q(k8 * E8, fa), legs,
                                lambda: min(q(cb[0]), q(pa[0]), Q(faq, k8)))
        # ---- verticals (all strike pairs) and butterflies (no future needed)
        for typ in 'CP':
            curve = sorted(ch[typ].items())
            for x in range(len(curve)):
                k1, (b1, a1, i1) = curve[x]
                for y in range(x + 1, len(curve)):
                    k2, (b2, a2, i2) = curve[y]
                    legs = lambda: [sym(i1), sym(i2)]
                    if typ == 'C':
                        if a1 and b2:  # buy K1 call, sell K2 call for a credit
                            sc.note('vertical_call_monotonic', minute, fam, exp, b2[0][0] - a1[0][0], 1,
                                    lambda: fee(fam, i1, 'a', a1[0][0], u) + fee(fam, i2, 'b', b2[0][0], u), legs,
                                    lambda: min(q(a1[0]), q(b2[0])))
                        if b1 and a2:  # sell K1 call, buy K2 call: credit above the maximum payoff
                            gross, scale = ((b1[0][0] - a2[0][0] - (k2 - k1) * E8 // SK, 1) if lin else
                                            ((b1[0][0] - a2[0][0]) * k2 - (k2 - k1) * E8, k2))
                            sc.note('vertical_call_overpay', minute, fam, exp, gross, scale,
                                    lambda: fee(fam, i1, 'b', b1[0][0], u) + fee(fam, i2, 'a', a2[0][0], u), legs,
                                    lambda: min(q(b1[0]), q(a2[0])))
                    else:
                        if a2 and b1:  # buy K2 put, sell K1 put for a credit
                            sc.note('vertical_put_monotonic', minute, fam, exp, b1[0][0] - a2[0][0], 1,
                                    lambda: fee(fam, i2, 'a', a2[0][0], u) + fee(fam, i1, 'b', b1[0][0], u), legs,
                                    lambda: min(q(a2[0]), q(b1[0])))
                        if b2 and a1 and (lin or fut):  # sell K2 put, buy K1 put: credit above the maximum payoff
                            # Inverse put-spread coin payoff (K2-K1)/X is unbounded as X falls; only the USD
                            # bound K2-K1 holds, so the coin credit is sold forward at the same-expiry future bid.
                            if lin:
                                sc.note('vertical_put_overpay', minute, fam, exp, b2[0][0] - a1[0][0] - (k2 - k1) * E8 // SK, 1,
                                        lambda: fee(fam, i2, 'b', b2[0][0], u) + fee(fam, i1, 'a', a1[0][0], u), legs,
                                        lambda: min(q(b2[0]), q(a1[0])))
                            else:
                                fb = fut[0][0][0]
                                sc.note('vertical_put_overpay', minute, fam, exp, (b2[0][0] - a1[0][0]) * fb - (k2 - k1) * E8 * E8 // SK, fb,
                                        lambda: fee(fam, i2, 'b', b2[0][0], u) + fee(fam, i1, 'a', a1[0][0], u) + FUT_FEE * (b2[0][0] - a1[0][0]), legs,
                                        lambda: min(q(b2[0]), q(a1[0])))
            for x in range(len(curve)):
                ka, (ba, aa, ia) = curve[x]
                if not aa:
                    continue
                for z in range(x + 2, min(len(curve), x + WING + 1)):
                    kc, (bc, ac, ic) = curve[z]
                    if not ac:
                        continue
                    for y in range(x + 1, z):
                        kb, (bb, ab, ib) = curve[y]
                        if not bb:
                            continue
                        wa, wc, wt = kc - kb, kb - ka, kc - ka
                        sc.note(('call' if typ == 'C' else 'put') + '_butterfly', minute, fam, exp,
                                wt * bb[0][0] - wa * aa[0][0] - wc * ac[0][0], wt,
                                lambda: Q(wa, wt) * fee(fam, ia, 'a', aa[0][0], u) + fee(fam, ib, 'b', bb[0][0], u) + Q(wc, wt) * fee(fam, ic, 'a', ac[0][0], u),
                                lambda: [sym(ia), sym(ib), sym(ic)],
                                lambda: min(Q(aa[0][1] * wt, wa * E8), q(bb[0]), Q(ac[0][1] * wt, wc * E8)))
        # ---- boxes every 5th minute: best executable lending (long box) and borrowing (short box) rate
        if box_minute and (fut or lin):
            strikes = sorted(ch['C'].keys() & ch['P'].keys())
            best_lend = best_borrow = None
            for x in range(len(strikes)):
                k1 = strikes[x]
                (c1b, c1a, c1), (p1b, p1a, p1) = ch['C'][k1], ch['P'][k1]
                for k2 in strikes[x + 1:]:
                    (c2b, c2a, c2), (p2b, p2a, p2) = ch['C'][k2], ch['P'][k2]
                    w8 = (k2 - k1) * E8 // SK
                    legs = lambda: [sym(c1), sym(c2), sym(p2), sym(p1)]
                    if c1a and c2b and p2a and p1b:
                        cost = (c1a[0][0] - c2b[0][0] + p2a[0][0] - p1b[0][0] + fee(fam, c1, 'a', c1a[0][0], u)
                                + fee(fam, c2, 'b', c2b[0][0], u) + fee(fam, p2, 'a', p2a[0][0], u) + fee(fam, p1, 'b', p1b[0][0], u))
                        pay = Q(w8) if lin else Q(w8 * E8, fut[1][0][0]) * (1 - FUT_FEE)
                        cap = min(q(c1a[0]), q(c2b[0]), q(p2a[0]), q(p1b[0]))
                        if cost <= 0:
                            sc.note('box_long_nonpositive_cost', minute, fam, exp, 1, 1, lambda: Q(0), legs, lambda: cap)
                        elif cap >= MIN_SIZE[fam[0]]:
                            rate = (pay / cost - 1) / years
                            if best_lend is None or rate > best_lend[0]:
                                best_lend = (rate, legs(), cap)
                    if c1b and c2a and p2b and p1a:
                        credit = (c1b[0][0] - c2a[0][0] + p2b[0][0] - p1a[0][0] - fee(fam, c1, 'b', c1b[0][0], u)
                                  - fee(fam, c2, 'a', c2a[0][0], u) - fee(fam, p2, 'b', p2b[0][0], u) - fee(fam, p1, 'a', p1a[0][0], u))
                        owe = Q(w8) if lin else Q(w8 * E8, fut[0][0][0]) * (1 + FUT_FEE)
                        if credit > owe:
                            sc.note('box_short_credit_above_debt', minute, fam, exp, 1, 1, lambda: Q(0), legs, lambda: cap)
                        cap = min(q(c1b[0]), q(c2a[0]), q(p2b[0]), q(p1a[0]))
                        if credit > 0 and cap >= MIN_SIZE[fam[0]]:
                            rate = (owe / credit - 1) / years
                            if best_borrow is None or rate < best_borrow[0]:
                                best_borrow = (rate, legs(), cap)
            if best_lend or best_borrow:
                def dec(x):
                    return None if x is None else D(x.numerator) / x.denominator
                sc.box[(fam, exp)].append({'minute': minute, 'days': dec(years * 365),
                                           'best_lend_rate': dec(best_lend and best_lend[0]), 'lend_legs': best_lend and best_lend[1],
                                           'lend_capacity_coin': dec(best_lend and best_lend[2]),
                                           'best_borrow_rate': dec(best_borrow and best_borrow[0]), 'borrow_legs': best_borrow and best_borrow[1],
                                           'borrow_capacity_coin': dec(best_borrow and best_borrow[2])})
    # ---- inverse calendar calls and inverse-vs-linear cross family
    for base in ('BTC', 'ETH'):
        ifam, lfam = (base, 'inverse'), (base, 'linear')
        inv = sorted((exp, ch) for (fam, exp), ch in chains.items() if fam == ifam)
        for x in range(len(inv)):
            for y in range(x + 1, len(inv)):
                (e1, ch1), (e2, ch2) = inv[x], inv[y]
                f1, f2 = futs.get((ifam, e1)), futs.get((ifam, e2))
                if not f1 or not f2 or f2[0][0][0] < f1[1][0][0]:
                    continue
                for k in ch1['C'].keys() & ch2['C'].keys():
                    (b1, a1, i1), (b2, a2, i2) = ch1['C'][k], ch2['C'][k]
                    if b1 and a2:
                        sc.note('calendar_inverse_call', minute, ifam, e1, b1[0][0] - a2[0][0], 1,
                                lambda: fee(ifam, i1, 'b', b1[0][0], 0) + fee(ifam, i2, 'a', a2[0][0], 0), lambda: [sym(i1), sym(i2)],
                                lambda: min(q(b1[0]), q(a2[0])))
        u = under.get(base)
        for (fam, exp), ich in chains.items():
            if fam != ifam or (lfam, exp) not in chains or (ifam, exp) not in futs:
                continue
            lch = chains[(lfam, exp)]
            fb, fa = futs[(ifam, exp)][0][0][0], futs[(ifam, exp)][1][0][0]
            for typ in 'CP':
                for k in ich[typ].keys() & lch[typ].keys():
                    (ib, ia, ii), (lb, la, li) = ich[typ][k], lch[typ][k]
                    legs = lambda: [sym(ii), sym(li)]
                    if ia and lb:  # buy inverse (coin premium valued at the future ask), sell linear (USDC)
                        sc.note('cross_buy_inverse_sell_linear', minute, lfam, exp, lb[0][0] * E8 - ia[0][0] * fa, E8,
                                lambda: fee(lfam, li, 'b', lb[0][0], u) + Q(fee(ifam, ii, 'a', ia[0][0], u) * fa, E8) + FUT_FEE * Q(ia[0][0] * fa, E8),
                                legs, lambda: min(q(ia[0]), q(lb[0])))
                    if ib and la:
                        sc.note('cross_sell_inverse_buy_linear', minute, lfam, exp, ib[0][0] * fb - la[0][0] * E8, E8,
                                lambda: fee(lfam, li, 'a', la[0][0], u) + Q(fee(ifam, ii, 'b', ib[0][0], u) * fb, E8) + FUT_FEE * Q(ib[0][0] * fb, E8),
                                legs, lambda: min(q(ib[0]), q(la[0])))


def main(windows):
    sc = Screen()
    quality = {}
    for w in windows:
        d = OUT / 'windows' / w
        books, stats = load_window(d)
        expected = len(json.loads((d / 'instruments.json').read_text()))
        quality[w] = dict(stats, expected_instruments=expected, minutes=len(books),
                          mean_valid_books_per_minute=round(stats['valid_books'] / max(len(books), 1), 1))
        for n, minute in enumerate(sorted(books)):
            run_minute(sc, minute, books[minute], n % 5 == 0)
        print(w, quality[w], 'candidates so far', len(sc.cands), flush=True)
    return sc, quality


if __name__ == '__main__':
    windows = sys.argv[1:] or sorted(p.name for p in (OUT / 'windows').iterdir() if (p / 'instruments.json').exists())
    sc, quality = main(windows)
    best = {k: {'best_gross_per_coin': str(D(v[0]) / v[1] / E8), 'minute': v[2], 'family': v[3], 'expiry': v[4], 'legs': v[5]}
            for k, v in sc.best.items()}
    out = {'method': __doc__, 'windows': windows, 'quality': quality, 'comparisons': dict(sc.count),
           'gross_positive': dict(sc.gross_pos), 'net_positive_after_entry_fees': dict(sc.net_pos), 'closest_to_profit': best,
           'candidates': sc.cands, 'gross_positive_observations': sc.gross_list, 'box_rates': {f'{k[0][0]}-{k[0][1]} {k[1]}': v for k, v in sc.box.items()}}
    (OUT / 'screen.json').write_text(json.dumps(out, default=str, indent=1) + '\n')
    print(json.dumps({k: out[k] for k in ('comparisons', 'gross_positive', 'net_positive_after_entry_fees')}, indent=1))
