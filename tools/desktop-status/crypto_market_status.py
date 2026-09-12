#!/usr/bin/python3
"""Lightweight Ubuntu status indicator for crypto-market-info.

The indicator is intentionally read-only. It checks the systemd user units and
two bounded ClickHouse HTTP queries every 30 seconds by default.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import pathlib
import shlex
import subprocess
import sys
import threading
import urllib.parse
import urllib.request
from typing import Any


COLLECTOR_UNIT = "crypto-market-info-collector.service"
SOAK_UNIT = "crypto-market-info-perp-soak.service"
EXPECTED_STREAM_COUNT = 5

BOOK_QUERY = r"""
SELECT
    i.exchange,
    i.market_type,
    i.exchange_symbol,
    b.latest_minute,
    ifNull(dateDiff('second', b.latest_minute, now('UTC')), 999999) AS age_seconds,
    ifNull(b.valid_seconds, 0) AS valid_seconds
FROM
(
    SELECT exchange, market_type, exchange_symbol, max(instrument_id) AS instrument_id
    FROM instrument FINAL
    WHERE (exchange = 'Binance' AND exchange_symbol = 'BTCUSDT')
       OR (exchange = 'OKX' AND exchange_symbol IN ('BTC-USDT', 'BTC-USDT-SWAP'))
       OR (exchange = 'Bybit' AND exchange_symbol = 'BTCUSDT')
    GROUP BY exchange, market_type, exchange_symbol
) AS i
LEFT JOIN
(
    SELECT
        instrument_id,
        max(minute_time) AS latest_minute,
        bitCount(argMax(valid_bitmap, minute_time)) AS valid_seconds
    FROM order_book_minute FINAL
    WHERE minute_time >= now('UTC') - INTERVAL 10 MINUTE
    GROUP BY instrument_id
) AS b USING (instrument_id)
ORDER BY i.exchange, i.market_type
"""

YIELD_QUERY = r"""
SELECT
    max(collected_at) AS latest_collected,
    dateDiff('second', max(collected_at), now('UTC')) AS age_seconds
FROM yield_observation FINAL
WHERE collected_at >= now('UTC') - INTERVAL 8 HOUR
"""


@dataclasses.dataclass(frozen=True)
class StreamHealth:
    exchange: str
    market_type: str
    symbol: str
    latest_minute: str
    age_seconds: int
    valid_seconds: int


@dataclasses.dataclass(frozen=True)
class HealthSnapshot:
    state: str
    summary: str
    collector_active: bool
    soak_active: bool
    clickhouse_ok: bool
    streams: tuple[StreamHealth, ...]
    yield_age_seconds: int | None
    checked_at: dt.datetime
    errors: tuple[str, ...]

    def text(self) -> str:
        lines = [
            f"状态: {self.summary}",
            f"ClickHouse: {'正常' if self.clickhouse_ok else '异常'}",
            f"生产 collector: {'运行中' if self.collector_active else '未运行'}",
            f"永续验收: {'运行中' if self.soak_active else '未运行'}",
        ]
        for stream in self.streams:
            lines.append(
                f"{stream.exchange} {market_name(stream.market_type)} {stream.symbol}: "
                f"{stream.valid_seconds}/60, {stream.age_seconds} 秒前"
            )
        if self.yield_age_seconds is not None:
            lines.append(f"最近收益写入: {format_age(self.yield_age_seconds)}前")
        lines.extend(f"错误: {error}" for error in self.errors)
        lines.append(f"检查时间: {self.checked_at.astimezone():%Y-%m-%d %H:%M:%S}")
        return "\n".join(lines)


def market_name(market_type: str) -> str:
    return {"spot": "现货", "perpetual": "永续"}.get(market_type, market_type)


def format_age(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds} 秒"
    if seconds < 3600:
        return f"{seconds // 60} 分钟"
    return f"{seconds // 3600} 小时 {seconds % 3600 // 60} 分钟"


def local_minute(value: str) -> str:
    if not value:
        return "无"
    try:
        parsed = dt.datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=dt.timezone.utc
        )
        return parsed.astimezone().strftime("%m-%d %H:%M")
    except ValueError:
        return value


def fetch_clickhouse_json(base_url: str, database: str, query: str) -> list[dict[str, Any]]:
    params = urllib.parse.urlencode({"database": database, "default_format": "JSON"})
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/?{params}",
        data=query.encode("utf-8"),
        headers={"Content-Type": "text/plain; charset=utf-8"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=4) as response:
        payload = json.load(response)
    data = payload.get("data")
    if not isinstance(data, list):
        raise ValueError("ClickHouse response does not contain a data array")
    return data


def read_unit_states(units: tuple[str, ...]) -> dict[str, str]:
    command = ["systemctl", "--user", "show", *units, "-p", "Id", "-p", "ActiveState"]
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=4,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or "systemctl --user show failed")

    result: dict[str, str] = {}
    current_id = ""
    for line in completed.stdout.splitlines():
        if line.startswith("Id="):
            current_id = line.removeprefix("Id=")
        elif line.startswith("ActiveState=") and current_id:
            result[current_id] = line.removeprefix("ActiveState=")
            current_id = ""
    return result


def classify(
    collector_active: bool,
    streams: tuple[StreamHealth, ...],
    clickhouse_ok: bool,
    yield_age_seconds: int | None,
    yield_ok: bool,
) -> tuple[str, str]:
    if not collector_active:
        return "down", "生产 collector 未运行"
    if not clickhouse_ok:
        return "down", "ClickHouse 或盘口查询异常"
    if len(streams) != EXPECTED_STREAM_COUNT:
        return "down", f"仅发现 {len(streams)}/{EXPECTED_STREAM_COUNT} 条生产行情流"
    if any(stream.age_seconds < 0 or stream.age_seconds > 240 for stream in streams):
        return "down", "盘口数据已停止更新"
    if any(stream.valid_seconds != 60 or stream.age_seconds > 150 for stream in streams):
        return "degraded", "盘口仍在写入，但存在延迟或无效秒"
    if not yield_ok:
        return "degraded", "盘口正常，收益表检查失败"
    if yield_age_seconds is None or yield_age_seconds > 7200:
        return "degraded", "盘口正常，收益数据超过 2 小时未更新"
    return "healthy", "生产采集正常"


def probe(base_url: str, database: str) -> HealthSnapshot:
    checked_at = dt.datetime.now(dt.timezone.utc)
    errors: list[str] = []

    try:
        unit_states = read_unit_states((COLLECTOR_UNIT, SOAK_UNIT))
    except Exception as exc:  # status must remain available even if systemd lookup fails
        unit_states = {}
        errors.append(f"systemd: {exc}")

    collector_active = unit_states.get(COLLECTOR_UNIT) == "active"
    soak_active = unit_states.get(SOAK_UNIT) == "active"

    streams: tuple[StreamHealth, ...] = ()
    clickhouse_ok = False
    try:
        rows = fetch_clickhouse_json(base_url, database, BOOK_QUERY)
        streams = tuple(
            StreamHealth(
                exchange=str(row["exchange"]),
                market_type=str(row["market_type"]),
                symbol=str(row["exchange_symbol"]),
                latest_minute=str(row.get("latest_minute") or ""),
                age_seconds=int(row["age_seconds"]),
                valid_seconds=int(row["valid_seconds"]),
            )
            for row in rows
        )
        clickhouse_ok = True
    except Exception as exc:
        errors.append(f"ClickHouse 盘口: {exc}")

    yield_age_seconds: int | None = None
    yield_ok = False
    if clickhouse_ok:
        try:
            rows = fetch_clickhouse_json(base_url, database, YIELD_QUERY)
            if rows and rows[0].get("latest_collected"):
                yield_age_seconds = int(rows[0]["age_seconds"])
                yield_ok = True
            else:
                errors.append("收益表在最近 8 小时内没有观测")
        except Exception as exc:
            errors.append(f"ClickHouse 收益: {exc}")

    state, summary = classify(
        collector_active,
        streams,
        clickhouse_ok,
        yield_age_seconds,
        yield_ok,
    )
    return HealthSnapshot(
        state=state,
        summary=summary,
        collector_active=collector_active,
        soak_active=soak_active,
        clickhouse_ok=clickhouse_ok,
        streams=streams,
        yield_age_seconds=yield_age_seconds,
        checked_at=checked_at,
        errors=tuple(errors),
    )


class IndicatorApplication:
    def __init__(self, args: argparse.Namespace) -> None:
        import gi

        gi.require_version("Gtk", "3.0")
        gi.require_version("AyatanaAppIndicator3", "0.1")
        from gi.repository import AyatanaAppIndicator3, GLib, Gtk

        self.AyatanaAppIndicator3 = AyatanaAppIndicator3
        self.GLib = GLib
        self.Gtk = Gtk
        self.args = args
        self.refreshing = False
        self.last_state: str | None = None
        self.icon_dir = pathlib.Path(__file__).resolve().parent / "icons"

        self.indicator = AyatanaAppIndicator3.Indicator.new(
            "crypto-market-info-status",
            str(self.icon_dir / "checking.svg"),
            AyatanaAppIndicator3.IndicatorCategory.SYSTEM_SERVICES,
        )
        self.indicator.set_status(AyatanaAppIndicator3.IndicatorStatus.ACTIVE)
        self.indicator.set_title("市场数据采集状态")

        self.menu = Gtk.Menu()
        self.summary_item = self._label("生产采集：检查中")
        self.db_item = self._label("ClickHouse：检查中")
        self.book_item = self._label("盘口：检查中")
        self.yield_item = self._label("收益：检查中")
        self.soak_item = self._label("永续验收：检查中")
        self.checked_item = self._label("最近检查：--")
        self.stream_items = [self._label("") for _ in range(EXPECTED_STREAM_COUNT)]
        for item in self.stream_items:
            item.set_no_show_all(True)

        self.menu.append(Gtk.SeparatorMenuItem())
        refresh_item = Gtk.MenuItem(label="立即刷新")
        refresh_item.connect("activate", lambda _item: self.schedule_refresh())
        self.menu.append(refresh_item)

        log_item = Gtk.MenuItem(label="打开生产采集日志")
        log_item.connect("activate", self.open_log)
        self.menu.append(log_item)

        self.menu.append(Gtk.SeparatorMenuItem())
        quit_item = Gtk.MenuItem(label="退出状态指示器")
        quit_item.connect("activate", lambda _item: Gtk.main_quit())
        self.menu.append(quit_item)

        self.menu.show_all()
        for item in self.stream_items:
            item.hide()
        self.indicator.set_menu(self.menu)

        GLib.timeout_add_seconds(args.interval, self.schedule_refresh)
        self.schedule_refresh()

    def _label(self, text: str):
        item = self.Gtk.MenuItem(label=text)
        item.set_sensitive(False)
        self.menu.append(item)
        return item

    def schedule_refresh(self) -> bool:
        if self.refreshing:
            return True
        self.refreshing = True
        threading.Thread(target=self._refresh_worker, daemon=True).start()
        return True

    def _refresh_worker(self) -> None:
        snapshot = probe(self.args.clickhouse_url, self.args.database)
        self.GLib.idle_add(self.apply_snapshot, snapshot)

    def apply_snapshot(self, snapshot: HealthSnapshot) -> bool:
        self.refreshing = False
        icon_path = self.icon_dir / f"{snapshot.state}.svg"
        self.indicator.set_icon_full(str(icon_path), snapshot.summary)
        self.summary_item.set_label(f"生产采集：{snapshot.summary}")
        self.db_item.set_label(
            f"ClickHouse：{'正常' if snapshot.clickhouse_ok else '异常'}"
        )

        if snapshot.streams:
            newest = max(snapshot.streams, key=lambda stream: stream.latest_minute)
            valid_count = sum(stream.valid_seconds == 60 for stream in snapshot.streams)
            self.book_item.set_label(
                f"盘口：{valid_count}/{EXPECTED_STREAM_COUNT} 完整，"
                f"最新 {local_minute(newest.latest_minute)}"
            )
        else:
            self.book_item.set_label("盘口：无可用状态")

        if snapshot.yield_age_seconds is None:
            self.yield_item.set_label("收益：检查失败或最近 8 小时无数据")
        else:
            self.yield_item.set_label(
                f"收益：最近写入 {format_age(snapshot.yield_age_seconds)}前"
            )
        self.soak_item.set_label(
            f"永续验收：{'运行中' if snapshot.soak_active else '未运行'}"
        )
        self.checked_item.set_label(
            f"最近检查：{snapshot.checked_at.astimezone():%H:%M:%S}"
        )

        for index, item in enumerate(self.stream_items):
            if index >= len(snapshot.streams):
                item.hide()
                continue
            stream = snapshot.streams[index]
            item.set_label(
                f"  {stream.exchange} {market_name(stream.market_type)}："
                f"{stream.valid_seconds}/60，{stream.age_seconds} 秒前"
            )
            item.show()

        if self.last_state is not None and snapshot.state != self.last_state:
            self.notify_transition(snapshot)
        self.last_state = snapshot.state
        return False

    def notify_transition(self, snapshot: HealthSnapshot) -> None:
        title = "市场数据采集已恢复" if snapshot.state == "healthy" else "市场数据采集状态变化"
        try:
            subprocess.Popen(
                [
                    "notify-send",
                    "--app-name=市场数据采集",
                    f"--icon={self.icon_dir / (snapshot.state + '.svg')}",
                    title,
                    snapshot.summary,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except OSError:
            pass

    def open_log(self, _item) -> None:
        command = f"tail -n 100 -F {shlex.quote(self.args.log_file)}"
        try:
            subprocess.Popen(
                ["gnome-terminal", "--", "bash", "-lc", command],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except OSError:
            pass

    def run(self) -> None:
        self.Gtk.main()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", type=int, default=30, help="检查间隔秒数，最小 10 秒")
    parser.add_argument("--clickhouse-url", default="http://127.0.0.1:8123")
    parser.add_argument("--database", default="crypto_market_info")
    parser.add_argument(
        "--log-file",
        default="/home/ubuntu/.local/share/crypto-market-info-collector/logs/collector.log",
    )
    parser.add_argument("--once", action="store_true", help="检查一次并输出文本，不启动桌面图标")
    args = parser.parse_args()
    if args.interval < 10:
        parser.error("--interval 不能小于 10 秒")
    return args


def main() -> int:
    args = parse_args()
    if args.once:
        snapshot = probe(args.clickhouse_url, args.database)
        print(snapshot.text())
        return {"healthy": 0, "degraded": 1, "down": 2}[snapshot.state]

    try:
        app = IndicatorApplication(args)
    except (ImportError, ValueError) as exc:
        print(
            "无法加载 Ubuntu AppIndicator。请安装 gir1.2-ayatanaappindicator3-0.1。\n"
            f"详细错误: {exc}",
            file=sys.stderr,
        )
        return 2
    app.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
