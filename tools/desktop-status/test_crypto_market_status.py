import datetime as dt
import importlib.util
import pathlib
import sys
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("crypto_market_status.py")
SPEC = importlib.util.spec_from_file_location("crypto_market_status", MODULE_PATH)
assert SPEC and SPEC.loader
STATUS = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = STATUS
SPEC.loader.exec_module(STATUS)


def stream(age=70, valid=60):
    return STATUS.StreamHealth("Binance", "spot", "BTCUSDT", "2026-09-12 18:36:00", age, valid)


class ClassifyTests(unittest.TestCase):
    def test_healthy_requires_five_fresh_complete_streams(self):
        state, _ = STATUS.classify(True, tuple(stream() for _ in range(5)), True, 30, True)
        self.assertEqual(state, "healthy")

    def test_missing_stream_is_down(self):
        state, _ = STATUS.classify(True, tuple(stream() for _ in range(4)), True, 30, True)
        self.assertEqual(state, "down")

    def test_invalid_second_is_degraded(self):
        streams = tuple(stream(valid=59 if index == 0 else 60) for index in range(5))
        state, _ = STATUS.classify(True, streams, True, 30, True)
        self.assertEqual(state, "degraded")

    def test_stale_book_is_down(self):
        streams = tuple(stream(age=241 if index == 0 else 70) for index in range(5))
        state, _ = STATUS.classify(True, streams, True, 30, True)
        self.assertEqual(state, "down")

    def test_yield_failure_only_degrades_fresh_books(self):
        state, _ = STATUS.classify(True, tuple(stream() for _ in range(5)), True, None, False)
        self.assertEqual(state, "degraded")


class FormattingTests(unittest.TestCase):
    def test_utc_minute_is_rendered_in_local_timezone(self):
        value = STATUS.local_minute("2026-09-12 18:36:00")
        self.assertTrue(value.endswith("18:36") or value.endswith("02:36"))


if __name__ == "__main__":
    unittest.main()
