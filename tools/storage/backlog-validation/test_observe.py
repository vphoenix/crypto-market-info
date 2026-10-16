import copy
import unittest
from observe import latency_summary


class LatencyAcceptanceTests(unittest.TestCase):
    def test_missing_shards_and_whole_minutes_never_pass(self):
        now = 6000 + 10
        lags = {minute: 20 for minute in range(84 * 60, 99 * 60, 60)}
        members = {minute: dict(cex=337, options_catalog=2796) for minute in lags}
        self.assertTrue(latency_summary(lags, members, 2796, now)['latency_scope_complete'])
        missing_shard = copy.deepcopy(members)
        missing_shard[min(lags)]['options_catalog'] -= 32
        self.assertFalse(latency_summary(lags, missing_shard, 2796, now)['latency_scope_complete'])
        missing_minute = dict(lags)
        del missing_minute[min(lags)]
        self.assertFalse(latency_summary(missing_minute, members, 2796, now)['latency_scope_complete'])
        self.assertIsNone(latency_summary(lags, members, 0, now)['last_shard_p99_seconds'])

    def test_new_listing_does_not_change_earlier_plan_membership(self):
        now = 6010
        lags = {minute: 20 for minute in range(84 * 60, 99 * 60, 60)}
        expected = {minute: 2796 if minute < 96*60 else 2802 for minute in lags}
        members = {minute: dict(cex=337, options_catalog=count) for minute, count in expected.items()}
        self.assertTrue(latency_summary(lags, members, expected, now)['latency_scope_complete'])


if __name__ == '__main__':
    unittest.main()
