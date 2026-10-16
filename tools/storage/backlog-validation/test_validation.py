import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('validation', Path(__file__).with_name('run-validation.py'))
validation = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validation)


class CapacityScopeTests(unittest.TestCase):
    def test_small_current_plan_cannot_pass_using_healthy_low_latency(self):
        at = "2026-10-14T12:00:00+00:00"
        row = dict(utc=at, writer=dict(pid=23, time=at, spool_index_complete=True,
                   redis_index_complete=True, old_pending=0, blocked=0),
                   services={'storage-writer': {'MainPID': '23', 'ActiveState': 'active'}},
                   closed_minute_count=15, latency_scope_complete=True, last_shard_p99_seconds=10,
                   latency_scope=dict(expected_cex_members=337, expected_options_members={'minute': 2870}),
                   source_health=[dict(time=at, msg='options catalog runtime health', entries=2870),
                                  dict(time=at, msg='OKX paired runtime health', books=dict(ReadyInstruments=332, InvalidInstruments=0))])
        self.assertTrue(validation.live_ready_for_isolated_load(row))
        for failure in ['small_plan', 'small_runtime', 'stale_health', 'invalid_books', 'missing_plan']:
            with self.subTest(failure=failure):
                bad = copy.deepcopy(row)
                if failure == 'small_plan': bad['latency_scope']['expected_options_members']['minute'] = 11
                if failure == 'small_runtime': bad['source_health'][0]['entries'] = 11
                if failure == 'stale_health': bad['source_health'][0]['time'] = '2026-10-14T11:55:00+00:00'
                if failure == 'invalid_books': bad['source_health'][1]['books']['InvalidInstruments'] = 1
                if failure == 'missing_plan': bad.pop('latency_scope')
                self.assertTrue(validation.live_has_healthy_latency(bad))
                self.assertFalse(validation.live_ready_for_isolated_load(bad))

    def test_isolated_load_requires_full_live_latency_not_only_empty_old_queue(self):
        row = dict(utc="2026-10-14T12:00:00+00:00", writer=dict(pid=23,time="2026-10-14T12:00:00+00:00",spool_index_complete=True, redis_index_complete=True, old_pending=0, blocked=0),
                   services={'storage-writer':{'MainPID':'23','ActiveState':'active'}},closed_minute_count=15, latency_scope_complete=True, last_shard_p99_seconds=25)
        self.assertTrue(validation.live_has_healthy_latency(row))
        for field, value in [('latency_scope_complete', False), ('last_shard_p99_seconds', 31),
                             ('last_shard_p99_seconds', None)]:
            invalid = copy.deepcopy(row)
            invalid[field] = value
            self.assertFalse(validation.live_has_healthy_latency(invalid))
        row['writer']['redis_index_complete']=False
        self.assertFalse(validation.live_has_healthy_latency(row))
        row['writer']['redis_index_complete']=True
        row['writer']['old_pending'] = 1
        self.assertFalse(validation.live_has_healthy_latency(row))

    def test_fresh_observer_cannot_hide_stale_or_invalid_writer_metrics(self):
        row=dict(utc="2026-10-14T12:00:00+00:00",writer=dict(pid=23,time="2026-10-14T12:00:00+00:00",spool_index_complete=True, redis_index_complete=True,old_pending=0,blocked=0),services={'storage-writer':{'MainPID':'23','ActiveState':'active'}},closed_minute_count=15,latency_scope_complete=True,last_shard_p99_seconds=10)
        self.assertTrue(validation.live_has_healthy_latency(row))
        for timestamp in [None,"bad","2026-10-14T11:59:00+00:00","2026-10-14T12:01:00+00:00","2026-10-14T12:00:00"]:
            row['writer']['time']=timestamp
            self.assertFalse(validation.live_has_healthy_latency(row))

    def test_new_process_cannot_inherit_fresh_empty_metrics_from_old_process(self):
        row=dict(utc="2026-10-14T12:00:00+00:00",writer=dict(pid=22,time="2026-10-14T11:59:59+00:00",spool_index_complete=True,redis_index_complete=True,old_pending=0,blocked=0),services={'storage-writer':{'MainPID':'23','ActiveState':'active'}},closed_minute_count=15,latency_scope_complete=True,last_shard_p99_seconds=10)
        self.assertFalse(validation.live_has_healthy_latency(row))
        row['writer']['pid']=23
        self.assertTrue(validation.live_has_healthy_latency(row))
        row['services']['storage-writer']['ActiveState']='activating'
        self.assertFalse(validation.live_has_healthy_latency(row))

    def test_full_cex_cannot_hide_startup_option_plan_or_missing_minute(self):
        coverage = dict(distinct_streams=337, minutes_per_stream={str(i): 90 for i in range(337)})
        manifest = dict(Minutes={str(i): dict(cex=dict(Members=337), options_catalog=dict(Members=2806))
                                 for i in range(90)})
        self.assertTrue(validation.fixture_has_full_scope(coverage, manifest))
        partial = copy.deepcopy(manifest)
        partial['Minutes']['0']['options_catalog']['Members'] = 11
        self.assertFalse(validation.fixture_has_full_scope(coverage, partial))
        missing = copy.deepcopy(coverage)
        missing['minutes_per_stream']['0'] = 89
        missing['minutes_per_stream']['1'] = 91
        self.assertFalse(validation.fixture_has_full_scope(missing, manifest))
        del partial['Minutes']['0']
        self.assertFalse(validation.fixture_has_full_scope(coverage, partial))


if __name__ == '__main__':
    unittest.main()
