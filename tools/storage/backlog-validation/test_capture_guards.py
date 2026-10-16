import copy
import datetime as dt
import unittest
from capture_guards import stable_full_scope

class CaptureGuardTests(unittest.TestCase):
    def fixture(self):
        now=dt.datetime(2026,10,15,0,10,tzinfo=dt.timezone.utc)
        rows=[]
        for minute in range(11):
            at=(now-dt.timedelta(minutes=10-minute)).isoformat()
            rows.append(dict(utc=at,writer_metrics_fresh=True,writer=dict(pid=2,time=at,spool_index_complete=True,redis_index_complete=True,blocked=0),services={'collector':{'MainPID':'1'},'storage-writer':{'MainPID':'2','ActiveState':'active'}},source_health=[dict(time=at,msg='options catalog runtime health',entries=2822),dict(time=at,msg='OKX paired runtime health',books=dict(ReadyInstruments=332,InvalidInstruments=0))]))
        return rows,[dict(effective=(now-dt.timedelta(hours=1)).isoformat(),books=2822)],now
    def test_instant_full_scope_does_not_hide_recent_shrink(self):
        rows,plans,now=self.fixture();self.assertTrue(stable_full_scope(rows,plans,now))
        for failure in ['short_history','scope_shrink','stale_metrics','writer_restart','missing_observation','stale_source','redis_incomplete']:
            with self.subTest(failure=failure):
                r,p,n=copy.deepcopy((rows,plans,now))
                if failure=='short_history':r=r[-3:]
                if failure=='scope_shrink':p.append(dict(effective=r[3]['utc'],books=11));p.append(dict(effective=r[4]['utc'],books=2822))
                if failure=='stale_metrics':r[5]['writer_metrics_fresh']=False
                if failure=='writer_restart':r[5]['services']['storage-writer']['MainPID']='3'
                if failure=='missing_observation':del r[5]
                if failure=='stale_source':r[5]['source_health'][0]['time']=r[0]['utc']
                if failure=='redis_incomplete':r[5]['writer']['redis_index_complete']=False
                self.assertFalse(stable_full_scope(r,p,n))
if __name__=='__main__':unittest.main()
