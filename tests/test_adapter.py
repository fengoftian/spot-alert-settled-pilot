import copy,gzip,json,pathlib,sqlite3,tempfile,unittest
from unittest.mock import patch
import adapter as ad,journal,cloud,persistence
R=ad.ROOT

def bar(t,price=100,quote=100):return [t,str(price),str(price*1.001),str(price*.999),str(price),str(quote/price),t+59999,str(quote),1,str(quote/price/2),str(quote/2),'0']
class AdapterTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.data=pathlib.Path(self.tmp.name);self.c=ad.setup(self.data)
 def tearDown(self):self.c.close();self.tmp.cleanup()
 def test_two_observations_must_both_follow_minimum_age(self):
  b=bar(0)
  for t in [61000,80000,119999,120000]:self.assertFalse(ad.accept(self.c,'X',b,t,'raw/test'))
  self.assertFalse(ad.accept(self.c,'X',b,134999,'raw/test'));self.assertTrue(ad.accept(self.c,'X',b,135000,'raw/test'))
 def test_duplicate_receipt_does_not_manufacture_stability(self):
  b=bar(0);ad.accept(self.c,'X',b,120000,'r');self.assertFalse(ad.accept(self.c,'X',b,120000,'r'));self.assertFalse(ad.accept(self.c,'X',b,134999,'r'))
 def test_actual_three_revisions_are_held_then_stable_synthetic_repeat_admits(self):
  for f in json.loads((R/'tests/actual_revisions.json').read_text()):
   a,b=f['observations'];s=f['symbol'];self.assertFalse(ad.accept(self.c,s,a['row'],a['received'],'original'));self.assertFalse(ad.accept(self.c,s,b['row'],b['received'],'revised'));self.assertTrue(ad.accept(self.c,s,b['row'],b['received']+15000,'synthetic-repeat'))
   with self.assertRaises(ad.IntegrityError):ad.accept(self.c,s,a['row'],b['received']+16000,'synthetic-late-change')
 def test_pending_state_survives_cold_restore(self):
  b=bar(0);ad.accept(self.c,'X',b,120000,'one');self.c.commit();self.c.close();self.c=ad.setup(self.data);self.assertTrue(ad.accept(self.c,'X',b,135000,'two'))
 def test_out_of_order_and_malformed_receipts_fail(self):
  b=bar(0);ad.accept(self.c,'X',b,120000,'r')
  with self.assertRaises(ad.IntegrityError):ad.accept(self.c,'X',b,119000,'r')
  bad=copy.deepcopy(b);bad[3]='500'
  with self.assertRaises(ValueError):ad.accept(self.c,'X',bad,135000,'r')
 def test_revised_provisional_restarts_mature_clock(self):
  b=bar(0);ad.accept(self.c,'X',b,120000,'r');b[4]='100.01';self.assertFalse(ad.accept(self.c,'X',b,135000,'r'));self.assertFalse(ad.accept(self.c,'X',b,149999,'r'));self.assertTrue(ad.accept(self.c,'X',b,150000,'r'))
 def test_new_81_accounts_and_identical_risk_parameters(self):
  ad.A.create_accounts(self.c,ad.CFG);self.assertEqual(self.c.execute('SELECT COUNT(*) FROM accounts').fetchone()[0],81)
  for file in ['a_config.json','b_config.json']:
   cfg=json.loads((R/'vendor'/file).read_text())
   for k,v in cfg.items():
    if k not in {'models','schema','question','disk_limit_bytes'}:self.assertEqual(ad.CFG[k],v,k)
 def test_namespaced_b_state_does_not_replace_a_state(self):
  ad.A.setkv(self.c,'qual:X',{'A0':True});ad.B.engine.setkv(self.c,'qual:X',{'B0':False});self.assertEqual(ad.A.getkv(self.c,'qual:X'),{'A0':True});self.assertEqual(ad.B.engine.getkv(self.c,'qual:X'),{'B0':False})
 def test_unified_a2_and_concurrent_control_match_with_future_invariance(self):
  rows=[bar(i*60000,100,100) for i in range(ad.A.WARM)]
  for i in range(len(rows)-60,len(rows)):rows[i]=bar(i*60000,111,400)
  for x in rows:self.c.execute('INSERT INTO bars VALUES(?,?,?,?)',('BTCUSDT',x[0],json.dumps(bar(x[0])),x[6]+1))
  ad.A.setkv(self.c,'processed:X',rows[-2][0]);ad.A.setkv(self.c,'B:processed:X',rows[-2][0]);ad.A.setkv(self.c,'qual:X',{m:False for m in ['A0','A1','A2']});ad.B.engine.setkv(self.c,'qual:X',{m:False for m in ad.B.MODELS})
  f={'start':rows[-1][0],'end':rows[-1][0]+86400000};ad.signals(self.c,'X',rows,f,rows[-1][6]+100000)
  events={x['model']:x for x in self.c.execute('SELECT * FROM events')};self.assertIn('A2',events);self.assertIn('C_A2',events)
  for k in ['signal','due','deadline','status']:self.assertEqual(events['A2'][k],events['C_A2'][k])
  n=len(events);ad.signals(self.c,'X',rows,f,rows[-1][6]+100001);self.assertEqual(self.c.execute('SELECT COUNT(*) FROM events').fetchone()[0],n)
 def test_expired_events_never_backfill_and_limits_remain(self):
  ad.A.create_accounts(self.c,ad.CFG);self.c.execute("INSERT INTO events VALUES('test','A0','X',0,60000,180000,'{}','PENDING','')");ad.A.entries(self.c,{}, {},ad.CFG,{'end':9999999},180001,'none');self.assertEqual(self.c.execute('SELECT status FROM events').fetchone()[0],'REJECTED');self.assertEqual(self.c.execute('SELECT COUNT(*) FROM fills').fetchone()[0],0)
 def test_pending_and_admissions_are_journaled(self):
  journal.install(self.c);ad.accept(self.c,'X',bar(0),120000,'r');self.c.commit();p,h=journal.checkpoint(self.c,self.data/'journal',1,'parent');d=json.loads(gzip.decompress(p.read_bytes()));self.assertTrue(any(x['table']=='pending' for x in d['changes']))
 def test_push_retries_same_commit_and_refuses_remote_divergence(self):
  calls=[]
  def git(root,*args):
   calls.append(args)
   if args==('rev-parse','HEAD'):return 'ours'
   if args[:1]==('push',):raise RuntimeError('remote commit_refs')
   if args[:1]==('ls-remote',):return 'intruder refs/heads/observations'
   if args==('rev-parse','HEAD^'):return 'parent'
   return ''
  with patch.object(persistence,'git',side_effect=git),patch.object(persistence.time,'sleep'):
   with self.assertRaisesRegex(RuntimeError,'advanced'):persistence.push(self.data,'observations')
  self.assertFalse(any('--force' in a for a in calls))
 def test_ambiguous_success_is_acknowledged_without_rewrite(self):
  def git(root,*args):
   if args==('rev-parse','HEAD'):return 'ours'
   if args[:1]==('push',):raise RuntimeError('timeout')
   if args[:1]==('ls-remote',):return 'ours refs/heads/observations'
  with patch.object(persistence,'git',side_effect=git):persistence.push(self.data,'observations')
 def test_bad_book_or_slow_response_not_eligible(self):
  b={'asks':[['101','2']],'bids':[['99','2']]};self.assertTrue(ad.valid_book(b,{'status':200,'duration':1}));self.assertFalse(ad.valid_book(b,{'status':200,'duration':6}));self.assertFalse(ad.valid_book({'asks':[['99','2']],'bids':[['101','2']]},{'status':200,'duration':1}))

 def test_content_addressing_retains_receipts_and_all_body_versions(self):
  import hashlib
  raw='[]';rec={'raw':raw,'sha256':hashlib.sha256(raw.encode()).hexdigest(),'received':100}
  one=ad.A.save_raw([rec],'one');rec['received']=200;two=ad.A.save_raw([rec],'two');self.assertEqual(len(list((self.data/'raw/bodies').iterdir())),1)
  a=json.loads(gzip.decompress((self.data/one).read_bytes()))[0];b=json.loads(gzip.decompress((self.data/two).read_bytes()))[0];self.assertEqual((a['received'],b['received']),(100,200));self.assertIn('raw',rec);self.assertEqual(gzip.decompress((self.data/a['body_ref']).read_bytes()).decode(),raw)
 def test_book_from_before_feature_availability_is_excluded(self):
  self.assertEqual(ad.entry_books_since({'X':({},{'received':99})},100),{});self.assertIn('X',ad.entry_books_since({'X':({},{'received':100})},100))
 def test_extended_journal_replays_pending_and_admission(self):
  import hashlib
  self.c.commit();seed=self.data/'seed.gz'
  with (self.data/'state.sqlite').open('rb') as src,gzip.open(seed,'wb') as dst:dst.write(src.read())
  manifest=self.data/'SEED.json';manifest.write_text(json.dumps({'seed_sha256':journal.sha(seed)}));journal.install(self.c);ad.accept(self.c,'X',bar(0),120000,'first');self.c.commit();one,parent=journal.checkpoint(self.c,self.data/'deltas',1,journal.sha(seed));ad.accept(self.c,'X',bar(0),135000,'second');self.c.commit();journal.checkpoint(self.c,self.data/'deltas',2,parent)
  count,_=journal.restore(seed,manifest,self.data/'deltas',self.data/'cold.sqlite');cold=sqlite3.connect(self.data/'cold.sqlite');self.assertEqual(count,2);self.assertEqual(cold.execute('SELECT COUNT(*) FROM pending').fetchone()[0],0);self.assertEqual(cold.execute('SELECT available FROM admissions').fetchone()[0],135000);cold.close()
 def test_runtime_capacity_is_combined_original_capacity_with_reserve(self):
  old=[json.loads((R/'vendor'/x).read_text())['disk_limit_bytes'] for x in ['a_config.json','b_config.json']];self.assertEqual(ad.CFG['disk_limit_bytes'],sum(old))
if __name__=='__main__':unittest.main()
