"""Hosted qualification and fresh warmup; never imports any interrupted study state."""
import concurrent.futures,datetime,gzip,json,os,pathlib,shutil,sqlite3,tempfile,time
import adapter as ad
from persistence import checkout,commit,git,push,sha,write,archive as archive_raw
R=ad.ROOT

def runtime(path):
 path=pathlib.Path(path);path.mkdir(parents=True,exist_ok=True);shutil.copyfile(R/'config.json',path/'config.json');return ad.setup(path)
def guard():
 if os.environ.get('GITHUB_ACTIONS')!='true' or os.environ.get('GITHUB_REPOSITORY')!='fengoftian/spot-alert-settled-pilot':raise RuntimeError('Exact hosted repository required')
def qualification(phase):
 guard()
 if (R/'FREEZE.json').exists():raise RuntimeError('Qualification cannot amend a frozen study')
 with tempfile.TemporaryDirectory() as tmp:
  tmp=pathlib.Path(tmp);obs=checkout(R,tmp/'q','qualification',phase==1);data=tmp/'runtime'
  if phase==2:
   one=json.loads((obs/'Q1.json').read_text());assert one['new_failed_responses']==0 and min(one['admissions_per_symbol'].values())>=ad.POLICY['qualification_minimum_admissions_per_symbol'];assert sha(obs/'Q1.sqlite.gz')==one['snapshot_sha256']
   data.mkdir()
   with gzip.open(obs/'Q1.sqlite.gz','rb') as src,(data/'state.sqlite').open('wb') as dst:shutil.copyfileobj(src,dst)
  c=runtime(data);cohort=json.loads((R/'COHORT_SEED.json').read_text());deadline=time.monotonic()+ad.POLICY['qualification_phase_seconds'];next_books=0;errors=0
  while time.monotonic()<deadline:
   began=time.monotonic();_,receipts=ad.capture(c,data,cohort);errors+=sum(x.get('status')!=200 for x in receipts)
   if time.monotonic()>=next_books:
    index=ad.A.getkv(c,'q_book_index',0);symbols=cohort['collected_symbols'];wanted=[symbols[(index+i)%len(symbols)] for i in range(3)];book,ref=ad.books(c,wanted,'qualification-books');seen=set(ad.A.getkv(c,'q_book_symbols',[]))
    for s,(raw,rec) in book.items():
     row=c.execute('SELECT t,available FROM admissions WHERE symbol=? ORDER BY t DESC LIMIT 1',(s,)).fetchone()
     if row and row['available']<=rec['received']<=row['t']+240000:seen.add(s)
    ad.A.setkv(c,'q_book_symbols',sorted(seen));ad.A.setkv(c,'q_book_index',index+3);next_books=began+60
   ad.A.setkv(c,'q_captures',ad.A.getkv(c,'q_captures',0)+1);c.commit();time.sleep(max(0,ad.POLICY['poll_seconds']-(time.monotonic()-began)))
  assert c.execute('SELECT COUNT(*) FROM accounts').fetchone()[0]==0
  integrity=c.execute('PRAGMA integrity_check').fetchone()[0];counts={s:c.execute('SELECT COUNT(*) FROM admissions WHERE symbol=?',(s,)).fetchone()[0] for s in cohort['collected_symbols']};seen=ad.A.getkv(c,'q_book_symbols',[]);c.commit();c.close()
  archive=obs/f'Q{phase}.sqlite.gz'
  with (data/'state.sqlite').open('rb') as src,gzip.open(archive,'wb') as dst:shutil.copyfileobj(src,dst)
  archive_raw(data,obs)
  result={'phase':phase,'source_commit':git(R,'rev-parse','HEAD'),'run_id':os.environ['GITHUB_RUN_ID'],'snapshot_sha256':sha(archive),'sqlite_integrity':integrity,'admissions_per_symbol':counts,'timely_book_symbols':seen,'new_failed_responses':errors,'cold_resume_verified':phase==2,'accounts':0,'strategy_evaluation':False,'parent_snapshot_sha256':None if phase==1 else one['snapshot_sha256']}
  write(obs/f'Q{phase}.json',result)
  if phase==2:
   result['passed']=integrity=='ok' and errors==0 and min(counts.values())>=ad.POLICY['qualification_minimum_admissions_per_symbol'] and set(seen)==set(cohort['collected_symbols'])
   write(obs/'QUALIFICATION.json',result)
  commit(obs,'Append hosted data-only qualification phase '+str(phase));push(obs,'qualification')
  if phase==2 and not result['passed']:raise RuntimeError('Hosted qualification gates not met; no study launched')
  print(json.dumps(result,indent=2),flush=True)

def warm_symbol(symbol,start,end):
 captures=[];previous=None
 for attempt in range(ad.POLICY['warmup_max_observations']):
  _,rows,rr=ad.A.fetch_symbol(symbol,start,end,47);captures.extend(rr)
  if any(x.get('status')!=200 for x in rr) or not rows or rows[-1][0]!=end-60000 or len(rows)<ad.A.WARM or any(x[0]!=rows[0][0]+i*60000 for i,x in enumerate(rows)):raise RuntimeError('Incomplete required warmup: '+symbol)
  if previous==rows:return symbol,rows,captures
  previous=rows;time.sleep(15)
 raise RuntimeError('Warmup did not stabilize: '+symbol)

def prepare():
 guard()
 if (R/'FREEZE.json').exists():raise RuntimeError('Study already frozen; never reset')
 with tempfile.TemporaryDirectory() as tmp:
  tmp=pathlib.Path(tmp);q=checkout(R,tmp/'q','qualification');qualification=json.loads((q/'QUALIFICATION.json').read_text());assert qualification['passed'] and qualification['source_commit']==git(R,'rev-parse','HEAD')
  data=tmp/'runtime';c=runtime(data);seed=json.loads((R/'COHORT_SEED.json').read_text());symbols=seed['collected_symbols'];info,rec=ad.A.request('exchangeInfo',{'symbols':json.dumps(symbols,separators=(',',':')),'showPermissionSets':'false'});ad.A.save_raw([rec],'inception');assert rec.get('status')==200
  current={x['symbol']:x for x in info['symbols']};assert set(current)==set(symbols) and all(current[s]['status']=='TRADING' and current[s].get('isSpotTradingAllowed') for s in symbols)
  end=(ad.A.clock_ms()//60000-ad.POLICY['bootstrap_old_minutes'])*60000;start=end-(31*1440+65)*60000
  cohort={'captured':ad.A.clock_ms(),'warmup_end':end,'members':[current[x['symbol']] for x in seed['members']],'collected_symbols':symbols,'benchmark_metadata':{s:current[s] for s in ['BTCUSDT','ETHUSDT']},'selection':'Same original 44 symbol identities, no outcome selection/replacement; fresh public metadata and fresh observations'};write(R/'COHORT.json',cohort);coverage=[]
  with concurrent.futures.ThreadPoolExecutor(max_workers=ad.CFG['maximum_workers']) as pool:
   for symbol,rows,rr in pool.map(lambda s:warm_symbol(s,start,end),symbols):
    ref=ad.A.save_raw(rr,'warmup-'+symbol);received=max(x['received'] for x in rr);ad.A.store_bars(c,symbol,rows,received,end);c.commit();coverage.append({'symbol':symbol,'first':rows[0][0],'last':rows[-1][0],'minutes':len(rows),'unavailable_prefix_minutes':(rows[0][0]-start)//60000,'seven_day_complete':True,'two_matching_mature_observations':True,'receipt':ref});print('Warmup',symbol,len(rows),flush=True)
  ad.A.create_accounts(c,ad.CFG)
  for symbol in symbols:
   rows=[json.loads(x[0]) for x in c.execute('SELECT raw FROM bars WHERE symbol=? ORDER BY t',(symbol,))];ad.A.setkv(c,'processed:'+symbol,rows[-2][0]);ad.A.setkv(c,'B:processed:'+symbol,rows[-2][0]);ad.signals(c,symbol,rows,{'start':end+10**12,'end':end+10**12+86400000},end);day=rows[-1][0]//86400000*86400000
   next(x for x in coverage if x['symbol']==symbol)['b0_baseline_available']=ad.B.daily_return_threshold(c,symbol,day,ad.CFG) is not None
  write(R/'WARMUP_COVERAGE.json',coverage);shutil.copyfile(R/'COHORT.json',data/'COHORT.json');now=ad.A.clock_ms();future=((now+1800000+3599999)//3600000)*3600000;finish=future+30*86400000
  names=['adapter.py','bootstrap.py','cloud.py','persistence.py','journal.py','config.json','ADMISSION_POLICY.json','COHORT_SEED.json','COHORT.json','WARMUP_COVERAGE.json','VENDOR_IDENTITY.json','DESIGN.md','TEST_RESULTS.json','run_tests.py','health.py','.github/workflows/health.yml','.github/workflows/collector.yml']+[str(x.relative_to(R)) for x in (R/'vendor').iterdir() if x.is_file()]+[str(x.relative_to(R)) for x in (R/'tests').iterdir() if x.is_file()]
  freeze={'schema':'settled-spot-comparison-v1','frozen':now,'start':future,'end':finish,'tail_end':finish+14400000,'start_nzdt':ad.A.nz(future),'end_nzdt':ad.A.nz(finish),'start_utc':ad.A.utc(future),'end_utc':ad.A.utc(finish),'hashes':{n:sha(R/n) for n in names},'technical_qualification':qualification,'owner_authority':'17 October 2026: autonomous corrected collector, hosted validation and separately frozen future paper comparison','comparisons':['A1-A0','A2-A1','A3-A2','B0-C_A2','B1-C_A2','B2-C_A2','B3-C_A2'],'not_independent_replication_of_old_windows':True}
  write(R/'FREEZE.json',freeze);ad.A.setkv(c,'stage','WARMUP');c.commit();counts={t:c.execute('SELECT COUNT(*) FROM '+t).fetchone()[0] for t in ['bars','events','accounts','positions','fills','marks']};assert counts['accounts']==81 and not any(counts[t] for t in ['events','positions','fills','marks']);c.close()
  commit(R,'Freeze new settled-data comparison before future start');push(R,'main')
  obs=checkout(R,tmp/'obs','observations',True);(obs/'seed').mkdir();archive=tmp/'seed.sqlite.gz'
  with (data/'state.sqlite').open('rb') as src,gzip.open(archive,'wb') as dst:shutil.copyfileobj(src,dst)
  parts=[]
  with archive.open('rb') as src:
   i=0
   while True:
    b=src.read(48*1024*1024)
    if not b:break
    name=f'seed/{i:04d}.part';(obs/name).write_bytes(b);parts.append({'path':name,'bytes':len(b),'sha256':sha(obs/name)});i+=1
  write(obs/'SEED.json',{'seed_sha256':sha(archive),'seed_parts':parts,'freeze_sha256':sha(R/'FREEZE.json'),'snapshot_counts':counts,'source_commit':git(R,'rev-parse','HEAD'),'warmup_only':True});archive_raw(data,obs);(obs/'journal').mkdir();write(obs/'STATUS.json',{'stage':'WARMUP','window':freeze,'accounts':81,'positions':0,'performance_evaluation':'DEFERRED_TO_FIXED_END'});commit(obs,'Publish fresh warmup-only seed and original public receipts');push(obs,'observations');print(json.dumps({'window':{k:freeze[k] for k in ['start_nzdt','end_nzdt','tail_end']},'accounts':81,'seed_parts':len(parts),'source_commit':git(R,'rev-parse','HEAD')},indent=2))
  from persistence import run
  run('gh','workflow','run','collector.yml','--repo','fengoftian/spot-alert-settled-pilot','--ref','main','-f','mode=collect')
if __name__=='__main__':
 import sys
 if sys.argv[1]=='prepare':prepare()
 else:qualification(int(sys.argv[1]))
