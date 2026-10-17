"""Single-writer hosted collector for a separately frozen future screen."""
import argparse,datetime,gzip,json,os,pathlib,shutil,sqlite3,tempfile,time
import adapter as ad,journal
from persistence import checkout,commit,git,push,sha,write,run,archive,clean_archived
journal.TABLES=journal.TABLES+('pending','admissions')
R=ad.ROOT

def freeze():
 f=json.loads((R/'FREEZE.json').read_text())
 for name,h in f['hashes'].items():
  if sha(R/name)!=h:raise RuntimeError('Frozen identity mismatch: '+name)
 return f

def persist(obs,c,data,f,seq,parent):
 seq+=1;dest=obs/'journal'/f'{seq:08d}.json.gz'
 if dest.exists():
  j=json.loads(gzip.decompress(dest.read_bytes()));assert j['sequence']==seq and j['parent_sha256']==parent;new=sha(dest)
 else:dest,new=journal.checkpoint(c,obs/'journal',seq,parent)
 archive(data,obs)
 write(obs/'STATUS.json',ad.status(c,json.loads((R/'COHORT.json').read_text()),f))
 write(obs/'CLOUD_STATUS.json',{'repository':os.environ['GITHUB_REPOSITORY'],'run_id':os.environ['GITHUB_RUN_ID'],'source_commit':git(R,'rev-parse','HEAD'),'workflow_head_sha':os.environ['GITHUB_SHA'],'freeze_sha256':sha(R/'FREEZE.json'),'checkpoint_sequence':seq,'checkpoint_sha256':new,'published_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
 for name in ['RESULTS.json','RESULTS.md','DIAGNOSTICS.json']:
  if (data/name).exists():shutil.copyfile(data/name,obs/name)
 commit(obs,'Append settled study checkpoint '+str(seq));push(obs,'observations')
 clean_archived(data,obs)
 return seq,new

def final_report(c,data,cohort,f):
 # The original account/report formulas run only after the fixed tail; append missing-coverage and per-arm diagnostics.
 ad.A.load_freeze=lambda:f;ad.A.config=lambda:ad.CFG;c.commit();ad.A.report()
 diagnostics={'stage':ad.A.getkv(c,'stage'),'issues':[dict(x) for x in c.execute('SELECT * FROM issues')],'per_model':{},'contrasts':f['comparisons'],'policy_comparison_not_indicator_causality':True,'continuous_original_account_formulas':True,'data_admission_policy':ad.POLICY,'no_annualization':True}
 for model in ad.CFG['models']:
  diagnostics['per_model'][model]={'events':[dict(x) for x in c.execute('SELECT status,COUNT(*) AS n FROM events WHERE model=? GROUP BY status',(model,))],'accepted_symbol_day_clusters':c.execute("SELECT COUNT(*) FROM (SELECT DISTINCT e.symbol,CAST(e.signal/86400000 AS INTEGER) FROM events e JOIN fills f ON e.id=f.event WHERE f.side='BUY' AND e.model=?)",(model,)).fetchone()[0],'account_filter_decisions':[dict(x) for x in c.execute("SELECT reason,COUNT(*) AS n FROM events WHERE model=? GROUP BY reason",(model,))]}
 result=json.loads((data/'RESULTS.json').read_text());by_account={x['account']:x for x in result['accounts']};clusters={}
 for account in by_account:
  events=[dict(x) for x in c.execute("SELECT DISTINCT e.id,e.symbol,e.signal FROM events e JOIN fills f ON f.event=e.id WHERE f.account=? AND f.side='BUY' ORDER BY e.signal",(account,))];clusters[account]=events
 diagnostics['per_account_samples']={k:{'accepted_events':len(v),'symbol_day_clusters':len({(e['symbol'],e['signal']//86400000) for e in v})} for k,v in clusters.items()}
 diagnostics['contrasts']=[]
 for comparison in f['comparisons']:
  left,right=comparison.split('-')
  for cap in ad.CFG['capitals']:
   for slip in ad.CFG['slippage_bps']:
    lk=f'{left}:{cap}:{slip}';rk=f'{right}:{cap}:{slip}';l=by_account[lk];r=by_account[rk];diagnostics['contrasts'].append({'contrast':comparison,'capital':cap,'slip':slip,'left_entries':l['entries'],'right_entries':r['entries'],'left_sample':diagnostics['per_account_samples'][lk],'right_sample':diagnostics['per_account_samples'][rk],'period_return_difference':l['period_return']-r['period_return'],'max_drawdown_difference':l['max_drawdown']-r['max_drawdown'],'causal_indicator_effect':False})
 spans=sorted((e['signal'],e['signal']+ad.CFG['hold_seconds']*1000) for k,v in clusters.items() if k.endswith(':2000:15') for e in v);merged=[]
 for start,end in spans:
  if merged and start<=merged[-1][1]:merged[-1][1]=max(merged[-1][1],end)
  else:merged.append([start,end])
 diagnostics['primary_temporal_cluster_intervals']=merged;diagnostics['primary_temporal_clusters']=len(merged);diagnostics['cluster_limit']='Connected overlapping four-hour signal horizons; temporal separation does not prove independent observations.'
 write(data/'DIAGNOSTICS.json',diagnostics)

def collect():
 from bootstrap import guard,runtime
 guard()
 if not (R/'FREEZE.json').exists():print('Not yet frozen; no collection');return
 f=freeze();cohort=json.loads((R/'COHORT.json').read_text())
 with tempfile.TemporaryDirectory() as tmp:
  tmp=pathlib.Path(tmp);obs=checkout(R,tmp/'obs','observations');data=tmp/'runtime';data.mkdir()
  assert json.loads((obs/'SEED.json').read_text())['freeze_sha256']==sha(R/'FREEZE.json')
  with journal.materialized_seed(obs) as seed:seq,parent=journal.restore(seed,obs/'SEED.json',obs/'journal',data/'state.sqlite')
  c=runtime(data);shutil.copyfile(R/'COHORT.json',data/'COHORT.json');deadline=time.monotonic()+ad.POLICY['lease_seconds'];checkpoint=time.monotonic();decision=0;failed=False
  try:
   if ad.A.getkv(c,'integrity_halt',False) or ad.A.getkv(c,'stage')=='DISK_CAP_HALT':raise ad.IntegrityError('Saved halt requires review')
   while time.monotonic()<deadline and ad.A.clock_ms()<f['tail_end']:
    began=time.monotonic()
    if sum(p.stat().st_size for p in data.rglob('*') if p.is_file())>ad.CFG['disk_limit_bytes'] or shutil.disk_usage(data).free<3*1024**3:
     ad.A.setkv(c,'stage','DISK_CAP_HALT');ad.A.issue(c,'DISK_CAP','Runtime cap reached; no relaxation');c.commit();raise ad.IntegrityError('Disk cap')
    ad.capture(c,data,cohort)
    if time.monotonic()>=decision:ad.process(c,data,cohort,f);decision=began+ad.POLICY['decision_seconds']
    if time.monotonic()-checkpoint>=ad.POLICY['checkpoint_seconds']:seq,parent=persist(obs,c,data,f,seq,parent);checkpoint=time.monotonic()
    time.sleep(max(0,ad.POLICY['poll_seconds']-(time.monotonic()-began)))
   if ad.A.clock_ms()>=f['tail_end']:
    ad.A.setkv(c,'stage','COMPLETE_OR_UNRESOLVED_AT_TAIL');final_report(c,data,cohort,f)
  except Exception:
   failed=True;raise
  finally:
   c.commit();halt=ad.A.getkv(c,'integrity_halt',False) or ad.A.getkv(c,'stage')=='DISK_CAP_HALT'
   try:seq,parent=persist(obs,c,data,f,seq,parent)
   finally:
    c.close()
    if halt:run('gh','api','repos/fengoftian/spot-alert-settled-pilot/actions/workflows/collector.yml/disable','--method','PUT')
  if ad.A.clock_ms()>=f['tail_end']:run('gh','api','repos/fengoftian/spot-alert-settled-pilot/actions/workflows/collector.yml/disable','--method','PUT')
  elif not failed:run('gh','workflow','run','collector.yml','--repo','fengoftian/spot-alert-settled-pilot','--ref','main','-f','mode=collect')
if __name__=='__main__':collect()
