"""Independent operational observer; never reads cash or interim return outcomes."""
import base64,datetime,gzip,hashlib,json,os,subprocess,concurrent.futures
REPO='fengoftian/spot-alert-settled-pilot'
def api(path,method='GET',body=None):
 p=subprocess.run(['gh','api',path,'--method',method]+(['--input','-'] if body is not None else []),input=json.dumps(body) if body is not None else None,capture_output=True,text=True,timeout=20)
 if p.returncode:raise RuntimeError(p.stderr[:300])
 return json.loads(p.stdout) if p.stdout.strip() else None
def file(path,ref):return base64.b64decode(api(f'repos/{REPO}/contents/{path}?ref={ref}')['content'])
def main():
 now=int(datetime.datetime.now(datetime.timezone.utc).timestamp()*1000);previous={};previous_sha=None
 try:
  old=api(f'repos/{REPO}/contents/health.json?ref=health-state');previous_sha=old['sha'];previous=json.loads(base64.b64decode(old['content']))
 except RuntimeError:pass
 try:
  head=api(f'repos/{REPO}/branches/observations')['commit']['sha'];st=json.loads(file('STATUS.json',head));cl=json.loads(file('CLOUD_STATUS.json',head));seed=json.loads(file('SEED.json',head));f=json.loads(file('FREEZE.json',cl['source_commit']));producer=api(f'repos/{REPO}/actions/runs/'+cl['run_id']);problems=[]
  assert cl['repository']==REPO and producer['head_sha']==cl['source_commit']==cl['workflow_head_sha'];assert hashlib.sha256(file('FREEZE.json',cl['source_commit'])).hexdigest()==seed['freeze_sha256']==cl['freeze_sha256']
  if st['integrity_halt'] or st['stage']=='DISK_CAP_HALT':problems.append('HALT')
  if now<f['tail_end'] and now-st['last_capture']>900000:problems.append('STALE')
  if producer['conclusion'] in {'failure','cancelled','timed_out'}:problems.append('PRODUCER_FAILED')
  name=f"journal/{cl['checkpoint_sequence']:08d}.json.gz";b=file(name,head);assert hashlib.sha256(b).hexdigest()==cl['checkpoint_sha256'];rr=json.loads(gzip.decompress(file(st['last_raw'],head)));seen=set()
  from urllib.parse import urlparse,parse_qs
  def verify(r):
   assert r.get('status')==200;raw=gzip.decompress(file(r['body_ref'],head));assert hashlib.sha256(raw).hexdigest()==r['sha256'];return parse_qs(urlparse(r['url']).query)['symbol'][0]
  with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:seen=set(pool.map(verify,rr))
  assert seen==set(x['symbol'] for x in st['coverage'])
  result={'checked_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'problems':problems,'observations_commit':head,'source_commit':cl['source_commit'],'run_id':cl['run_id'],'receipt_symbols':len(seen),'tick_age_seconds':(now-st['last_capture'])/1000,'tail_end':f['tail_end'],'unavailable_checks':0}
 except Exception as ex:
  result={'checked_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'problems':['UNAVAILABLE'],'detail':str(ex)[:400],'unavailable_checks':previous.get('unavailable_checks',0)+1}
 category='HALT' if 'HALT' in result['problems'] else ('COLLECTION' if any(x in result['problems'] for x in ['STALE','PRODUCER_FAILED']) else ('UNAVAILABLE' if result['problems'] and result['unavailable_checks']>=2 else None));result['incident']=category
 try:api(f'repos/{REPO}/git/ref/heads/health-state')
 except RuntimeError:api(f'repos/{REPO}/git/refs','POST',{'ref':'refs/heads/health-state','sha':api(f'repos/{REPO}/branches/main')['commit']['sha']})
 body={'branch':'health-state','message':'Record independent collection health','content':base64.b64encode((json.dumps(result,indent=2)+'\n').encode()).decode()}
 if previous_sha:body['sha']=previous_sha
 api(f'repos/{REPO}/contents/health.json','PUT',body);print(json.dumps(result,indent=2))
 if result.get('tail_end') and now>=result['tail_end']:api(f'repos/{REPO}/actions/workflows/health.yml/disable','PUT')
 return 1 if category and category!=previous.get('incident') else 0
if __name__=='__main__':raise SystemExit(main())
