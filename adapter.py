"""Versioned data adapter; vendor alert, account and execution formulas are unchanged."""
import concurrent.futures,gzip,hashlib,importlib.util,json,pathlib,sqlite3,time,uuid
ROOT=pathlib.Path(__file__).resolve().parent

def module(name,path):
 spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
A=module('original_a',ROOT/'vendor/engine.py');B=module('original_b',ROOT/'vendor/study.py')
base_get=B.engine.getkv;base_set=B.engine.setkv

def bkey(k):return 'B:'+k if k.startswith(('qual:','processed:')) else k
B.engine.getkv=lambda c,k,default=None:base_get(c,bkey(k),default)
B.engine.setkv=lambda c,k,v:base_set(c,bkey(k),v)
CFG=json.loads((ROOT/'config.json').read_text());POLICY=json.loads((ROOT/'ADMISSION_POLICY.json').read_text())
class IntegrityError(ValueError):pass

def save_raw(records,label):
 root=A.ROOT;dest=root/'raw';(dest/'bodies').mkdir(parents=True,exist_ok=True);saved=[]
 for rec in records:
  x=dict(rec)
  if 'raw' in x:
   raw=x.pop('raw').encode();h=hashlib.sha256(raw).hexdigest();assert h==x['sha256'];body=dest/'bodies'/(h+'.json.gz')
   if not body.exists():body.write_bytes(gzip.compress(raw,mtime=0))
   x['body_ref']='raw/bodies/'+body.name
  saved.append(x)
 name=f'{A.clock_ms()}-{label}-{uuid.uuid4().hex[:8]}.json.gz';(dest/name).write_bytes(gzip.compress(json.dumps(saved,separators=(',',':')).encode(),mtime=0));return 'raw/'+name
A.save_raw=save_raw

def setup(data):
 data=pathlib.Path(data);data.mkdir(parents=True,exist_ok=True);A.ROOT=data;B.engine.ROOT=data
 c=A.connect(data/'state.sqlite')
 c.executescript('''CREATE TABLE IF NOT EXISTS pending(symbol TEXT,t INTEGER,raw TEXT,mature_since INTEGER,last_received INTEGER,receipt TEXT,PRIMARY KEY(symbol,t));
 CREATE TABLE IF NOT EXISTS admissions(symbol TEXT,t INTEGER,available INTEGER,receipt TEXT,PRIMARY KEY(symbol,t));''')
 return c

def accept(c,symbol,row,received,receipt):
 A.validate_bar(row);closed=row[6]+1
 if received<closed:return False
 body=json.dumps(row,separators=(',',':'));old=c.execute('SELECT raw FROM bars WHERE symbol=? AND t=?',(symbol,row[0])).fetchone()
 if old:
  if old[0]!=body:raise IntegrityError('Admitted candle revised: '+symbol+' '+str(row[0]))
  return False
 prev=c.execute('SELECT raw,mature_since,last_received FROM pending WHERE symbol=? AND t=?',(symbol,row[0])).fetchone()
 if prev and received<prev[2]:raise IntegrityError('Out-of-order receipt')
 mature=received>=closed+POLICY['minimum_age_ms'];since=prev[1] if prev and prev[0]==body else None
 if mature and since is None:since=received
 if prev and prev[0]==body and since is not None and received>prev[2] and received-since>=POLICY['stable_separation_ms']:
  c.execute('INSERT INTO bars VALUES(?,?,?,?)',(symbol,row[0],body,received));c.execute('INSERT INTO admissions VALUES(?,?,?,?)',(symbol,row[0],received,receipt));c.execute('DELETE FROM pending WHERE symbol=? AND t=?',(symbol,row[0]));return True
 c.execute('INSERT OR REPLACE INTO pending VALUES(?,?,?,?,?,?)',(symbol,row[0],body,since,received,receipt));return False

def signals(c,symbol,rows,f,received):
 A.process_signals(c,symbol,rows,CFG,f,received);B.process_signals(c,symbol,rows,CFG,f,received)

def capture(c,data,cohort):
 now=A.clock_ms();end=now//60000*60000;symbols=cohort['collected_symbols'];todo=[]
 for s in symbols:
  last=c.execute('SELECT MAX(t) FROM bars WHERE symbol=?',(s,)).fetchone()[0]
  todo.append((s,(last-60000) if last is not None else end-5*60000,end,CFG['max_pages_per_symbol_per_tick']))
 batches=[]
 with concurrent.futures.ThreadPoolExecutor(max_workers=CFG['maximum_workers']) as pool:
  for s,rows,records in pool.map(lambda x:A.fetch_symbol(*x),todo):batches.append((s,rows,records))
 rr=[r for _,_,records in batches for r in records];ref=A.save_raw(rr,'candles');admitted=0;errors=[]
 for s,rows,records in batches:
  for rec in records:
   if rec.get('status')!=200:
    A.issue(c,'PUBLIC_FETCH_ERROR',s+': '+str(rec.get('error')));continue
   for row in json.loads(rec['raw']):
    if row[6]>=end:continue
    try:admitted+=accept(c,s,row,rec['received'],ref)
    except (ValueError,ArithmeticError) as ex:errors.append(str(ex));A.issue(c,'CANDLE_INTEGRITY',str(ex))
 A.setkv(c,'last_capture',A.clock_ms());A.setkv(c,'last_raw',ref)
 if errors:A.setkv(c,'integrity_halt',True);A.setkv(c,'stage','INTEGRITY_HALT')
 c.commit()
 if errors:raise IntegrityError('; '.join(errors))
 return admitted,rr

def valid_book(raw,rec):
 if rec.get('status')!=200 or rec.get('duration',99)>CFG['maximum_book_request_seconds']:return False
 try:return bool(raw['asks'] and raw['bids'] and 0<float(raw['bids'][0][0])<float(raw['asks'][0][0]))
 except (KeyError,TypeError,ValueError,IndexError):return False

def books(c,symbols,label='books'):
 rr=[];out={}
 with concurrent.futures.ThreadPoolExecutor(max_workers=CFG['maximum_workers']) as pool:
  for s,(raw,rec) in zip(sorted(symbols),pool.map(lambda s:A.request('depth',{'symbol':s,'limit':100}),sorted(symbols))):
   rr.append(rec)
   if valid_book(raw,rec):out[s]=(raw,rec)
   else:A.issue(c,'BOOK_UNAVAILABLE',s)
 ref=A.save_raw(rr,label) if rr else '';return out,ref

def entry_books_since(observed,available):return {s:v for s,v in observed.items() if v[1]['received']>=available}

def process(c,data,cohort,f):
 now=A.clock_ms();members={x['symbol']:x for x in cohort['members']}
 last=A.getkv(c,'last_tick',now)
 if now-last>120000 and now>=f['start']:A.issue(c,'COLLECTOR_DOWNTIME',str((now-last)/1000)+' seconds; missed books not recoverable')
 checked=A.getkv(c,'metadata_checked',0)
 if now-checked>=86400000:
  info,rec=A.request('exchangeInfo',{'symbols':json.dumps(cohort['collected_symbols'],separators=(',',':')),'showPermissionSets':'false'});A.save_raw([rec],'metadata');current={x['symbol']:x for x in info.get('symbols',[])} if info else {}
  disabled=[s for s,m in members.items() if s not in current or current[s]['status']!='TRADING' or not current[s].get('isSpotTradingAllowed') or current[s]['filters']!=m['filters']]
  A.setkv(c,'current_meta',current);A.setkv(c,'disabled_symbols',disabled);A.setkv(c,'metadata_checked',now)
  if not current:A.issue(c,'METADATA_UNAVAILABLE','New entries disabled')
 history={}
 feature_available=A.clock_ms()
 for s in cohort['collected_symbols']:
  processed=min(A.getkv(c,'processed:'+s,now),A.getkv(c,'B:processed:'+s,now))
  history[s]=[json.loads(x[0]) for x in c.execute('SELECT raw FROM bars WHERE symbol=? AND t>=? ORDER BY t',(s,processed-A.WARM*60000))]
 for s in members:signals(c,s,history[s],f,A.clock_ms())
 for s in A.getkv(c,'disabled_symbols',[]):c.execute("UPDATE events SET status='REJECTED',reason='CURRENT_SYMBOL_METADATA' WHERE symbol=? AND status IN ('PENDING','PENDING_CONFIRMATION','PENDING_RETEST')",(s,))
 c.commit();now=A.clock_ms()
 wanted={x[0] for x in c.execute("SELECT DISTINCT symbol FROM events WHERE status='PENDING' AND due<=? AND deadline>=?",(now,now))}|{x[0] for x in c.execute('SELECT DISTINCT symbol FROM positions')}
 observed,ref=books(c,wanted);prices={}
 for s in cohort['collected_symbols']:
  r=c.execute('SELECT raw,t FROM bars WHERE symbol=? ORDER BY t DESC LIMIT 1',(s,)).fetchone()
  if r:prices[s]=(float(json.loads(r[0])[4]),r[1]+60000)
 for s,(raw,rec) in observed.items():prices[s]=((float(raw['asks'][0][0])+float(raw['bids'][0][0]))/2,rec['received'])
 now=A.clock_ms()
 if now>=f['start']:
  A.mark_minute_paths(c,history,CFG,f,now);A.mark_accounts(c,prices,now,CFG);A.exits(c,prices,observed,CFG,f,now,ref);A.entries(c,entry_books_since(observed,feature_available),members,CFG,f,now,ref);A.mark_accounts(c,prices,now+1,CFG)
 A.setkv(c,'last_tick',now);A.setkv(c,'stage','WARMUP' if now<f['start'] else ('COLLECTING' if now<f['end'] else 'SETTLEMENT_TAIL'))
 # Only feature-cache warmup older than the fixed 32-day horizon is evicted. All post-start bars and immutable archives remain.
 cutoff=min(f['start'],now-POLICY['retention_days']*86400000)
 c.execute('DELETE FROM bars WHERE t<?',(cutoff,));c.execute('DELETE FROM admissions WHERE t<?',(cutoff,));c.execute('DELETE FROM pending WHERE t<?',(cutoff,));c.commit()

def status(c,cohort,f):
 coverage=[]
 for s in cohort['collected_symbols']:
  r=c.execute('SELECT COUNT(*),MAX(t) FROM bars WHERE symbol=?',(s,)).fetchone();coverage.append({'symbol':s,'bars':r[0],'last':r[1],'close_age_seconds':None if r[1] is None else (A.clock_ms()-r[1]-60000)/1000})
 return {'stage':A.getkv(c,'stage','QUALIFICATION'),'last_capture':A.getkv(c,'last_capture'),'last_tick':A.getkv(c,'last_tick'),'last_raw':A.getkv(c,'last_raw'),'integrity_halt':A.getkv(c,'integrity_halt',False),'coverage':coverage,'pending':c.execute('SELECT COUNT(*) FROM pending').fetchone()[0],'accounts':c.execute('SELECT COUNT(*) FROM accounts').fetchone()[0],'positions':c.execute('SELECT COUNT(*) FROM positions').fetchone()[0],'issues':[dict(x) for x in c.execute('SELECT * FROM issues ORDER BY id DESC LIMIT 12')],'performance_evaluation':'DEFERRED_TO_FIXED_END','window':f}
