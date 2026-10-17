"""Append-only publication with bounded same-commit retries; no force push or rebase."""
import hashlib,json,pathlib,subprocess,time
REPO='fengoftian/spot-alert-settled-pilot'
def run(*args,cwd=None):
 p=subprocess.run(args,cwd=cwd,capture_output=True,text=True,timeout=180)
 if p.returncode:raise RuntimeError(p.stderr[-1500:])
 return p.stdout.strip()
def git(root,*args):return run('git','-C',str(root),*args)
def push(root,branch,attempts=3):
 local=git(root,'rev-parse','HEAD');last=None
 for attempt in range(attempts):
  try:git(root,'push','origin','HEAD:'+branch);return
  except RuntimeError as ex:
   last=ex
   remote=git(root,'ls-remote','origin','refs/heads/'+branch).split()
   if remote and remote[0]==local:return  # ambiguous delivery succeeded
   if remote:
    git(root,'fetch','--depth=1','origin',branch)
    try:git(root,'merge-base','--is-ancestor',remote[0],local)
    except RuntimeError:raise RuntimeError('Remote advanced unexpectedly; refuse overwrite') from ex
   if attempt+1<attempts:time.sleep([3,9][attempt])
 raise last
def commit(root,message):
 git(root,'add','--','.')
 p=subprocess.run(['git','-C',str(root),'diff','--cached','--quiet'])
 if p.returncode:git(root,'-c','user.name=github-actions[bot]','-c','user.email=41898282+github-actions[bot]@users.noreply.github.com','commit','-m',message)
def checkout(root,dest,branch,create=False):
 dest=pathlib.Path(dest)
 refs=git(root,'ls-remote','origin','refs/heads/'+branch)
 if refs:
  git(root,'fetch','--depth=1','origin',branch);git(root,'worktree','add','--detach',str(dest),'FETCH_HEAD')
 elif create:
  git(root,'worktree','add','--detach',str(dest),'HEAD');git(dest,'checkout','--orphan',branch);git(dest,'rm','-rf','--','.')
 else:raise RuntimeError('Required branch absent: '+branch)
 return dest
def sha(p):return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
def write(p,obj):pathlib.Path(p).write_text(json.dumps(obj,indent=2)+'\n')

def archive(data,obs):
 import shutil,gzip
 source=pathlib.Path(data)/'raw';dest=pathlib.Path(obs)/'raw'
 if not source.exists():return
 for p in source.rglob('*'):
  if not p.is_file():continue
  target=dest/p.relative_to(source);target.parent.mkdir(parents=True,exist_ok=True)
  if target.exists():
   if sha(target)!=sha(p) and not ('bodies' in p.parts and gzip.decompress(target.read_bytes())==gzip.decompress(p.read_bytes())):raise RuntimeError('Immutable raw collision')
  else:shutil.copyfile(p,target)

def clean_archived(data,obs):
 import gzip
 source=pathlib.Path(data)/'raw';dest=pathlib.Path(obs)/'raw'
 if not source.exists():return
 for p in source.rglob('*'):
  if p.is_file():
   target=dest/p.relative_to(source)
   if target.exists() and (sha(p)==sha(target) or ('bodies' in p.parts and gzip.decompress(p.read_bytes())==gzip.decompress(target.read_bytes()))):p.unlink()
