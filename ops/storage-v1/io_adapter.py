"""Storage-only operations adapter. Frozen scientific files remain unchanged."""
import base64
import gzip
import hashlib
import json
import pathlib
import subprocess
import uuid

PREFIX = 'raw/sharded-v1/bodies'
BASE_SOURCE = '9187830b9882cbc623906d5f4ed8d8dcdf8b8745'
FREEZE_SHA256 = '745b27f9b97408d44c712d741545b8503095cad310e2f4ebadd61ef464d463c5'
COLLECTOR_WORKFLOW = 'collector-ops-v1.yml'
OBSERVER_WORKFLOW = 'health-ops-v1.yml'


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def check_identity(root):
    root = pathlib.Path(root)
    manifest = root / 'ops/storage-v1/IDENTITY.json'
    identity = json.loads(manifest.read_text())
    assert identity['scientific_source'] == BASE_SOURCE
    assert sha(root / 'FREEZE.json') == identity['freeze_sha256'] == FREEZE_SHA256
    freeze = json.loads((root / 'FREEZE.json').read_text())
    for name, digest in {**freeze['hashes'], **identity['hashes']}.items():
        if sha(root / name) != digest:
            raise RuntimeError('Identity mismatch: ' + name)
    return sha(manifest)


def writer(engine):
    def save_raw(records, label):
        dest = engine.ROOT / 'raw'
        dest.mkdir(parents=True, exist_ok=True)
        saved = []
        for rec in records:
            item = dict(rec)
            if 'raw' in item:
                raw = item.pop('raw').encode()
                digest = hashlib.sha256(raw).hexdigest()
                if digest != item['sha256']:
                    raise RuntimeError('Response body hash mismatch')
                relative = pathlib.Path(PREFIX) / digest[:2] / (digest + '.json.gz')
                body = engine.ROOT / relative
                body.parent.mkdir(parents=True, exist_ok=True)
                if body.exists():
                    if gzip.decompress(body.read_bytes()) != raw:
                        raise RuntimeError('Immutable body collision')
                else:
                    body.write_bytes(gzip.compress(raw, mtime=0))
                item['body_ref'] = relative.as_posix()
            saved.append(item)
        name = f'{engine.clock_ms()}-{label}-{uuid.uuid4().hex[:8]}.json.gz'
        (dest / name).write_bytes(gzip.compress(json.dumps(saved, separators=(',', ':')).encode(), mtime=0))
        return 'raw/' + name
    return save_raw


def sparse_checkout(persistence, root, dest, branch, create=False):
    if branch != 'observations' or create:
        raise RuntimeError('Operations checkout is existing observations only')
    dest = pathlib.Path(dest)
    persistence.git(root, 'config', 'remote.origin.promisor', 'true')
    persistence.git(root, 'config', 'remote.origin.partialclonefilter', 'blob:none')
    persistence.git(root, 'fetch', '--filter=blob:none', '--depth=1', 'origin', branch)
    head = persistence.git(root, 'rev-parse', 'FETCH_HEAD')
    persistence.git(root, 'worktree', 'add', '--detach', '--no-checkout', str(dest), head)
    persistence.git(dest, 'sparse-checkout', 'set', '--no-cone', '/SEED.json', '/seed/', '/journal/',
                    '/STATUS.json', '/CLOUD_STATUS.json', '/OPERATIONS_STATUS.json',
                    '/RESULTS.json', '/RESULTS.md', '/DIAGNOSTICS.json')
    persistence.git(dest, 'checkout', '--detach', head)
    return dest


def sparse_commit(persistence, root, message):
    persistence.git(root, 'add', '--sparse', '--', '.')
    changed = subprocess.run(['git', '-C', str(root), 'diff', '--cached', '--quiet'])
    if changed.returncode not in (0, 1):
        raise RuntimeError('Unable to inspect staged changes')
    if changed.returncode:
        persistence.git(root, '-c', 'user.name=github-actions[bot]', '-c',
                        'user.email=41898282+github-actions[bot]@users.noreply.github.com',
                        'commit', '-m', message)


def routed_run(original):
    def run(*args, **kwargs):
        args = list(args)
        if args[:4] == ['gh', 'workflow', 'run', 'collector.yml']:
            args[3] = COLLECTOR_WORKFLOW
        if args[:3] == ['gh', 'api', 'repos/fengoftian/spot-alert-settled-pilot/actions/workflows/collector.yml/disable']:
            args[2] = args[2].replace('/collector.yml/', '/' + COLLECTOR_WORKFLOW + '/')
        return original(*args, **kwargs)
    return run


def read_blob(api, path, ref, repository='fengoftian/spot-alert-settled-pilot'):
    endpoint = f'repos/{repository}/contents/{path}?ref={ref}'
    meta = api(endpoint)
    if meta.get('encoding') == 'base64':
        body = base64.b64decode(meta['content'])
    else:
        body = subprocess.run(['gh', 'api', endpoint, '-H', 'Accept: application/vnd.github.raw'],
                              capture_output=True, check=True, timeout=90).stdout
    actual = hashlib.sha1(b'blob ' + str(len(body)).encode() + b'\0' + body).hexdigest()
    if actual != meta['sha']:
        raise RuntimeError('Git blob identity mismatch: ' + path)
    return body
