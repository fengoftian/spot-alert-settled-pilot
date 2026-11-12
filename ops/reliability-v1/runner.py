"""Operational recovery only; frozen collector and report calculations are reused."""
import base64
import datetime
import gzip
import hashlib
import importlib.util
import json
import os
import pathlib
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
HERE = pathlib.Path(__file__).resolve().parent
REPO = 'fengoftian/spot-alert-settled-pilot'
PREFIX = 'repos/' + REPO + '/'
COLLECTOR = 'collector-ops-v3.yml'
WATCHDOG = 'health-ops-v3.yml'
MARKER = 'OPS_PUBLICATION_FAILURE '
PREDECESSOR = 'd1f1749ea0e7678840ae2f138c2c33f5d7f90461'


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def identity():
    manifest = json.loads((HERE / 'IDENTITY.json').read_text())
    for name, expected in manifest['preserved_hashes'].items():
        if digest((ROOT / name).read_bytes()) != expected:
            raise RuntimeError('Preserved identity changed: ' + name)
    for name, expected in manifest['hashes'].items():
        if digest((ROOT / name).read_bytes()) != expected:
            raise RuntimeError('Reliability identity changed: ' + name)
    return digest((HERE / 'IDENTITY.json').read_bytes())


def hosted():
    if os.environ.get('GITHUB_ACTIONS') != 'true' or os.environ.get('GITHUB_REPOSITORY') != REPO:
        raise RuntimeError('Exact hosted repository required')


def previous(name):
    sys.path.insert(0, str(ROOT))
    sys.path.insert(0, str(ROOT / 'ops/storage-v1'))
    sys.path.insert(0, str(ROOT / 'ops/publication-v1'))
    spec = importlib.util.spec_from_file_location('previous_' + name, ROOT / 'ops/publication-v1' / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def retryable_publication(error):
    if isinstance(error, subprocess.TimeoutExpired):
        return 'push' in error.cmd
    # Refusal, unavailable reconciliation, and other unknown failures stay closed.
    text = str(error).lower()
    return isinstance(error, RuntimeError) and any(x in text for x in
        ('rpc failed', 'remote end hung up', 'http 502', 'http 503', 'http 504')) and 'refuse overwrite' not in text


def configure_collector():
    own = identity()
    old = previous('collect')
    old.configure()
    import cloud
    import persistence
    import io_adapter
    io_adapter.COLLECTOR_WORKFLOW = COLLECTOR
    write = persistence.write

    def provenance(path, obj):
        if pathlib.Path(path).name == 'OPERATIONS_STATUS.json':
            obj = dict(obj, reliability_identity_sha256=own,
                       event_driven_recovery=True, scientific_formulas_changed=False)
        return write(path, obj)
    persistence.write = provenance
    push = cloud.push
    state = {'connection': None}

    def monitored_push(root, branch, attempts=3):
        try:
            return push(root, branch, attempts)
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            c = state['connection']
            halted = c is None or cloud.ad.A.getkv(c, 'integrity_halt', False) or cloud.ad.A.getkv(c, 'stage') == 'DISK_CAP_HALT'
            record = dict(run_id=os.environ.get('GITHUB_RUN_ID'), source=os.environ.get('GITHUB_SHA'),
                          recoverable=not halted and retryable_publication(error),
                          integrity_or_disk_halt=bool(halted), error_type=type(error).__name__,
                          fixed_tail=cloud.freeze()['tail_end'])
            print(MARKER + json.dumps(record, sort_keys=True), flush=True)
            raise
    cloud.push = persistence.push = monitored_push
    persist = cloud.persist

    def tracked(obs, connection, *args):
        state['connection'] = connection
        return persist(obs, connection, *args)
    cloud.persist = tracked
    return cloud


def api(path, method='GET', body=None, raw=False):
    if path != PREFIX.rstrip('/') and not path.startswith(PREFIX):
        raise RuntimeError('Unexpected repository')
    args = ['gh', 'api', path, '--method', method]
    if raw:
        args += ['-H', 'Accept: application/vnd.github.raw']
    if body is not None:
        args += ['--input', '-']
    for attempt in range(2 if method == 'GET' else 1):
        try:
            p = subprocess.run(args, input=json.dumps(body) if body is not None else None,
                               capture_output=True, text=True, timeout=30)
            if p.returncode:
                raise RuntimeError(p.stderr[:300])
            return p.stdout if raw else (json.loads(p.stdout) if p.stdout.strip() else None)
        except (RuntimeError, subprocess.TimeoutExpired):
            if method != 'GET' or attempt:
                raise
            time.sleep(1)


def file(path, ref):
    meta = api(PREFIX + 'contents/' + path + '?ref=' + ref)
    raw = base64.b64decode(meta['content']) if meta.get('encoding') == 'base64' else api(PREFIX + 'contents/' + path + '?ref=' + ref, raw=True).encode()
    if hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest() != meta['sha']:
        raise RuntimeError('Git blob mismatch: ' + path)
    return raw


def choose(status, producer, active, enabled, now, tail, completed, marker, ledger):
    if completed:
        if now < tail:
            raise ValueError('Final artifacts cannot complete a future window')
        return 'complete'
    if not enabled:
        return 'disabled'
    if status.get('integrity_halt') or status.get('stage') == 'DISK_CAP_HALT':
        return 'attention_halt'
    if active:
        return 'wait_for_writer'
    run = str(producer['id'])
    if any(x['producer'] == run for x in ledger):
        return 'already_claimed'
    if sum(now - x['at_ms'] < 3600000 for x in ledger) >= 3:
        return 'attention_recovery_limit'
    if producer['status'] != 'completed':
        return 'wait_for_producer'
    if producer.get('conclusion') == 'success':
        return 'recover_final' if now >= tail else 'recover_handoff'
    if marker and marker.get('recoverable') is True and marker.get('integrity_or_disk_halt') is False:
        return 'recover_final' if now >= tail else 'recover_publication'
    return 'attention_unclassified_failure'


def validate_final(result, diagnostics, freeze, cfg):
    expected = {f'{m}:{cap}:{slip}' for m in cfg['models'] for cap in cfg['capitals'] for slip in cfg['slippage_bps']}
    contrasts = {(name, cap, slip) for name in freeze['comparisons'] for cap in cfg['capitals'] for slip in cfg['slippage_bps']}
    actual = {(x.get('contrast'), x.get('capital'), x.get('slip')) for x in diagnostics.get('contrasts', [])}
    return (result.get('window') == freeze and result.get('cagr') is None
            and {x['account'] for x in result.get('accounts', [])} == expected
            and len(result['accounts']) == len(expected) == 81
            and diagnostics.get('no_annualization') is True
            and actual == contrasts and len(diagnostics.get('contrasts', [])) == 63
            and set(diagnostics.get('per_account_samples', {})) == expected)


def ledger_read():
    try:
        old = api(PREFIX + 'contents/recovery.json?ref=health-state')
    except RuntimeError as error:
        if '404' in str(error):
            return [], None
        raise
    return json.loads(base64.b64decode(old['content']))['claims'], old['sha']


def claim(ledger, sha, producer, now):
    claims = ledger + [dict(producer=str(producer), at_ms=now)]
    body = dict(branch='health-state', message='Claim bounded collector recovery',
                content=base64.b64encode(json.dumps({'claims': claims}, sort_keys=True).encode()).decode())
    if sha:
        body['sha'] = sha
    # A conflict or ambiguous write aborts; it must not cause a duplicate dispatch.
    api(PREFIX + 'contents/recovery.json', 'PUT', body)


def observe():
    hosted()
    own = identity()
    source = os.environ['GITHUB_SHA']
    if api(PREFIX.rstrip('/'))['private']:
        raise RuntimeError('Standard public runner scope required')
    if api(PREFIX + 'branches/main')['commit']['sha'] != source:
        raise RuntimeError('Observer source is not current approved main')
    old = previous('observe')
    old.configure()
    import health
    old_api = health.api

    def observed_api(path, method='GET', body=None):
        if path.endswith('/disable'):
            return None  # Disable only after verifying persisted final artifacts below.
        if path == PREFIX + 'contents/health.json' and method == 'PUT':
            body = dict(body)
            result = json.loads(base64.b64decode(body['content']))
            result['reliability_identity_sha256'] = own
            body['content'] = base64.b64encode((json.dumps(result, indent=2) + '\n').encode()).decode()
        return old_api(path, method, body)
    health.api = observed_api
    health_result = health.main()
    head = api(PREFIX + 'branches/observations')['commit']['sha']
    status = json.loads(file('STATUS.json', head))
    cloud = json.loads(file('CLOUD_STATUS.json', head))
    seed = json.loads(file('SEED.json', head))
    freeze = json.loads((ROOT / 'FREEZE.json').read_text())
    if seed['freeze_sha256'] != cloud['freeze_sha256'] or seed['freeze_sha256'] != digest((ROOT / 'FREEZE.json').read_bytes()):
        raise RuntimeError('Frozen seed identity mismatch')
    proof = file(f"journal/{cloud['checkpoint_sequence']:08d}.json.gz", head)
    if digest(proof) != cloud['checkpoint_sha256'] or json.loads(gzip.decompress(proof))['sequence'] != cloud['checkpoint_sequence']:
        raise RuntimeError('Current checkpoint identity mismatch')
    producer = api(PREFIX + 'actions/runs/' + cloud['run_id'])
    if producer['head_sha'] != cloud['source_commit'] or cloud['source_commit'] != cloud['workflow_head_sha'] or producer['head_sha'] not in (source, PREDECESSOR):
        raise RuntimeError('Producer provenance mismatch')
    # A failed push may leave no checkpoint from the newest run. Diagnose that
    # exact newer run rather than repeatedly inspecting the older saved producer.
    candidates = api(PREFIX + 'actions/workflows/' + COLLECTOR + '/runs?per_page=20')['workflow_runs']
    if candidates:
        newest = candidates[0]
        if newest['created_at'] >= producer['created_at']:
            if newest['head_sha'] != source:
                raise RuntimeError('Latest collector source is not approved main')
            producer = newest
    active = []
    for state in ('in_progress', 'queued', 'pending', 'waiting', 'requested'):
        active += [x for x in api(PREFIX + 'actions/runs?status=' + state + '&per_page=100')['workflow_runs'] if 'collector' in x['path']]
    variables = {x['name']: x['value'] for x in api(PREFIX + 'actions/variables')['variables']}
    if any(variables.get(x) != 'false' for x in ('COLLECT_ENABLED', 'OPS_COLLECT_ENABLED', 'PUB_COLLECT_ENABLED')):
        raise RuntimeError('Predecessor collector gate is enabled')
    now = int(time.time() * 1000)
    completed = False
    if now >= freeze['tail_end']:
        try:
            raw = {n: file(n, head) for n in ('RESULTS.json', 'RESULTS.md', 'DIAGNOSTICS.json')}
        except RuntimeError as error:
            if '404' not in str(error):
                raise
        else:
            completed = validate_final(json.loads(raw['RESULTS.json']), json.loads(raw['DIAGNOSTICS.json']), freeze, json.loads((ROOT / 'config.json').read_text()))
            if not completed:
                raise RuntimeError('Persisted final artifacts failed validation')
    ledger, ledger_sha = ledger_read()
    marker = None
    if producer['status'] == 'completed' and producer.get('conclusion') == 'failure':
        jobs = api(PREFIX + 'actions/runs/' + str(producer['id']) + '/jobs')['jobs']
        for job in jobs:
            if job['conclusion'] != 'failure':
                continue
            log = api(PREFIX + 'actions/jobs/' + str(job['id']) + '/logs', raw=True)
            for line in log.splitlines():
                if MARKER in line:
                    marker = json.loads(line.split(MARKER, 1)[1])
        if marker and (marker.get('run_id') != str(producer['id']) or marker.get('source') != producer['head_sha'] or marker.get('fixed_tail') != freeze['tail_end']):
            raise RuntimeError('Recovery marker identity mismatch')
    action = choose(status, producer, active, variables.get('RELIABILITY_COLLECT_ENABLED') == 'true', now, freeze['tail_end'], completed, marker, ledger)
    if action.startswith('recover_'):
        # Recheck immediately before claiming; GitHub's shared concurrency group
        # is the final single-writer exclusion across all collector versions.
        if api(PREFIX + 'branches/observations')['commit']['sha'] != head:
            action = 'wait_for_new_checkpoint'
        else:
            claim(ledger, ledger_sha, producer['id'], now)
            api(PREFIX + 'actions/workflows/' + COLLECTOR + '/dispatches', 'POST', {'ref': 'main', 'inputs': {'mode': 'collect'}})
    elif action == 'complete':
        for workflow in (COLLECTOR, WATCHDOG):
            api(PREFIX + 'actions/workflows/' + workflow + '/disable', 'PUT')
    print(json.dumps(dict(action=action, observations_commit=head, checkpoint=cloud['checkpoint_sequence'],
                          integrity_halt=status['integrity_halt'], final_artifacts_verified=completed,
                          reliability_identity_sha256=own, economics_inspected=now >= freeze['tail_end']), sort_keys=True), flush=True)
    if action.startswith('attention_'):
        return 1
    return health_result if action not in ('complete', 'recover_final', 'recover_handoff', 'recover_publication') else 0


if __name__ == '__main__':
    if sys.argv[1] == 'collect':
        hosted()
        configure_collector().collect()
    elif sys.argv[1] == 'observe':
        raise SystemExit(observe())
    else:
        raise SystemExit('Unsupported mode')
