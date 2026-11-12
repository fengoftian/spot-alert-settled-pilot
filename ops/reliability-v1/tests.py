"""Offline failure/containment tests; no market data or account outcomes."""
import json
import pathlib
import subprocess
import sys
import unittest
from unittest.mock import patch
import runner as r


class RecoveryTests(unittest.TestCase):
    def decision(self, **changes):
        args = dict(status={'integrity_halt': False, 'stage': 'COLLECTING'},
                    producer={'id': 42, 'status': 'completed', 'conclusion': 'failure'},
                    active=[], enabled=True, now=10000000, tail=20000000,
                    completed=False, marker={'recoverable': True, 'integrity_or_disk_halt': False}, ledger=[])
        args.update(changes)
        return r.choose(**args)

    def test_publication_failure_recovery_and_post_tail_finalization(self):
        self.assertEqual(self.decision(), 'recover_publication')
        self.assertEqual(self.decision(now=20000001), 'recover_final')

    def test_integrity_and_disk_halts_never_resume(self):
        for status in ({'integrity_halt': True}, {'stage': 'DISK_CAP_HALT'}):
            self.assertEqual(self.decision(status=status), 'attention_halt')

    def test_every_queued_or_running_writer_blocks_recovery(self):
        for state in ('in_progress', 'queued', 'pending', 'waiting', 'requested'):
            self.assertEqual(self.decision(active=[{'status': state}]), 'wait_for_writer')

    def test_unknown_failures_and_no_marker_remain_closed(self):
        for marker in (None, {}, {'recoverable': False}, {'recoverable': True, 'integrity_or_disk_halt': True}):
            self.assertEqual(self.decision(marker=marker), 'attention_unclassified_failure')

    def test_one_claim_per_producer_and_bounded_restart_budget(self):
        self.assertEqual(self.decision(ledger=[{'producer': '42', 'at_ms': 1}]), 'already_claimed')
        self.assertEqual(self.decision(ledger=[{'producer': str(i), 'at_ms': 9999000} for i in range(3)]), 'attention_recovery_limit')
        self.assertEqual(self.decision(ledger=[{'producer': str(i), 'at_ms': 1} for i in range(3)]), 'recover_publication')

    def test_successful_lease_recovers_handoff_without_failure_marker(self):
        self.assertEqual(self.decision(producer={'id': 42, 'status': 'completed', 'conclusion': 'success'}, marker=None), 'recover_handoff')

    def test_incomplete_runs_and_disabled_gate_do_not_dispatch(self):
        self.assertEqual(self.decision(enabled=False), 'disabled')
        self.assertEqual(self.decision(producer={'id': 42, 'status': 'in_progress'}), 'wait_for_producer')

    def test_no_early_completion(self):
        with self.assertRaises(ValueError):
            self.decision(completed=True)
        self.assertEqual(self.decision(completed=True, now=20000001), 'complete')

    def test_push_timeout_is_distinct_from_reconciliation_timeout(self):
        self.assertTrue(r.retryable_publication(subprocess.TimeoutExpired(('git', 'push'), 180)))
        self.assertFalse(r.retryable_publication(subprocess.TimeoutExpired(('git', 'ls-remote'), 180)))
        self.assertFalse(r.retryable_publication(RuntimeError('Remote advanced unexpectedly; refuse overwrite')))
        self.assertFalse(r.retryable_publication(RuntimeError('unknown failure')))

    def test_claim_is_compare_and_swap_and_not_retried_on_failure(self):
        with patch.object(r, 'api', side_effect=RuntimeError('conflict')) as api:
            with self.assertRaises(RuntimeError):
                r.claim([], 'old-sha', 42, 100)
        self.assertEqual(api.call_count, 1)
        self.assertEqual(api.call_args.args[1], 'PUT')
        self.assertEqual(api.call_args.args[2]['sha'], 'old-sha')

    def test_final_validation_requires_same_window_and_complete_identities(self):
        cfg = json.loads((r.ROOT / 'config.json').read_text())
        freeze = json.loads((r.ROOT / 'FREEZE.json').read_text())
        accounts = [{'account': f'{m}:{c}:{s}'} for m in cfg['models'] for c in cfg['capitals'] for s in cfg['slippage_bps']]
        report = {'window': freeze, 'cagr': None, 'accounts': accounts}
        diagnostics = {'no_annualization': True,
                       'contrasts': [dict(contrast=n, capital=c, slip=s) for n in freeze['comparisons'] for c in cfg['capitals'] for s in cfg['slippage_bps']],
                       'per_account_samples': {x['account']: {} for x in accounts}}
        self.assertTrue(r.validate_final(report, diagnostics, freeze, cfg))
        self.assertFalse(r.validate_final(dict(report, accounts=accounts[:-1]), diagnostics, freeze, cfg))
        self.assertFalse(r.validate_final(dict(report, window={}), diagnostics, freeze, cfg))
        self.assertFalse(r.validate_final(report, dict(diagnostics, contrasts=[]), freeze, cfg))
        self.assertFalse(r.validate_final(report, dict(diagnostics, per_account_samples={}), freeze, cfg))

    def test_preserved_and_new_source_identity(self):
        self.assertEqual(len(r.identity()), 64)

    def test_actual_cloud_bindings_emit_safe_marker_without_changing_frozen_files(self):
        script = '''
import contextlib,io,json,pathlib,sqlite3,subprocess,tempfile,sys,os
sys.path.insert(0,sys.argv[1]);import runner
sys.path.insert(0,str(runner.ROOT));import cloud,persistence
def git(root,*args):
 if args[0]=='rev-parse':return 'local'
 if args[0]=='push':raise subprocess.TimeoutExpired(('git','push'),180)
 if args[0]=='ls-remote':return 'old refs/heads/observations'
 return ''
persistence.git=git
cloud.persist=lambda obs,c,*args:cloud.push(obs,'observations')
os.environ['GITHUB_RUN_ID']='42';os.environ['GITHUB_SHA']='source'
runner.configure_collector()
with tempfile.TemporaryDirectory() as td:
 c=sqlite3.connect(':memory:');c.execute('CREATE TABLE kv(k TEXT PRIMARY KEY,v TEXT)')
 for halted in (False,True):
  c.execute('INSERT OR REPLACE INTO kv VALUES (?,?)',('integrity_halt',json.dumps(halted)))
  buf=io.StringIO()
  with contextlib.redirect_stdout(buf):
   try:cloud.persist(pathlib.Path(td),c,{}, {},0,None)
   except subprocess.TimeoutExpired:pass
  line=next(x for x in buf.getvalue().splitlines() if runner.MARKER in x)
  marker=json.loads(line.split(runner.MARKER)[1])
  assert marker['recoverable'] is (not halted)
  assert marker['integrity_or_disk_halt'] is halted
  assert marker['run_id']=='42'
 assert cloud.push is persistence.push
runner.identity()
'''
        p = subprocess.run([sys.executable, '-c', script, str(r.HERE)], capture_output=True, text=True, timeout=45)
        self.assertEqual(p.returncode, 0, p.stderr)


if __name__ == '__main__':
    unittest.main(verbosity=2)
