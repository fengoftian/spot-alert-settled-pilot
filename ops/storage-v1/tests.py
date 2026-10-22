"""Focused storage tests only; never writes the frozen TEST_RESULTS.json."""
import base64
import gzip
import hashlib
import json
import pathlib
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import adapter
import persistence
import io_adapter as io


class StorageTests(unittest.TestCase):
    def test_original_and_sharded_receipts_preserve_every_observation(self):
        raw = '[1,"same UTF8 observation 你好"]'
        record = {'raw': raw, 'sha256': hashlib.sha256(raw.encode()).hexdigest(),
                  'status': 200, 'started': 11, 'received': 13, 'duration': 2.0}
        records = [record, dict(record, started=31, received=33), {'error': 'timeout', 'received': 45}]
        with tempfile.TemporaryDirectory() as tmp:
            old = pathlib.Path(tmp) / 'old'; new = pathlib.Path(tmp) / 'new'
            old.mkdir(); new.mkdir()
            fake = types.SimpleNamespace(ROOT=new, clock_ms=lambda: 1000)
            with patch.object(adapter.A, 'ROOT', old), patch.object(adapter.A, 'clock_ms', return_value=1000), patch.object(io.uuid, 'uuid4', return_value=types.SimpleNamespace(hex='0' * 32)):
                left_ref = adapter.save_raw(records, 'candles')
                right_ref = io.writer(fake)(records, 'candles')
            left = json.loads(gzip.decompress((old / left_ref).read_bytes()))
            right = json.loads(gzip.decompress((new / right_ref).read_bytes()))
            self.assertEqual(left_ref, right_ref)
            self.assertEqual(len(right), 3)
            for a, b in zip(left, right):
                if 'body_ref' in a:
                    self.assertTrue(b['body_ref'].startswith(io.PREFIX + '/'))
                    self.assertEqual(gzip.decompress((old / a.pop('body_ref')).read_bytes()),
                                     gzip.decompress((new / b.pop('body_ref')).read_bytes()))
                self.assertEqual(a, b)
            self.assertIn('raw', record)
            self.assertEqual(len(list((new / io.PREFIX).rglob('*.json.gz'))), 1)
            self.assertFalse((new / 'raw/bodies').exists())

    def test_invalid_body_hash_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake = types.SimpleNamespace(ROOT=pathlib.Path(tmp), clock_ms=lambda: 1)
            with self.assertRaisesRegex(RuntimeError, 'hash mismatch'):
                io.writer(fake)([{'raw': 'x', 'sha256': '0' * 64}], 'bad')

    def test_existing_body_cannot_be_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp); digest = hashlib.sha256(b'x').hexdigest()
            path = root / io.PREFIX / digest[:2] / (digest + '.json.gz')
            path.parent.mkdir(parents=True); path.write_bytes(gzip.compress(b'wrong', mtime=0))
            fake = types.SimpleNamespace(ROOT=root, clock_ms=lambda: 1)
            with self.assertRaisesRegex(RuntimeError, 'collision'):
                io.writer(fake)([{'raw': 'x', 'sha256': digest}], 'bad')
            self.assertEqual(gzip.decompress(path.read_bytes()), b'wrong')

    def test_successors_and_halt_disable_use_same_operations_writer(self):
        calls = []
        run = io.routed_run(lambda *a, **k: calls.append((a, k)))
        run('gh', 'workflow', 'run', 'collector.yml', '--repo', persistence.REPO)
        run('gh', 'api', 'repos/' + persistence.REPO + '/actions/workflows/collector.yml/disable', '--method', 'PUT')
        run('git', 'push', 'origin', 'HEAD:observations')
        self.assertEqual(calls[0][0][3], io.COLLECTOR_WORKFLOW)
        self.assertIn('/' + io.COLLECTOR_WORKFLOW + '/disable', calls[1][0][2])
        self.assertEqual(calls[2][0], ('git', 'push', 'origin', 'HEAD:observations'))

    def test_small_blob_identity(self):
        data = b'journal'; digest = hashlib.sha1(b'blob 7\0' + data).hexdigest()
        self.assertEqual(io.read_blob(lambda p: {'encoding': 'base64', 'content': base64.b64encode(data).decode(), 'sha': digest}, 'journal/x', 'pinned'), data)

    def test_large_blob_uses_raw_media_and_checks_identity(self):
        data = b'x' * (1024 * 1024 + 1)
        digest = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
        with patch.object(io.subprocess, 'run', return_value=types.SimpleNamespace(stdout=data)) as run:
            self.assertEqual(io.read_blob(lambda p: {'encoding': 'none', 'content': '', 'sha': digest}, 'journal/x', 'pinned'), data)
            self.assertIn('Accept: application/vnd.github.raw', run.call_args.args[0])

    def test_blob_mismatch_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, 'identity mismatch'):
            io.read_blob(lambda p: {'encoding': 'base64', 'content': 'eA==', 'sha': '0' * 40}, 'journal/x', 'pinned')

    def test_sparse_publication_preserves_legacy_tree_and_appends_raw(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = pathlib.Path(tmp); origin = tmp / 'origin.git'; seed = tmp / 'seed'; root = tmp / 'source'; dest = tmp / 'observations'
            subprocess.run(['git', 'init', '--bare', str(origin)], check=True, capture_output=True)
            subprocess.run(['git', 'clone', str(origin), str(seed)], check=True, capture_output=True)
            (seed / 'raw/bodies').mkdir(parents=True)
            (seed / 'raw/bodies/old.json.gz').write_bytes(b'legacy-body')
            (seed / 'raw/old.json.gz').write_bytes(b'legacy-receipt')
            (seed / 'SEED.json').write_text('{}')
            persistence.git(seed, 'add', '.')
            persistence.git(seed, '-c', 'user.name=test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'initial')
            persistence.git(seed, 'push', 'origin', 'HEAD:main', 'HEAD:observations')
            subprocess.run(['git', '-C', str(origin), 'symbolic-ref', 'HEAD', 'refs/heads/main'], check=True)
            subprocess.run(['git', '-C', str(origin), 'config', 'uploadpack.allowFilter', 'true'], check=True)
            subprocess.run(['git', 'clone', origin.as_uri(), str(root)], check=True, capture_output=True)
            before = persistence.git(root, 'rev-parse', 'HEAD:raw/bodies')
            io.sparse_checkout(persistence, root, dest, 'observations')
            self.assertTrue((dest / 'SEED.json').exists())
            self.assertFalse((dest / 'raw/bodies/old.json.gz').exists())
            source = tmp / 'runtime'; source.mkdir()
            fake = types.SimpleNamespace(ROOT=source, clock_ms=lambda: 1000)
            ref = io.writer(fake)([{'raw': 'new', 'sha256': hashlib.sha256(b'new').hexdigest()}], 'candles')
            persistence.archive(source, dest)
            io.sparse_commit(persistence, dest, 'append sharded data')
            self.assertEqual(persistence.git(dest, 'rev-parse', 'HEAD:raw/bodies'), before)
            self.assertEqual(persistence.git(dest, 'show', 'HEAD:raw/old.json.gz'), 'legacy-receipt')
            self.assertEqual(persistence.git(dest, 'ls-tree', 'HEAD', ref).split()[1], 'blob')
            persistence.push(dest, 'observations')
            self.assertEqual(persistence.git(origin, 'rev-parse', 'observations:raw/bodies'), before)

    def test_new_checkout_or_other_branch_is_refused(self):
        for branch, create in [('main', False), ('observations', True)]:
            with self.assertRaises(RuntimeError):
                io.sparse_checkout(persistence, ROOT, ROOT / 'unused', branch, create)

    def test_frozen_science_and_operations_identity(self):
        self.assertEqual(len(json.loads((ROOT / 'FREEZE.json').read_text())['hashes']), 26)
        self.assertEqual(len(io.check_identity(ROOT)), 64)


if __name__ == '__main__':
    unittest.main(verbosity=2)
