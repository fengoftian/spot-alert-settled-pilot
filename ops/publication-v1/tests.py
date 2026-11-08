"""Focused tests only. No market requests or frozen test-result writes."""
import base64
import importlib.util
import json
import pathlib
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'ops/storage-v1'))
sys.path.insert(0, str(HERE))
import persistence
import publication_identity
import push_adapter
from test_push_adapter import PublicationTests


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class IntegrationTests(unittest.TestCase):
    def test_original_failure_is_reproduced_without_remote_check(self):
        calls = []

        def git(root, *args):
            calls.append(args)
            if args == ('rev-parse', 'HEAD'):
                return 'local'
            raise subprocess.TimeoutExpired(('git', 'push'), 180)

        with patch.object(persistence, 'git', git):
            with self.assertRaises(subprocess.TimeoutExpired):
                persistence.push('/fixture', 'observations')
        self.assertEqual(calls, [('rev-parse', 'HEAD'), ('push', 'origin', 'HEAD:observations')])

    def fixture(self, root):
        origin = root / 'origin.git'; work = root / 'work'
        subprocess.run(['git', 'init', '--bare', str(origin)], check=True, capture_output=True)
        subprocess.run(['git', 'clone', str(origin), str(work)], check=True, capture_output=True)
        (work / 'journal').mkdir()
        (work / 'journal/00000001.json.gz').write_bytes(b'immutable fixture journal')
        persistence.git(work, 'add', '.')
        persistence.git(work, '-c', 'user.name=test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'fixture')
        return origin, work

    def test_real_git_push_timeout_after_acceptance_is_not_repeated(self):
        with tempfile.TemporaryDirectory() as tmp:
            origin, work = self.fixture(pathlib.Path(tmp))
            pushes = []

            def git(root, *args):
                result = persistence.git(root, *args)
                if args[0] == 'push':
                    pushes.append(args)
                    raise subprocess.TimeoutExpired(('git', 'push'), 180)
                return result

            push_adapter.make_push(git)(work, 'observations')
            self.assertEqual(len(pushes), 1)
            self.assertEqual(persistence.git(origin, 'rev-parse', 'observations'), persistence.git(work, 'rev-parse', 'HEAD'))
            self.assertEqual(persistence.git(origin, 'show', 'observations:journal/00000001.json.gz'), 'immutable fixture journal')

    def test_real_git_timeout_before_delivery_retries_without_new_commit(self):
        with tempfile.TemporaryDirectory() as tmp:
            origin, work = self.fixture(pathlib.Path(tmp))
            persistence.git(work, 'push', 'origin', 'HEAD:observations')
            previous = persistence.git(work, 'rev-parse', 'HEAD')
            (work / 'journal/00000002.json.gz').write_bytes(b'second immutable journal')
            persistence.git(work, 'add', '.')
            persistence.git(work, '-c', 'user.name=test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'append')
            local = persistence.git(work, 'rev-parse', 'HEAD'); pushes = []; pauses = []

            def git(root, *args):
                if args[0] == 'push':
                    pushes.append(args)
                    if len(pushes) == 1:
                        raise subprocess.TimeoutExpired(('git', 'push'), 180)
                return persistence.git(root, *args)

            push_adapter.make_push(git, pauses.append)(work, 'observations')
            self.assertEqual(len(pushes), 2)
            self.assertEqual(pauses, [3])
            self.assertEqual(persistence.git(work, 'rev-parse', 'HEAD'), local)
            self.assertEqual(persistence.git(origin, 'rev-parse', 'observations'), local)
            self.assertEqual(persistence.git(origin, 'rev-parse', 'observations^'), previous)

    def test_identity_preserves_all_scientific_and_storage_bytes(self):
        identity = publication_identity.check(ROOT)
        self.assertEqual(len(identity), 64)
        self.assertEqual(len(json.loads((ROOT / 'FREEZE.json').read_text())['hashes']), 26)
        self.assertEqual(len(json.loads((ROOT / 'ops/storage-v1/IDENTITY.json').read_text())['hashes']), 7)

    def test_collector_installs_actual_binding_and_preserves_routing(self):
        code = """
import importlib.util,pathlib,sys
p=pathlib.Path(sys.argv[1]);sys.path.insert(0,str(p.parent))
s=importlib.util.spec_from_file_location('new_collect',p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
m.configure();assert m.cloud.push is m.persistence.push
assert m.cloud.push.__module__=='push_adapter'
calls=[];r=m.io.routed_run(lambda *a,**k:calls.append(a))
r('gh','workflow','run','collector.yml','--repo',m.persistence.REPO,'--ref','main','-f','mode=collect')
r('gh','api','repos/'+m.persistence.REPO+'/actions/workflows/collector.yml/disable','--method','PUT')
assert calls[0][3]=='collector-ops-v2.yml' and calls[0][-2:]==('-f','mode=collect')
assert '/collector-ops-v2.yml/disable' in calls[1][2]
"""
        subprocess.run([sys.executable, '-B', '-c', code, str(HERE / 'collect.py')], check=True)

    def test_observer_preserves_dedup_payload_and_routes_own_tail_disable(self):
        module = load('publication_observe_test', HERE / 'observe.py')
        calls = []
        original = module.health.api
        try:
            module.health.api = lambda *a: calls.append(a)
            identity = module.configure()
            payload = {'problems': ['STALE'], 'incident': 'COLLECTION', 'unavailable_checks': 0}
            body = {'branch': 'health-state', 'content': base64.b64encode(json.dumps(payload).encode()).decode()}
            module.health.api('repos/' + module.health.REPO + '/contents/health.json', 'PUT', body)
            written = json.loads(base64.b64decode(calls[0][2]['content']))
            for k, v in payload.items():
                self.assertEqual(written[k], v)
            self.assertEqual(written['operations_identity_sha256'], identity)
            module.health.api('repos/' + module.health.REPO + '/actions/workflows/health.yml/disable', 'PUT')
            self.assertIn('/health-ops-v2.yml/disable', calls[1][0])
        finally:
            module.health.api = original

    def test_workflows_retain_public_runners_concurrency_and_activation_gates(self):
        for name, group, flag in [('collector-ops-v2.yml', 'settled-study-single-writer', 'PUB_COLLECT_ENABLED'),
                                  ('health-ops-v2.yml', 'settled-independent-health', 'PUB_HEALTH_ENABLED')]:
            text = (ROOT / '.github/workflows' / name).read_text()
            self.assertIn('runs-on: ubuntu-24.04', text)
            self.assertIn('group: ' + group, text)
            self.assertIn('cancel-in-progress: false', text)
            self.assertIn("if: vars." + flag + " == 'true'", text)
        collector = (ROOT / '.github/workflows/collector-ops-v2.yml').read_text()
        self.assertIn("cron: '43 */2 * * *'", collector)
        self.assertIn('options: [collect]', collector)
        observer = (ROOT / '.github/workflows/health-ops-v2.yml').read_text()
        self.assertIn("cron: '11,26,41,56 * * * *'", observer)
        qualify = (ROOT / '.github/workflows/qualify-publication-v1.yml').read_text()
        self.assertNotIn('collect.py', qualify)
        self.assertNotIn('actions/upload-artifact', qualify)


if __name__ == '__main__':
    unittest.main(verbosity=2)
