import subprocess
import types
import unittest
from push_adapter import install, make_push


class Git:
    def __init__(self, steps):
        self.steps = list(steps)
        self.calls = []

    def __call__(self, root, *args):
        self.calls.append((root, args))
        expected, result = self.steps.pop(0)
        if args != expected:
            raise AssertionError((args, expected))
        if isinstance(result, BaseException):
            raise result
        return result


HEAD = ('rev-parse', 'HEAD')
PUSH = ('push', 'origin', 'HEAD:observations')
REMOTE = ('ls-remote', 'origin', 'refs/heads/observations')
FETCH = ('fetch', '--depth=1', 'origin', 'observations')
ANCESTOR = ('merge-base', '--is-ancestor', 'old', 'local')


def timeout():
    return subprocess.TimeoutExpired(('git', 'push'), 180)


class PublicationTests(unittest.TestCase):
    def check(self, steps):
        git = Git(steps)
        pauses = []
        push = make_push(git, pauses.append)
        return git, pauses, push

    def test_success_needs_no_remote_reconciliation(self):
        g, pauses, push = self.check([(HEAD, 'local'), (PUSH, '')])
        push('/obs', 'observations')
        self.assertFalse(g.steps)
        self.assertEqual(pauses, [])

    def test_timeout_delivered_is_success_without_second_push(self):
        g, pauses, push = self.check([(HEAD, 'local'), (PUSH, timeout()), (REMOTE, 'local refs/heads/observations')])
        push('/obs', 'observations')
        self.assertFalse(g.steps)
        self.assertEqual(sum(args == PUSH for _, args in g.calls), 1)
        self.assertEqual(pauses, [])

    def test_timeout_then_ancestor_retries_same_head(self):
        g, pauses, push = self.check([(HEAD, 'local'), (PUSH, timeout()), (REMOTE, 'old refs/heads/observations'), (FETCH, ''), (ANCESTOR, ''), (PUSH, '')])
        push('/obs', 'observations')
        self.assertFalse(g.steps)
        self.assertEqual(pauses, [3])
        self.assertFalse(any('--force' in args for _, args in g.calls))

    def test_existing_runtime_error_path_still_reconciles(self):
        g, _, push = self.check([(HEAD, 'local'), (PUSH, RuntimeError('server failure')), (REMOTE, 'local refs/heads/observations')])
        push('/obs', 'observations')
        self.assertFalse(g.steps)

    def test_remote_advanced_refuses_overwrite(self):
        g, pauses, push = self.check([(HEAD, 'local'), (PUSH, timeout()), (REMOTE, 'old refs/heads/observations'), (FETCH, ''), (ANCESTOR, RuntimeError('not ancestor'))])
        with self.assertRaisesRegex(RuntimeError, 'refuse overwrite'):
            push('/obs', 'observations')
        self.assertFalse(g.steps)
        self.assertEqual(pauses, [])

    def test_unavailable_remote_check_aborts_without_retry(self):
        g, pauses, push = self.check([(HEAD, 'local'), (PUSH, timeout()), (REMOTE, timeout())])
        with self.assertRaises(subprocess.TimeoutExpired):
            push('/obs', 'observations')
        self.assertFalse(g.steps)
        self.assertEqual(pauses, [])

    def test_retries_bounded_to_original_three_attempts(self):
        steps = [(HEAD, 'local')]
        for _ in range(3):
            steps += [(PUSH, timeout()), (REMOTE, 'old refs/heads/observations'), (FETCH, ''), (ANCESTOR, '')]
        g, pauses, push = self.check(steps)
        with self.assertRaises(subprocess.TimeoutExpired):
            push('/obs', 'observations')
        self.assertFalse(g.steps)
        self.assertEqual(pauses, [3, 9])
        self.assertEqual(sum(args == PUSH for _, args in g.calls), 3)

    def test_absent_remote_retries_same_head_within_bound(self):
        g, pauses, push = self.check([(HEAD, 'local'), (PUSH, timeout()), (REMOTE, ''), (PUSH, '')])
        push('/obs', 'observations')
        self.assertFalse(g.steps)
        self.assertEqual(pauses, [3])

    def test_installs_actual_cloud_import_binding_without_io(self):
        def unused(*args):
            self.fail('install must not run Git')
        p = types.SimpleNamespace(git=unused, push=object())
        c = types.SimpleNamespace(push=p.push)
        wrapped = install(p, c)
        self.assertIs(c.push, wrapped)
        self.assertIs(p.push, wrapped)


if __name__ == '__main__':
    unittest.main(verbosity=2)
