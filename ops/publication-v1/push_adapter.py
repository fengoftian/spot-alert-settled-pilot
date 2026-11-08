"""Candidate operations adapter: reconcile ambiguous Git push timeouts.

No collector imports this file. Installing it requires hosted qualification and
an explicitly approved successor operations identity. Frozen bytes stay intact.
"""
import subprocess
import time


def make_push(git, sleep=time.sleep):
    def push(root, branch, attempts=3):
        if not 1 <= attempts <= 3:
            raise ValueError('One to three attempts required')
        local = git(root, 'rev-parse', 'HEAD')
        last = None
        for attempt in range(attempts):
            try:
                git(root, 'push', 'origin', 'HEAD:' + branch)
                return
            except (RuntimeError, subprocess.TimeoutExpired) as ex:
                # A timeout is ambiguous: the server may have accepted this HEAD.
                # Reconcile before retrying, preserving the original ancestor gate.
                last = ex
                remote = git(root, 'ls-remote', 'origin', 'refs/heads/' + branch).split()
                if remote and remote[0] == local:
                    return
                if remote:
                    git(root, 'fetch', '--depth=1', 'origin', branch)
                    try:
                        git(root, 'merge-base', '--is-ancestor', remote[0], local)
                    except RuntimeError:
                        raise RuntimeError('Remote advanced unexpectedly; refuse overwrite') from ex
                if attempt + 1 < attempts:
                    sleep((3, 9)[attempt])
        raise last
    return push


def install(persistence, cloud, sleep=time.sleep):
    """Wire both bindings: cloud imported push by value in the frozen source."""
    wrapped = make_push(persistence.git, sleep)
    persistence.push = wrapped
    cloud.push = wrapped
    return wrapped
