"""Same frozen study and storage hooks, plus safe ambiguous-push reconciliation."""
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'ops/storage-v1'))
import adapter
import cloud
import persistence
import io_adapter as io
import publication_identity
import push_adapter


def configure():
    identity = publication_identity.check(ROOT)
    io.COLLECTOR_WORKFLOW = 'collector-ops-v2.yml'
    adapter.A.save_raw = io.writer(adapter.A)
    cloud.checkout = lambda root, dest, branch, create=False: io.sparse_checkout(persistence, root, dest, branch, create)
    cloud.commit = lambda root, message: io.sparse_commit(persistence, root, message)
    cloud.run = io.routed_run(persistence.run)
    push_adapter.install(persistence, cloud)
    original_persist = cloud.persist

    def persist(obs, connection, data, freeze, sequence, parent):
        persistence.write(obs / 'OPERATIONS_STATUS.json', {
            'schema': 'publication-operations-v1',
            'identity_sha256': identity,
            'storage_identity_sha256': publication_identity.STORAGE_IDENTITY,
            'scientific_source': io.BASE_SOURCE,
            'freeze_sha256': io.FREEZE_SHA256,
            'body_prefix': io.PREFIX,
            'sparse_observations_checkout': True,
            'scientific_formulas_changed': False,
            'ambiguous_push_timeout_reconciliation': True,
            'maximum_push_attempts': 3,
            'git_command_timeout_seconds': 180,
        })
        return original_persist(obs, connection, data, freeze, sequence, parent)

    cloud.persist = persist
    return identity


def main():
    configure()
    cloud.collect()


if __name__ == '__main__':
    main()
