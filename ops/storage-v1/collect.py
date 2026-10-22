"""Run the unchanged frozen collector through explicitly pinned storage hooks."""
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import adapter
import cloud
import persistence
import io_adapter as io


def main():
    identity = io.check_identity(ROOT)
    # Only raw file layout, sparse publication and operations workflow routing change.
    adapter.A.save_raw = io.writer(adapter.A)
    cloud.checkout = lambda root, dest, branch, create=False: io.sparse_checkout(persistence, root, dest, branch, create)
    cloud.commit = lambda root, message: io.sparse_commit(persistence, root, message)
    cloud.run = io.routed_run(persistence.run)
    original_persist = cloud.persist

    def persist(obs, connection, data, freeze, sequence, parent):
        persistence.write(obs / 'OPERATIONS_STATUS.json', {
            'schema': 'storage-operations-v1',
            'identity_sha256': identity,
            'scientific_source': io.BASE_SOURCE,
            'freeze_sha256': io.FREEZE_SHA256,
            'body_prefix': io.PREFIX,
            'sparse_observations_checkout': True,
            'scientific_formulas_changed': False,
        })
        return original_persist(obs, connection, data, freeze, sequence, parent)

    cloud.persist = persist
    cloud.collect()


if __name__ == '__main__':
    main()
