"""Additive operations identity; the original scientific and storage bytes stay pinned."""
import hashlib
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'ops/storage-v1'))
import io_adapter

PREDECESSOR = 'c4a082dd019f2b33b2030c1615333d7f874a23ac'
STORAGE_IDENTITY = 'd8f2e00b3079b14f1ad0290747e0df62a73c23eaa3de7f87ed4768101fe7021a'


def check(root=ROOT):
    root = pathlib.Path(root)
    if io_adapter.check_identity(root) != STORAGE_IDENTITY:
        raise RuntimeError('Original storage identity changed')
    path = root / 'ops/publication-v1/IDENTITY.json'
    raw = path.read_bytes()
    identity = json.loads(raw)
    assert identity['predecessor_operations_source'] == PREDECESSOR
    assert identity['storage_identity_sha256'] == STORAGE_IDENTITY
    assert identity['scientific_source'] == io_adapter.BASE_SOURCE
    assert identity['freeze_sha256'] == io_adapter.FREEZE_SHA256
    for name, digest in identity['hashes'].items():
        if io_adapter.sha(root / name) != digest:
            raise RuntimeError('Publication identity mismatch: ' + name)
    return hashlib.sha256(raw).hexdigest()
