"""Unchanged observer with hash-verified reads for large journal blobs."""
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import health
import io_adapter as io


def main():
    io.check_identity(ROOT)
    health.file = lambda path, ref: io.read_blob(health.api, path, ref)
    original_api = health.api

    def api(path, method='GET', body=None):
        if path == 'repos/fengoftian/spot-alert-settled-pilot/actions/workflows/health.yml/disable':
            path = path.replace('/health.yml/', '/' + io.OBSERVER_WORKFLOW + '/')
        return original_api(path, method, body)

    health.api = api
    return health.main()


if __name__ == '__main__':
    raise SystemExit(main())
