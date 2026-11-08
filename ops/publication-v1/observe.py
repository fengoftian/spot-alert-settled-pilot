"""Original deduplicated observer, with new operations provenance and routing."""
import base64
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'ops/storage-v1'))
import health
import io_adapter as io
import publication_identity


def configure():
    identity = publication_identity.check(ROOT)
    original_api = health.api

    def api(path, method='GET', body=None):
        if path == 'repos/' + health.REPO + '/actions/workflows/health.yml/disable':
            path = path.replace('/health.yml/', '/health-ops-v2.yml/')
        if path == 'repos/' + health.REPO + '/contents/health.json' and method == 'PUT':
            body = dict(body)
            result = json.loads(base64.b64decode(body['content']))
            result.update(operations_identity_sha256=identity,
                          scientific_source=io.BASE_SOURCE,
                          ambiguous_push_timeout_reconciliation=True)
            body['content'] = base64.b64encode((json.dumps(result, indent=2) + '\n').encode()).decode()
        return original_api(path, method, body)

    health.api = api
    health.file = lambda path, ref: io.read_blob(health.api, path, ref)
    return identity


def main():
    configure()
    return health.main()


if __name__ == '__main__':
    raise SystemExit(main())
