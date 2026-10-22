# Storage operations v1 — 22 October 2026

Question: can the same frozen collector resume durable publication after GitHub rejects the flat raw-body Git tree, without changing scientific bytes, state or rules?

GitHub rejected a raw-body tree above its non-blob object-size limit. The original collector and observer workflows are contained, not edited. This separately versioned wrapper calls the frozen collector after checking all 26 original file hashes and this operations identity. Alert, admission, sizing, costs, clocks, entry deadlines, exits, resource limits and study dates remain unchanged. There is no reset, retrospective fill, new study or interim economic review.

Only new response bodies use `raw/sharded-v1/bodies/<first-two-SHA-hex>/<SHA>.json.gz`. Every actual receipt/version remains saved; legacy files and references remain unchanged. Sparse Git worktrees materialize seed, journals and status, while sparse staging retains archived raw trees and appends new raw files. Publication still uses the original ancestor check and bounded same-commit retries. Operations metadata identifies the wrapper in each checkpoint. Workflow routing sends healthy successors and halt-disable calls to the operations collector, under the same single-writer concurrency group. Original 3 GiB runtime and 3 GiB free-disk guards remain active.

The observer's separately versioned read adapter handles GitHub's omitted base64 content for files over 1 MiB using the raw media response, checking the Git blob hash before the unchanged observer checks body/journal SHA-256. Observer scheduling remains best effort and incident deduplication remains unchanged.

Run only `python3 -B ops/storage-v1/tests.py` for focused operations tests. Do not rerun the frozen tests that write TEST_RESULTS.json. Neither storage repair nor hosted success repairs missed live books, qualifies the strategy or extends the fixed window.
