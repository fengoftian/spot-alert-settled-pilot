# Settled study operational recovery

Owner authorized operational repair on 12 November 2026 NZDT. The latest stopped
producer exhausted three 180-second Git push attempts. This adapter keeps those
transport bounds and all preserved scientific, storage and publication bytes.
It does not claim to repair GitHub availability or reconstruct missing books.

The successor collector emits a source/run-bound recovery marker only for a
recognized publication transport failure with no current integrity/disk halt.
An independent watchdog runs after collector completion, with a 15-minute cron
fallback. It validates source, frozen seed, latest journal, gates and writer state
before claiming one recovery per producer using a compare-and-swap ledger. It
allows at most three recovery claims per hour; ambiguous dispatch/write failures,
unknown failures, competing collectors and integrity/disk halts remain closed.
All collector versions share the original single-writer concurrency group.

The original capture, admission, decision, sizing, cost, account, exit, resource
guards, seed, cohort and fixed study window remain unchanged. The watchdog reads
no interim economics. After the fixed tail it can recover the same final report
calculation, and disables collection/health only after checking persisted JSON,
Markdown and diagnostics for the original window and all 81 account identities.
The human-facing screening report still requires the scheduled independent review
of coverage, whole-account return/drawdown, costs, deployment and limitations.

Only standard public ubuntu-24.04 runners are used. No paid artifacts/cache,
external service, exchange account or orders are introduced. GitHub event and
runner latency remain best effort; historical collection gaps are permanent.

Pre-activation correction, 13 November 2026 NZDT: when the newest failed run has
not published a checkpoint, read that run's job log and validate its marker; do
not reuse the older checkpoint producer's log. A mocked complete observer path
verifies the exact log identity and one bounded dispatch without network access.
