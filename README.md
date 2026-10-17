# Settled-data Spot alert comparison

Public-data paper screening only. This new version preserves the original alert/account formulas while correcting candle admission and durable collection. It does not resume or overwrite the interrupted A/B studies. No account access, API keys or actual orders.

DESIGN.md defines the question, engineering gates, frozen constants, remaining limits and final reporting. Workflow launch first runs unit tests and two hosted data-only qualification phases, then fresh stable warmup, a future freeze and continuous single-writer collection. The `observations` branch retains original seed parts, journals, raw receipts and collection status. Outcomes are produced only at the fixed end/tail. `qualification` retains live technical receipts; `health-state` is a separate deduplicating operational observer.

Public standard ubuntu-24.04 runners; no paid runner, artifact upload, cache or Git LFS. GitHub scheduling is best effort. No short-window CAGR, trading validation or promised profitability.
