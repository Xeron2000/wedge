# TODOS

## Trading Runtime

### Accepted-artifact reuse across entry and exit

**What:** Reuse accepted NOAA forecast artifacts across entry and exit paths instead of re-fetching the same cycle/target repeatedly.

**Why:** Reduces duplicate NOAA requests, lowers hot-path latency noise, and makes the freshness contract real instead of purely conceptual.

**Context:** During `/plan-eng-review`, Phase 1+2 was intentionally reduced to shadow readiness probes, timestamps/metrics, bounded parallel fetch, feature flag rollout, and current static ordering. Artifact reuse was explicitly deferred because it introduces a new state model and invalidation rules that would have mixed structural and behavioral changes into the same PR. Pick this up after Phase 1+2 latency traces are stable and cycle marker / terminal reason paths are proven.

**Effort:** M
**Priority:** P1
**Depends on:** Phase 1+2 rollout completion and stable readiness / latency telemetry

### Target reordering experiment after latency baseline is stable

**What:** Run a separate experiment that reorders supported targets by liquidity/actionability instead of current static city/date order.

**Why:** Could improve useful reaction time, but only if measured separately from the base latency-path improvements.

**Context:** Review decisions explicitly kept current static ordering for Phases 1-3 so latency gains could be measured cleanly without strategy-policy confounds. This item should only be picked up once the new readiness-driven path has a stable baseline and the team can compare coverage-rate tradeoffs over a meaningful evaluation window.

**Effort:** M
**Priority:** P2
**Depends on:** Phase 1+2 rollout completion, stable latency baseline, and test coverage for coverage-rate regression checks

### Evaluate intra-cycle cancel/replace repricing

**What:** Decide whether the system should support cancel/replace or repricing within the same NOAA cycle when a better quote appears.

**Why:** Current v1 intentionally chooses one live order per key with no intra-cycle repricing; that keeps the system simple, but may cap competitiveness if the market moves quickly after readiness.

**Context:** The plan review deliberately chose a KISS order model for v1: one live order per idempotent key, no repricing, and no intra-cycle cancel/replace. This was a conscious defer, not an omission. Revisit only after real runtime data shows that quote movement inside a single cycle is materially hurting fills or PnL, because this change adds significant lifecycle, idempotency, and testing complexity.

**Effort:** L
**Priority:** P3
**Depends on:** Phase 1+2 rollout completion plus real fill/quote behavior data

## Completed
