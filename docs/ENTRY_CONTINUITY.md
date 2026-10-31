# Entry Continuity During Short Interruptions

## Failure Window Addressed

Automatic process recovery alone does not guarantee an entry is observed. Before
this change, every ETH/SOL restart reset `gapDailyEntry.activatedAt`. A candle
closing seconds before a crash/restart was therefore excluded even if the service
recovered within the existing five-minute entry window. SOL also waited 255 seconds
and ETH 75 seconds before starting execution monitoring.

## Purple Entry Recovery

Active ETH/SOL purple execution now starts immediately after exchange
initialization, retaining the existing serialized replica bootstraps that control
memory. Protective position reconciliation starts immediately too. There is no
extra parallel cohort rebuild and no separate order executor.

Each model writes a tiny, separate `<trade-state-file>.entry-lease.json` heartbeat
every minute. It records market/model identity, active status, original activation
and last observation. Writes use a temporary file, fsync and atomic rename.
It does not duplicate chart history, credentials or order contents.

After restart, original activation is preserved only when the prior heartbeat
was active, belongs to this exact market/model and is at most five minutes old.
Future/malformed/missing/disabled/expired records never grant retrospective
eligibility. An explicit disabled configuration invalidates continuity. Failed
heartbeat persistence does not grant restart-resume permission.

Signal requirements are unchanged: the same exact matching Spot intrusion/D
close, only the latest closed hour, at most five minutes old, with current valid
source data. A newer signal is not invented and an old signal is not queued for
unlimited delayed entry. Source reconstruction still has to finish in time.

Existing `handledCandle`, persisted pre-submit intent, uncertain-order recovery,
current-position checks and market coordinator still gate execution. A restart
does not clear those protections or permit a duplicate submission. Malformed or
unreadable trade-state files now block rather than silently becoming an empty
"no prior order" state. Files are preserved and status reports `STATE_UNREADABLE`.
Normal missing state for a genuinely new installation still starts empty.

New status: `execution.gapDailyEntry.continuity` contains readiness, heartbeat,
original activation, whether this startup resumed, and its reason. This is proof
of entry-monitor continuity only, not a guaranteed future exchange fill.

## Limits And The Next Availability Layer

- No single Render instance can guarantee uninterrupted entry detection. Long
  platform/network/provider outages, disk failure, exchange rejection and a crash
  before any durable observation can still mean a missed opportunity.
- This change does not widen BTC manual entry's existing freshness policy, move
  its plans, create test orders or modify SL, TP, trailing or risk sizing.
- Existing exchange-hosted stops remain separate from server computation.
  Trailing updates cannot be computed while the executor is down.
- Durable externally hosted signal storage plus an independent execution worker
  and carefully fenced failover are needed to cover an entire-instance/platform
  outage. Starting two trading instances without shared idempotency/locking is
  not a safe substitute and is not implemented here. No paid infrastructure is
  created or changed automatically.

Tests simulate ETH and SOL restarting across candle close, recovering while the
signal is fresh, and placing once. They cover disabled, wrong-market/model,
malformed, future, missing and expired heartbeat rejection, fsync failure, tiny
bounded writes, immediate startup and unreadable trade-state protection. All
exchange callbacks are mocks; no live orders are used.
