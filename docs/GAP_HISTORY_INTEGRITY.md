# Gap and intrusion history integrity

## Confirmed replay issue, 2026-10-23

The public Decentrader map calls `getOHLCHourlyCalculations(["btcusdt"])`.
Its 8,760 timestamped rows include cohort birth levels but only the latest
`*Active` flags. Direct inspection found an August 19 row with a later
`highRef` of 87,395.67. The original viewer filtered old events using these
current flags. A cohort removed after August disappeared from the August
replay too. This is look-ahead/survivorship bias, not evidence that the live
edge tracker was stuck. This logic existed in the June viewer.

Commit 048d355 copied this display-only reconstruction into older Spot-chart
boxes. Agreement across 498 recent overlapping hours did not validate older
history. Those current-survivor extensions remain quarantined on server and client.
No original raw snapshots, logs or saved alert evidence are deleted.

## ETH Source and Edge Audit, 2026-10-24

The deployed ETH Spot candle route reports ETHUSDT / Binance Spot / 1h.
The ETH replica configuration uses only Spot klines, $5 bins and its own
`binance-spot-eth-liquidation-cohorts-v2.1` model. The chart rejects cross-market
inputs and never imports BTC recovery or Decentrader BTC active flags.

Read-only live checks used the existing ETH timelapse response and direct Binance
`api/v3/klines?symbol=ETHUSDT&interval=1h` reads. The final audit covered candle
opens 2026-10-03 15:00 UTC through 2026-10-24 10:00 UTC:

- 500/500 OHLC/OHLC4 frames match Binance Spot.
- Independent application of the complete cohort seed and per-hour deltas gives
  the same nearest occupied left AND right prices for every one of 500 gap pairs.
- The left edge has 8 distinct values / 7 transitions; the right has 10 / 9.
- A fresh causal model replay from 10,340 Spot candles, with 8,760 prior warmup
  hours, reproduces all 500 live gap pairs without a mismatch. This uses the
  existing model implementation; the seed/delta edge check is separate arithmetic.
- All 54 saved intrusion timestamps and directions agree with the causal replay.
  The 9 records with original saved gap pairs match those pairs exactly. The
  other 45 records (August 19 through September 21) have no original gap pair;
  their reconstructed edges are NOT proof of what was saved/observed then and
  are never written back as original observations.

No production correction or trading change was required by these checks. This is
validation of the reconstructed model/source, not proof of real liquidation
inventory, historical email delivery time, all persistent archive contents, or
permanent absence of future provider/application faults.

Recheck without starting the production application:

```powershell
node scripts/audit-eth-gap-source.cjs --all-labels
```

The CLI performs only bounded public GET requests and local calculations. It
starts no collector, submits no order, sends no mail, and changes no state files.
Without flags it checks current payload geometry and 500 Spot candles; `--deep`
adds a full warmup replay, while `--all-labels` also covers older saved labels.
Missing original evidence is reported separately from a mismatch. Regression
tests explicitly detect a frozen right edge, source mixing, incorrect previous
gaps, and later candles rewriting earlier ETH/BTC causal snapshots.

## Historical display protection

- Decentrader historical-frame gaps use the independent causal Binance recovery
  for the default active/all-sides/all-leverages view. Unsupported filters or
  uncovered hours remain unknown; current-survivor gaps are never restored.
- Historical intrusion UI shows saved records when available; otherwise unknown,
  not a falsely reconstructed event and not proof of no intrusion.
- The latest Decentrader frame, Public V2 causal calculations, mail gates,
  manual entry plans and all exchange SL/TP/trailing behavior remain unchanged.
- Public V2's first retained gap has unknown birth unless consecutive source
  records prove its transition. No hard-coded earlier anchor is applied.
- The compact Decentrader history endpoint returns 409
  `HISTORICAL_STATE_UNVERIFIED`; an older 200 response lacking verified historical
  provenance is also rejected by the chart.

## Display Recovery, 2026-10-23

The initial quarantine hid both historical edges. The display now restores both
from a causal replay of 17,536 closed Binance Spot BTCUSDT 1H candles, using the
existing v2.4 model and a full 8,760-hour warmup before the first displayed hour.
The immutable generated file `public/data/btc-gap-recovery-v1.json` contains
8,776 hourly gap pairs from October 23, 2025 through October 23, 2026 15:00 UTC.
It retains generation time, source range, endpoint and candle SHA-256. Reproduce
it offline with `scripts/recover-btc-gap-history.cjs`; do not run the production
application to generate it. The script rejects missing source hours.

`GET /open-liquidity/v2/gap-projection-history?market=BTC-USD` reads this recovery
plus the BTC v2.4 append-only archive. It performs no external requests or disk
writes, coalesces concurrent reads and caches for one minute. Only valid matching
BTC model rows with verified fingerprints can extend the archive. First-seen
archived values win over the offline reconstruction; revisions never silently
replace first observations. Missing hours are not filled. Integrity errors return
503; the chart retains its last accepted history and shows the failed read.

The recovered history is explicitly `causal-model-replay`, NOT original
Decentrader snapshots and NOT historical email timing evidence. Current rolling
Public V2 frames always win overlap in the chart. Different models are rejected.
No trading, mail, manual-entry, SL, TP or trailing code consumes this recovery.
The Decentrader histogram itself still has current active flags; only its gap
overlay and edge values are restored, with provenance shown. Saved intrusions
remain visible; absent records stay unknown, not reconstructed from wrong bars.

Independent checks against 658 contemporaneous logged observations found 658
matching left edges and 646 matching right edges at the logged provider frame.
The 12 right-edge differences cluster around transitions in delayed provider
frames. This is incomplete validation, not proof that every hour matches the
provider. The causal replay gives 61,000 / 67,000 for August 19 14:00 UTC
(available after its 15:00 UTC close), not October's 87,400. Later August 19 hours
have different right edges; 67,000 is not asserted for the whole day.

## Persistent Public V2 Gap Observation Archive

All existing replica collectors write a compact independent archive on their
existing deferred persistence path, after close processing. No new source
requests, timers, trade gates or env variables are introduced.

Location beneath each pair's existing configured persistent history directory:

```text
gap-observations/<modelVersion>-YYYY-MM.jsonl
```

The existing rolling 500-frame replay file remains unchanged. Archive files are
append-only and are not pruned when the replay rolls or rebuilds. They retain
market/symbol/model version, candle-open and candle-close, source observation
and recording time, OHLC, source-window size and gap edges/counts (or null).
Repeated facts are deduplicated across restarts. Changed values append an
explicit revision linked to the prior fingerprint; the original line remains.
Later changes must never silently rewrite earlier observations.

Every Public V2 archive row is labelled `causal-model-snapshot`, not independently
observed historical Decentrader truth. In particular, a bootstrap computed in
October for an August candle retains its October observation time. This is NOT
proof that August data arrived at its candle close. Matching BTC rows extend
the display-only recovery endpoint; they are never used as an entry source.

Successful Decentrader source fetches also archive ONLY the actual latest
provider gap under the existing monitor-state directory, as
`gap-observations/decentrader-btc-observed-v1-YYYY-MM.jsonl`. These rows are
labelled `provider-observed-snapshot` and retain their actual observation time;
they do not claim on-time availability at the provider frame's candle close.
No OHLC candles are invented when the provider only supplies OHLC4. No older
current-active replay frames are imported as original observations.

The intrusion DOM study keeps its first stored alert gap. Differing later
recomputations are retained separately in `gapRevisions` instead of silently
overwriting it. Repeated identical revisions are deduplicated. This preserves
existing evidence; it cannot certify that an old first record was correct or
recover any original values that were overwritten before this fix.

`status.gapArchive` reports process-local rows/revisions written, last successful
write and last error. A corrupt file or fingerprint mismatch fails visibly and
is not overwritten. Archive failures are logged and do not cancel entries or
prevent existing rolling replay persistence. Missing historical hours remain
missing. A power-loss-truncated file requires explicit recovery, not silent
replacement. Persistent disk health and provider availability remain operational
risks; this change cannot guarantee 100% data availability or source correctness.

## Ongoing Preservation For All Sources, 2026-10-23

The Decentrader observed BTC feed and all seven Public V2 collectors keep their
independent gap journals. BTC, ETH, INJ, SOL and ZEC use their configured Binance
Spot feed; GOLD/SILVER retain their existing Futures sources. No source switches,
trade-enable flags, mail gates, order sizing, SL, TP or trailing rules change.
The existing `/app/data` paths verified in live status remain in use; explicit
existing history/state paths are honored. No new env variables are required.

Gap writes now flush the file with `fsync`, check fingerprint/revision chains
when loading changed files, retain failed original batches and retry after one
minute even without another intrusion. First-seen facts are never replaced;
corrections append revisions. A malformed/truncated file blocks that archive
with a visible error rather than overwriting evidence. Trading does not await
gap archive writes. A same-size, same-timestamp external change may not invalidate
a writer's index cache, but full integrity audits and restart reads still verify
the contents. Checksums are corruption detection, not external attestation.

Additional monthly append-only journals, beneath each configured pair directory:

```text
liquidity-observations/source-candles/<modelVersion>-YYYY-MM.jsonl
liquidity-observations/intrusions/<modelVersion>-YYYY-MM.jsonl
```

Public V2 archives the already-fetched closed OHLC candles, including the source
warmup window, source venue, symbol, model version, price-step and cohort-window
parameters. Repeated candles deduplicate; later changed OHLC appends a revision.
Original source rows can reproduce model gap edges after a restart, independently
of the rolling 500-frame chart file. This uses no additional provider requests.
The existing ten-second deferred persistence path remains after close evaluation.

The intrusion journal retains compact original event/gap/entrant and SMTP timing
evidence, not complete orderbook, CoinGlass or DOM research datasets. Public V2
copies records before its 10,000-row working-history limit trims them, and imports
the still-available working history once at first successful check. Decentrader
copies compact study evidence before pruning and captures available delay records
when delay history is updated. Imports are labelled `stored-record-import`, not
claimed as recovered original live observations. Existing records already lost
before this change cannot be recovered by this mechanism. Public V2 benchmark
updates now also retain the first gap/observation time and record differing later
gaps separately, matching the existing Decentrader protection.

New journals use `fsync`, revision chains, copied inputs, serialized per-source
writes and retryable failed batches. The process-wide pending-evidence payload
budget is 64 MiB: exhaustion rejects new captures explicitly, increments
`rejectedRecords` and logs an error; it never silently deletes queued originals.
Gap journals additionally cap a failed-write backlog at 10,000 records per source.
This is a memory safeguard during prolonged disk failure, not a promise of lossless
capture when storage is unavailable. Disk failures, rejected captures and audit
errors require operational attention.

`status.observationArchive` exposes only storage-health metadata, including pending
records, errors, write counters, rejection counters and the last audit. Successful
journal writes trigger a local audit at most hourly: current month plus a rotating
older month, fingerprint chains, missing hours inside the actual recorded span and
available disk space. Less than 128 MiB free logs a warning. An empty intrusion
month can be legitimate; a missing observation is never inferred to mean no signal.
Audits do not claim full-month coverage or timely source availability, and do not
repair missing historical hours. Write counters are process-local, not disk totals.

There is intentionally no new public HTTP archive export. On the user's machine
or in an authorized Render shell, inspect a specific month read-only, for example:

```text
node scripts/audit-liquidity-storage.cjs /app/data/open-liquidity-v2 BTC-USD BTCUSDT binance-spot binance-spot-liquidation-cohorts-v2.4 2026-10
node scripts/audit-liquidity-storage.cjs /app/data BTC-USD BTCUSDT decentrader decentrader-btc-observed-v1 2026-10
```

The script prints counts, coverage, integrity and free-space metadata, not raw
intrusion/mail records. It never imports the application/controller entry point.

### Limits And Backup

Append-only storage is not an independent backup. It protects against rolling
eviction and silent logical replacement, not deletion/failure of the entire Render
disk. No external storage/account integration was configured by this change.
Independent off-disk backups are still needed for that failure class. Writes
queued in memory or within the deferred write window can be lost on abrupt process
termination, particularly if disk writes were already failing. Binance public
prices may allow a later causal reconstruction; missing original Decentrader
observations or actual mail timing cannot be fabricated. Archiving records what
was received, not a guarantee that a provider's data was itself correct.

## Regression Checks For Preservation

- All seven configured pairs have separate identities and directories.
- Original candles reproduce saved model gap edges after restart.
- Original gaps, timestamps and entrants survive later corrections and state trim.
- Repeated reads deduplicate; failed evidence survives and retries automatically.
- Corrupt files are preserved, revision chains fail visibly, and unsafe month paths
  are rejected. Audits report actual missing hours rather than forward-filling.
- The pending-memory limit rejects explicitly, not through an unbounded backlog.

## Existing Regression Checks

- Future extreme candles cannot change past Public V2 gap edges or intrusions.
- Invalid current-active archives cannot enter the Spot overlay via 200 or 409.
- Decentrader historical UI restores causal gap edges only for supported filters,
  retains saved records and distinguishes unknown hours from no intrusions.
- Archive tests cover restarts, rolling eviction, duplicate reads, revisions,
  concurrent writes, copying queued values, corrupt files and disk failures.
- Browser checks cover desktop/mobile and block all non-read requests.

Old log recovery evidence is preserved separately in the user's workspace:
`BTC-gap-herstel-2026-10-23.md` and
`btc-gap-recovered-evidence-2026-10-23.json`. It is incomplete, provenance-labelled
evidence, not a complete hourly gap reconstruction. The user-reported 67,000 on
August 19 was not established by those logs; the independent causal calculation
now reproduces it at the specific hour documented above.
