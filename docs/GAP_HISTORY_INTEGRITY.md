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
history. Those extensions are now quarantined on both the server and client.
No original raw snapshots, logs or saved alert evidence are deleted.

## Historical display protection

- Decentrader historical-frame gap edges are withheld, not labelled as 87,400.
- Historical intrusion UI shows saved records when available; otherwise unknown,
  not a falsely reconstructed event and not proof of no intrusion.
- The latest Decentrader frame, Public V2 causal calculations, mail gates,
  manual entry plans and all exchange SL/TP/trailing behavior remain unchanged.
- Public V2's first retained gap has unknown birth unless consecutive source
  records prove its transition. No hard-coded earlier anchor is applied.
- The compact Decentrader history endpoint returns 409
  `HISTORICAL_STATE_UNVERIFIED`; an older 200 response lacking verified historical
  provenance is also rejected by the chart.

## Persistent Public V2 gap observation archive

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
proof that August data arrived at its candle close. The new archive is not
automatically imported into the chart or used as an entry source.

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

## Regression checks

- Future extreme candles cannot change past Public V2 gap edges or intrusions.
- Invalid current-active archives cannot enter the Spot overlay via 200 or 409.
- Decentrader historical UI hides computed edges, retains saved records and
  distinguishes unknown hours from no intrusions.
- Archive tests cover restarts, rolling eviction, duplicate reads, revisions,
  concurrent writes, copying queued values, corrupt files and disk failures.
- Browser checks cover desktop/mobile and block all non-read requests.

Old log recovery evidence is preserved separately in the user's workspace:
`BTC-gap-herstel-2026-10-23.md` and
`btc-gap-recovered-evidence-2026-10-23.json`. It is incomplete, provenance-labelled
evidence, not a complete hourly gap reconstruction. The user-reported 67,000 on
August 19 has not been independently verified in the recovered sources.
