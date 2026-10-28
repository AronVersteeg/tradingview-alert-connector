# ETH Spot Intrusion + Daily Experiment

This is a separate ETH-only entry model. BTC manual entries, TPs and protective
management are unchanged. Binance Spot ETHUSDT supplies the signal; orders are
placed on dYdX ETH-USD, not on Binance Spot.

## Activation

The new switch defaults to **false**. After deploying, enable both:

```dotenv
OPEN_LIQUIDITY_V2_ETH_GAP_DAILY_ENTRY_ENABLED=true
OPEN_LIQUIDITY_V2_ETH_AUTO_TRADE_ENABLED=true
```

The existing ETH monitor must also be enabled (its default is true):
`OPEN_LIQUIDITY_V2_ETH_INTRUSION_MONITOR_ENABLED=true`.
Keep the existing initial SL/trailing settings enabled:
`DECENTRADER_DYNAMIC_SL_ENABLED`, `DECENTRADER_DYNAMIC_SL_LIVE_UPDATES_ENABLED`
and `DYDX_V4_FRACTAL_TRAIL_ENABLED`. Missing protective execution support blocks
new entries. Existing risk budgets, minimum size, dynamic downsizing, market
status and stop-order capacity checks remain enforced.

While the experiment is enabled, ETH Delay, old Shadow and manual entry execution
are suppressed; existing manual plans remain stored. Their mail settings are not
changed. The new model does not depend on Delay/OI filters or the Shadow switch.
Other pairs' switches are unchanged. Turning off the experiment stops its entries,
not the protection of an existing position. Turning it off restores the other
ETH entry models according to their own switches; set the ETH pair switch false
as well when all new ETH entries should stop.

## Signal

- A fresh Public V2 Spot cohort intrusion and a Daily fractal break must occur on
  the **same newly closed 1H candle**, in the same direction.
- Long: previous close at/below an intact Daily HIGH, new close strictly above.
  Short: previous close at/above an intact Daily LOW, new close strictly below.
- The Daily fractal must have been confirmed before the signal candle opened.
  An earlier wick break makes that level ineligible. Equal touches do not count.
- Uses the exact pure projection and matching functions behind the purple ETH
  chart marker, including older still-intact Daily levels. Weekly/Monthly data
  availability is not an entry dependency. An intrusion is not a green-box exit.
- **Futures delta is observation-only.** The positive-FD entry requirement was
  removed on 2026-10-28 at the user's request. Negative, zero, missing or failed
  FD measurements do not block an otherwise valid purple entry. The executor
  does not request/wait for FD. All purple lines and their chart labels remain
  available, independent of FD sign. No environment change is needed.
- Only the latest closed hour, within five minutes of its close, is eligible.
  The first successful scan after activation/restart arms the model; earlier
  closed candles and historical purple markers cannot open a trade retrospectively.
- Closed-hour checks start around five seconds after the hour. Provider latency
  and processing can delay them; failed source checks retry within the existing
  five-minute window and otherwise skip the entry.
- Source identity, causal ETH model, matching replica/Spot OHLC and Daily
  coverage are checked. Missing/stale/mismatched data never invent a signal.

## Position Management

Initial SL and subsequent trailing use the existing **dYdX 1H** fractal logic,
buffers and stop-breach protection, exactly as the existing managed setup.
The entry explicitly uses the existing `NO_TP` execution profile and an empty TP
ladder. This also disables the exchange client's optional safety/fallback TPs.
`takeProfitMode: trailing-only` is persisted with the position, preventing later
dynamic TP synchronization or restart recovery from recreating TP orders,
even when `DECENTRADER_DYNAMIC_TP_ENABLED=true` for BTC.

No adding to, reversing or replacing an existing ETH position. Order intent is
written before submission; one signal candle is attempted at most once even if
its cohorts change. Uncertain submission is reconciled against the actual fill,
with stop repair, rather than resubmitted. Reconciliation retries every five
seconds for up to five minutes, independently of map reads and entry switches.
Unattributable positions require
operator inspection and are not silently adopted. No risk or exchange rejection
can be guaranteed away, and no unprotected entry is deliberately requested.

## Inspection And Tests

The existing `/open-liquidity/v2/status?market=ETH-USD` monitor status contains
`gapDailyEntry`: activation, latest decision, signal candle and execution outcome.
`futuresDeltaFilter.enabled` is false. Previously stored FD evidence is preserved
in old state/order intents; new entries do not require or attach an FD decision.
Existing positions and uncertain pre-deploy submissions continue protective
reconciliation without an FD check. The chart cache and its stored measurements
are unchanged.
The existing ETH state file also holds the durable intent and position policy.
No new public raw-history export endpoint is introduced.

Tests: `test/ethGapDailyEntry.test.ts`, `test/dydxV4EntryRiskGuard.test.ts`,
plus the existing ETH/BTC chart, entry and managed-position regression suites.
Tests use mocks/temp files only; they do not submit live/test orders or send mail.
