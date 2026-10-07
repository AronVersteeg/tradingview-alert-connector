# Public V2 closed-hour timing

Public V2 intrusions and BTC manual overrides first check at the hour boundary
plus five seconds. If the newest closed candle is not available, they retry
every five seconds during the first thirty seconds, then use bounded/minute
retries. Existing Binance cooldown and request-weight protection stays enabled.

## Fast Path

At startup each Public V2 collector reconstructs its raw active cohorts from the
source candles once. These bootstraps remain serialized to limit memory peaks
and yield to the event loop so manual checks are not blocked by the whole replay.

After warmup, a collector processes only consecutive missing closed candles.
It retains raw cohort prices and birth indices, applies the same liquidation
sweeps and rolling 8,760-birth expiry, and advances the same compact 500-frame
seed/delta history. Warm refreshes do not wait for other markets' bootstraps.
Repeated checks with no new candle do not create cohorts again. Corrections to
the last processed candle rebuild the state instead of applying it twice.
Missing or open candles cannot be treated as a current confirmed intrusion.

The updated in-memory history is immediately available to the intrusion monitor.
Historical disk persistence runs ten seconds later and is not an alert gate.
No manual plan, SL, TP, trailing, trade enable flag or entry qualification rule
is changed by this optimization.

## Verification

`GET /open-liquidity/v2/status?market=BTC-USD` exposes `liveReplayReady` and
`lastRefreshTiming`: mode, queue/fetch/calculation/total milliseconds, number of
processed candles, candle-open, candle-close and time elapsed after close.
The same timing is logged with each replica refresh. A warmed normal hour should
show `mode: incremental` and `processedCandles: 1`, not a full-year replay.

For a real intrusion, also compare the stored `candleClosedAt`,
`firstObservedAt` and `smtpSentAt`. Refresh completion is not SMTP acceptance,
and neither proves inbox delivery time. Five seconds is the first check target,
not an unconditional latency guarantee during restart, outage or cooldown.

`GET /decentrader/manual-override?market=BTC-USD` exposes
`lastCandleObservation` when armed plans are checked: Futures source, candle
identity, close value, actual observation time and elapsed milliseconds after
close. `triggeredAt` records detection, not the earlier request start.
The existing manual trigger email still reports the execution result and is
sent after order setup, so its timestamp is not the detection timestamp.

## Future Combined Entry

The optional manual-entry/intrusion checkbox is NOT implemented. Current manual
entries use Binance Futures closed candles; BTC Public V2 uses Binance Spot.
A future combined gate must explicitly choose its price source, compare the
exact candle identity and await both observations before executing once.
Email arrival timestamps must not be used to match candles.
