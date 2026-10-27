# Exploratory BTC Impulse Study

Research only, requested comparison of the two largest Daily-intrusion replay
winners (2026-06-01 11:00 UTC short; 2026-08-19 15:00 UTC long). No changes to
live orders, filters, SL/TP/trailing, dashboards or mail. These are not newly
validated impulse labels for The List.

## Reproduce

The first command uses public GET requests only; others are fully offline.
Python requires the bundled numpy/pandas runtime. Node uses the repository's
ts-node dependency. Never import the trading application bootstrap.

```powershell
python scripts/fetch-btc-impulse-study.py <daily-replay-directory> <study-directory>
node -r ts-node/register scripts/build-btc-structure-study.ts <daily-replay-directory> <study-directory> <weekly-replay-directory>
python scripts/analyze-btc-impulses.py <daily-replay-directory> <study-directory>
python scripts/report-btc-impulse-study.py <study-directory>
python -m unittest discover -s test -p btcImpulseStudy_test.py -v
```

Keep captures/reports outside Git. The study directory retains raw public
REST responses, 32 daily metrics ZIPs and extracted CSVs, full/selected saved
monitor context, source URLs and calculated SHA-256 hashes, structure JSON,
all per-signal features, audit JSON and self-contained report HTML. ZIP CRC is
verified; hashes are locally calculated, not a claim of publisher signatures.
Successful files are cached locally so repeat runs do not redownload them.
418/429 stops acquisition, without retries or host bypass. No credentials.

## Sources And Availability

- Original 12,000 closed Binance BTCUSDT Spot hourly candles and original 125
  saved intrusions; the original 11 Daily-plus-intrusion signal identities are
  fixed. Compare all 11 independently to avoid omitting overlapping signals
  based on existing-position state. Two controls are profitable, not false
  impulses merely because they are not the two largest winners.
- 12,000 BTCUSDT USD-M Futures hourly candles, 12,000 ETHUSDT Spot hourly
  candles, and BTC Spot/Futures 5-minute bars covering six hours before each
  entry; 1H OHLC, base volume and taker-buy volume reconcile to 12 x 5m bars
  for every signal. Funding includes only settlements strictly before entry.
- Historical D/W/M fractals; only confirmed records before the signal open
  are eligible. Include fresh wick-intact breaks, first close breaks and
  recrosses as distinct notions. Distance to the next intact level is divided
  by that trade's original initial-stop distance, not a future trailing stop.
- Existing LuxAlgo-derived orderblock engine, pivot 5 / Wick. A new close
  break must cross the far edge of an opposing block known at candle open:
  bearish top for long, bullish bottom for short. Broken/unknown blocks are
  not intact barriers. Analyze age, touches, pivot-volume ratios, prior breaks
  and distances. Break metadata is verified against candle-prefix replay.
- Free Binance USD-M `metrics` daily ZIPs provide OI and long/short ratios
  beyond the REST API's one-month limit. Parse/sort timestamps, reject stale
  endpoint matches and show 0/5/10-minute label-lag sensitivity. A 5-minute
  lag is the main conservative assumption, not proof of original publication
  latency or archive-label semantics. Never align labels as exact availability
  timestamps without qualification.
- Saved CoinGlass event/review snapshots are timestamp-audited separately.
  No pre-entry event snapshot exists for June 1. August 19 has an event
  snapshot roughly 57 minutes before entry, not a fresh close-time snapshot.
  Later reviews cannot predict the entry. Do not infer historical depth,
  canceled orders, consumption/absorption or spoofing from current books.

References:
- https://github.com/binance/binance-public-data
- https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest-Statistics
- https://github.com/binance/binance-public-data/issues/491 (reported, unconfirmed labeling change)
- https://docs.coinglass.com/reference/spot-orderbook-heatmap (paid historical heatmap plans)

## Features And Bias Controls

190 candidate variables, 182 complete/varying across all 11 events in this
capture: volume normalization, base/quote taker delta, 1/3/6/12/24/72/168-hour
returns/flow, 1H/4H/1D Wilder RSI/ATR/ADX/DI, EMA distances, MACD, Bollinger
width, Donchian levels, daily/weekly VWAP, ETH relative returns, Futures/Spot
basis, funding, OI/positioning, intrahour end-flow and structure features.
Main search excludes alternate 0m/10m OI lags to avoid presenting uncertain
availability alignment as a better predictor. Missing features are not zero.

All rolling computations use past/current closed data; 4H/Daily resamples
include only completed periods. Explicit millisecond timestamp normalization
guards pandas datetime-unit changes. Indicator/resampling prefix-causality,
Wilder SMA seeding, signed volume accounting and lagged as-of joins have five
dedicated scientific unit tests. Features are direction-normalized where
appropriate: a positive value means aligned sell flow for shorts, buy flow
for longs. Taker delta does not identify position opening/closing.

Targets were selected using realized outcomes. Ranked one-variable thresholds
are fit to those two targets, not validated predictions. Search calibration
enumerates all 55 possible two-event groups: after the same feature search,
47 can be perfectly isolated by at least one one-sided threshold (85.5%).
This is a descriptive overfitting check, not an IID p-value or independent
validation. Some trades overlap, variables correlate, archive revisions and
intrusion availability are not proven. No automatic threshold optimization
or live filter installation is justified by this sample alone.

## First Findings

- Last-5m directional Futures delta: +30.44%, +25.86% for targets; other nine
  range -22.19% to +19.59%. Raw June 1 short delta is negative (-30.44%), not
  buying pressure. A post-hoc 25% gate isolates the two in this sample only.
  A 15m window does not preserve the clean separation: August 19 is +3.56%.
- Total Spot-volume ratios to the previous 24-hour mean are 1.63x and 19.08x.
  A high 2x/3x volume gate would discard the largest winner. Absolute last-5m
  net Futures flow is about $156.56m / $44.40m, while losing August 21 has
  about $135.73m. More volume or more dollars is not sufficient.
- Pre-signal 20h Bollinger width: 2.03%, 2.26%; smaller-winner September 21
  is 1.66%, other eight range 2.92%-7.48%. A 2.5% threshold is exploratory,
  not pre-registered or validated. Compression is more coherent than demanding
  extreme breakout volume.
- Next intact Daily barrier: 1.22R / 2.14R. A fixed >=1R check keeps the two,
  winning September 21 and losing May 28. It removes six of seven independently
  negative signals, but is not a perfect winner classifier.
- June 1 has no fresh opposing orderblock/W/M break at entry. August 19 has a
  Weekly break and breaks a 691-hour-old block, but losing August 21 breaks a
  2,296-hour-old block. September 21 breaks Monthly and a 5,586-hour-old block
  yet only earns a small profit. Age or more HTF breaks alone is insufficient.
- Entry-hour OI at the assumed 5m lag is +0.01% / -0.62%; no common large flush.
  This is not the old strategy's later Delay window. CoinGlass support/friction
  is ~1.94 for August 19 vs ~2.78 for losing August 21; no shared June evidence.

The research hypothesis to observe on new, timestamped signals is early
breakout from compression + aligned aggressive end-flow + room before the
next structural barrier. It remains a hypothesis, not a demonstrated cause
or a promise of future profit. No live changes were made.
