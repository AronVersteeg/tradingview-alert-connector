# BTC Intrusion / Daily Strategy Replay

Offline research harness. It does not add dashboard controls, HTTP routes,
trade executors, timers or emails. Do not import the application bootstrap.

## Inputs And Reproduction

Use Node with the repository's existing ts-node dependency. Playwright and
pngjs are used only for rendering and screenshot QA, from the bundled runtime
when they are not installed locally.

```powershell
node scripts/fetch-btc-replay.cjs "C:\path\to\replay-output"
node -r ts-node/register scripts/replay-btc-intrusion-daily.ts "C:\path\to\replay-output"
node scripts/render-btc-replay.cjs "C:\path\to\replay-output"
```

Only the first command uses the network. It performs public GET requests:
Binance server time, BTCUSDT Spot 1H/Daily klines, the saved Decentrader intrusion
payload and the non-secret risk/trailing status. No API keys are needed. Do not
export credentials. Capture requests are sequential, with 700ms between hourly
pages. HTTP failures stop rather than repeatedly retrying a banned host.
The default capture includes 12,000 closed 1H candles (500 days), extending
further only when the earliest saved intrusion requires more stop warmup.
Fresh captures include both Daily and Weekly klines. The runner defaults to
`D`; pass `W` as its second argument to test the Weekly variant.

For a fair comparison using a pre-existing Daily capture, fetch only the
additional Weekly candles to the **original cutoff**, into a new directory:

```powershell
node scripts/fetch-btc-replay.cjs "C:\path\to\weekly-output" --from "C:\path\to\daily-output\inputs.json"
node -r ts-node/register scripts/replay-btc-intrusion-daily.ts "C:\path\to\weekly-output" W
node scripts/render-btc-replay.cjs "C:\path\to\weekly-output"
```

This keeps hourly candles, saved intrusions, Daily inputs, risk settings and
cutoff unchanged. The augmented capture records the base input SHA-256 and
the additional retrieval manifest. It refuses to overwrite its base input.

The compact `inputs.json` retains the exact relevant source fields, raw klines,
cutoff and retrieval manifest (URLs, times and response SHA-256). Analysis is
reproducible offline. The report additionally records the input file SHA-256.
Keep captures and generated reports outside Git.

Outputs: `rapport.html`, `replay.json`, `signals.csv`, `trades-single.csv`,
`trades-all-signals.csv`, overview/trade/all-signal high-resolution PNGs,
and desktop/mobile render QA. The HTML is self-contained, with SVG charts and
no scripts or external assets. Open it directly, without a development server.

## Exact Strategy Being Tested

- Reuse `compactIntrusionHistory`, `projectDailyFractals` and
  `buildIntrusionDailyBreakouts`, the same evaluators as the purple overlay.
  Alert timestamps are candle-open, never SMTP time. BTC intrusions come from
  the saved Decentrader history, not the current active-map flags or a newly
  invented Binance intrusion reconstruction.
- Only a fresh Daily fractal close break in the intrusion's direction on the
  same 1H candle qualifies. Wick breaks and confirmed-at checks follow the
  current overlay exactly. No OI/Delay/delta/color/orderblock/TP filters.
- In `W` mode, replace the Daily requirement with a fresh Weekly fractal
  close break. A simultaneous Daily break is neither required nor excluded;
  Weekly highs/lows often coincide with Daily levels. There is no additional
  green-gap-edge break requirement. Multiple Weekly levels crossed on one
  intrusion candle still yield only one entry. The detector uses the same
  pure close-break evaluator as the Daily and gap/Weekly overlays; no live
  checkbox, entry model or execution configuration is changed.
- Entry is next 1H candle open, after the signal candle closes. This is a
  hypothetical fill assumption, not evidence of actual low-latency fills.
- Reuse the live `buildFractalStop` with Spot highs/lows: window 2, lookback 72,
  buffer 0.1%, initial minimum-distance widening disabled, trailing delay 0
  unless captured live status differs, maximum distance from captured status.
  Risk is the requested $15 per trade, also checked against live status.
  The window/lookback/buffer are supplied settings/code defaults, not remotely
  verified values because the status does not expose them.
- Position size is $15 / absolute entry-to-initial-stop distance, without
  compounding, rounding or collateral caps. Fees/gaps can exceed $15 losses.
- Stops must already be active before the candle's wick can hit them. New
  confirmed fractal stops activate at the next candle, tighten only and require
  a newer pivot than the previous anchor. No TP orders and no fixed-duration
  exits. An adverse opening gap exits at that open, not the better stop price.
- One-position mode skips signals arriving while a position is open at the
  next open, even if that existing position stops later in the same candle.
  Independent mode gives every signal its own virtual trade, allowing overlap.
  That diagnostic mode is not a realizable single BTC net-position portfolio.
- Open positions remain open and marked to market separately. The exact stop
  execution time inside an hourly bar is not known. Reported drawdown is only
  realized equity drawdown, not intrabar floating-equity drawdown.

## Required Interpretation

This is an **alert-time hypothesis**, not a verified live backtest. Saved
records do not establish that an intrusion was known at that candle's close;
some records were reconstructed later. A later SMTP time neither proves first
observation nor inbox delivery. Missing observations remain unknown, not zero.
The archive is a partial sample; lack of a saved intrusion cannot be treated
as proof that the market produced none. All result claims must preserve this
selection/availability limitation.

The price/fractal audit removes all future Daily and hourly candles at each
signal close and verifies identical historical signals. This guards against
price-derived lookahead but cannot establish intrusion timing availability.
The engine passes only historical row prefixes to the stop selector as well.

Live stops use dYdX candles/oracle/execution, not Binance Spot. Shorts here are
synthetic linear exposure to Spot prices, not unborrowed Spot short sales.
USD-equivalent assumes 1 USDT = 1 USD. Base results exclude fees, funding,
spread, execution delay and slippage; 5bp/2bp and 10bp/5bp per-side fee/slippage
scenarios are explicit assumptions, not verified venue fees. Do not interpret
small-sample profitability, especially concentration in one winner, as a
statistically established edge or a guarantee of future profit.

## Verification

`test/fractalStrategyReplay.test.ts` covers entry timing, exact initial risk,
both directions, adverse gaps and costs, historical prefixes, activation of
trailing after confirmation, tighten-only/newer-pivot rules, no future stop
rescue, overlaps, conflicts/deduplication, missing history/stops, open positions,
realized drawdown and parity with the real live fractal selector.

The screenshot renderer blocks all HTTP(S) requests, checks rendered chart
pixels and validates desktop/mobile overflow. It never launches the trading
application or interacts with the live dashboard.
Tests also cover W-only selection, D/W coincidence, multiple levels per
signal, exact intrusion direction/hour, closed/confirmed/fresh levels and
prefix causality. The original Daily capture was re-evaluated after adding W:
all fills, stop trajectories, skipped signals and results matched exactly.

## First Capture: 2026-10-27

12,000 hourly candles through 13:00 UTC, 125 unique saved intrusions, 11 purple
signals. One-position replay: nine closed trades, three wins and six losses,
$106.72 gross. Assumed 5bp fee plus 2bp slippage per side: $99.73 net; 10bp/5bp:
$91.65. The largest winner contributed $101.06 gross (94.7% of net gross-case
P&L); excluding it leaves $5.66 gross and $0.43 under the 5bp/2bp assumptions.
Independent overlapping signals: 11 closed trades, $104.68 gross.

These are not out-of-sample results. The available saved intrusion timestamps
run from February to September 2026, with unknown completeness and first
observation timing. The positive aggregate does not establish a robust edge.
All 11 price-signal prefix audits matched; this does not resolve the separate
intrusion-availability limitation. Sixty offline suites / 550 tests passed.

## Weekly Comparison: Same Capture

481 Binance Weekly klines were fetched to the original 2026-10-27 13:00 UTC
cutoff. Unfinished weeks are excluded. W selected three signals and three
closed trades, with no overlapping entries and no realized losses:

| Signal candle-open, Europe/Amsterdam | Direction | Entry, Europe/Amsterdam | Gross P&L |
| --- | --- | --- | --- |
| 2026-06-04 03:00 | Short | 2026-06-04 04:00 | $1.59 |
| 2026-08-19 17:00 | Long | 2026-08-19 18:00 | $30.50 |
| 2026-09-21 10:00 | Long | 2026-09-21 11:00 | $7.30 |

Total: $39.38 gross, $38.07 after assumed 5bp fee / 2bp slippage per side,
or $36.54 under 10bp/5bp. Independent and single-position results coincide.
The best winner contributes 77.4% of gross P&L. All three price-signal prefix
audits matched. Three historical winners do not establish a reliable 100%
win rate, a causal live signal or a statistically proven edge. In particular,
the June short entered after most of the large earlier Daily-triggered move.
