# Strategy Retest and BTC Trade Replay

## Dashboard: Green and Purple Retest

The BTC, ETH, SOL and ZEC Public V2 panels show a hypothetical retest, not
dYdX account positions. For each market, the archive is reconstructed from
its own Binance USDT Spot and corresponding dYdX USD 1H candles. Green and
directional Yellow signals are computed with the same causal cohort model
as the recent native Public V2 frames, not with older filtered chart markers.
No historical account orders or fills are used.
Each trade is rendered in one chart, including the signal hour, entry, Yellow
checks, pivot/confirmation, initial and updated stop, and exit candle.

Green requires a Spot intrusion and a gap close break. Purple additionally
requires a same-direction intrusion and the first 1H close through a previously
confirmed Daily level on that candle. Purple reconstructs the available Daily
history at each signal using the same Williams and close-break projection as
the live chart. Both modes share the stop rules, but simulate positions and
P&L separately, over the same market-specific time range.

- The model starts flat at the beginning of this window, allows one position at
  a time, and does not trigger another entry while one is running.
- Entry uses the next dYdX hour open as a fill proxy. Sizing uses the live fractal
  selector and a fixed $15 simulated price-risk budget, capped by distance to
  the body-open stop. It does not model historical collateral, exchange depth,
  execution latency, fees, funding or tick/size rounding.
- The initial stop is the exact dYdX signal-body open. A later same-direction
  Yellow can tighten only to a usable dYdX fractal known at that close. Stops
  affect following candles only. An existing stop hit inside an hour takes
  precedence over the Yellow at its end. No TPs. An adverse opening gap exits
  at the adverse open. The exact intrabar exit time is unknown.
- An unresolved missing dYdX hour while a position is open ends the retest with
  an incomplete trade. Missing prices, fills and stops are not fabricated.
  Exchange stop execution can differ from the simulated OHLC stop hit.
- Per-trade gross P&L appears in the trade picker and summary. Closed trades
  use the simulated exit price; an open trade is marked to the last closed
  dYdX hour in the replay. That value is explicitly unrealized. Incomplete
  trades show no estimate; no trading fees, funding or slippage are deducted.
  The amount next to the trade picker sums all available estimates in the
  list, including unrealized P&L, before those costs. Unknown values are excluded.
- The toolbar shows the default budget and accepts a temporary USD budget per
  trade. Size, initial price risk and P&L scale proportionally in the browser.
  This leaves all signal, entry, stop and exit prices unchanged and does not
  touch the live configuration. Clearing the field restores the default.

The retest is read-only, loaded on request, and cached for 60 seconds. It has
no dYdX account reads or writes and never invokes order placement. The recent
500-hour Spot window can extend the archive when at least 24 overlapping
hours agree on Green and Yellow events, and dYdX has consecutive new candles.
Both periods are simulated together, so an open position cannot reset at the
join. Without a verified join the dashboard explicitly shows the archive cutoff.
With no usable archive it falls back to the recent window only.

The bundled BTC and ZEC retests start on 1 January 2025. ETH and SOL start
on 11 October 2025 at 02:00 UTC, after missing dYdX hours that can interrupt
open positions under the configured stop guards. A replay starts flat; it
does not infer that an earlier position survived the missing candles.

The public endpoint is `GET /open-liquidity/v2/green-retest?market=ETH-USD`
with optional `strategy=green` (default) or `strategy=purple`, and optional
`id`. The older BTC-only URL remains an alias for BTC Green. Refresh
each `research-data/*-green-retest-history.json` from the repository root
before the recent window no longer overlaps it. No heavy historical downloads
or rebuilding run on Render. Pass explicit UTC bounds for a reproducible build:

```shell
node -r ts-node/register scripts/build-green-retest-history.ts ETH-USD 2025-10-11T02:00:00Z 2026-11-19T20:00:00Z
node -r ts-node/register scripts/build-purple-retest-history.ts ETH-USD
```

The Green builder warms up the Spot model with 8,760 consecutive candles.
The Purple builder uses that archive and writes a small signal sidecar for
the same period, without duplicating the dYdX history. Missing dYdX source
hours are left blank, never filled with Binance prices.

## Legacy Protected Account Replay API

The earlier account replay API remains read-only and is not used by the Green
retest panel. It uses the existing dashboard token in the
`X-Webhook-Token` header, never in the URL. Account history is not public or
included in other dashboard payloads.

- Positions are actual BTC-USD positions from the configured dYdX subaccount 0,
  using the indexer's open/close timestamps and average entry/exit prices. These
  are position averages, not individual fill markers. Positions entered outside
  this bot can also appear; no strategy is inferred from an exchange position.
- The latest 100 account positions are requested and up to 16 BTC positions are
  displayed. The candle request runs only when a position is selected and is
  capped at 168 closed 1H candles. Long positions can use the separate exit
  window. Missing candles are not interpolated.
- The chart uses the same dYdX candle normalization as the stop engine. The
  contextual Williams 2/2 markers appear on their pivot only after confirmation;
  they are not evidence that a particular fractal moved a stop.
- The separate bounded journal asynchronously records BTC stop orders that
  the executor sees confirmed through the dYdX indexer. It records observation
  time, not the exact exchange activation time. The plotted dashed stop path
  connects observed levels only; it does not imply continuous coverage or
  extrapolate past the last observation. Older or externally managed stops
  without journal evidence are left blank. Entry and exit markers are aligned
  to the containing candle; the displayed times are from the exchange.

No change to entry rules, order requests, stop selection, retries, trailing,
notifications or position ownership. Chart requests never run inside the
execution or protection checks. No new environment setting is required.

Tests use temporary files and fixtures. The offline
`scripts/check-btc-trade-replay.cjs` verifies desktop/mobile chart pixels and
scrub/play behavior, without starting the live trading service.
