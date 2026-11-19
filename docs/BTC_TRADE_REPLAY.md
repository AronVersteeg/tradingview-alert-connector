# BTC Trade Replay

## Dashboard: Green Retest

The BTC Public V2 panel now shows a hypothetical retest, not the dYdX account
positions. It uses the current native Binance BTCUSDT Spot Public V2 frames
(up to 500 hours) to reconstruct Green and directional Yellow signals. No
historical account orders, fills, or older filtered chart markers are used.
Each trade is rendered in one chart, including the signal hour, entry, Yellow
checks, pivot/confirmation, initial and updated stop, and exit candle.

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

The retest is read-only, loaded on request, and cached for 60 seconds. It has
no dYdX account reads or writes and never invokes order placement. TypeScript,
offline execution tests, and a desktop/mobile fixture cover the revised panel.

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
