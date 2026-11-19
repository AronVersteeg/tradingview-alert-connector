# BTC Trade Replay

On BTC Public V2, select **Trades laden** beneath the Binance Spot chart.
The panel is read-only. It uses the existing dashboard token in the
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
