# BTC Public V2 candle chart

Only `/decentrader/map?source=open-v2` displays the new independent candlestick
chart, directly below the existing cohort/histogram/gap/intrusion map. Other
tabs retain their existing layout. No entry, alert, plan, SL, TP or trailing
code consumes this chart.

## Data

- Venue and symbol: Binance Spot BTCUSDT, UTC-based `1h` candles.
- Initial/historical reads: `GET /open-liquidity/v2/btc-spot-candles`.
- Earlier page: `?before=<earliest candle open in milliseconds>` (exclusive).
- Latest page: 1,000 candles, shared for 15 seconds between viewers.
- Older pages: five-minute cache, at most eight cached pages and eight requests
  in flight. Each browser retains at most 12,000 candles.
- Existing Binance HTTP cooldown and weight protection applies to REST reads.
- Browser live feed: `wss://stream.binance.com:443/ws/btcusdt@kline_1h`, with
  port 9443 as a reconnect alternative. Only the visible BTC chart connects.
- A stalled/disconnected stream falls back to shared REST reads every thirty
  seconds. The status distinguishes Live, REST and Paused; opening a socket
  alone is not considered proof of a live tick.
- The unfinished candle is visible and explicitly marked `closed: false` in
  the data. This is independent of confirmed closed-hour intrusion detection.
- OHLC and volumes are actual candle values, not OHLC4 or reconstructed cohorts.
- Amsterdam/UTC formatting changes labels only, never candle timestamps.
- No chart candle histories are written to disk.

## Extension Point

After first data load, `window.btcSpotChart` exposes the chart and series,
source identity, and `getCandles()` (defensive copies). The browser emits
`btc-spot-chart:ready`. Future custom visual layers can attach here without
coupling rendering to a trading executor. No strategy checkbox is added.

## Libraries

TradingView Lightweight Charts 5.2.1 is vendored as a standalone ES module;
selected Lucide Static 1.60.0 toolbar icons are vendored as SVGs. Production
does not depend on a CDN, a new package install or a third-party embed.
Licenses and the TradingView attribution notice are supplied beside the assets;
the chart includes the required visible credit/link and attribution logo.

Verified npm tarball integrity (SHA-512):

```text
lightweight-charts@5.2.1
IVwoK1RLFiLPubaKIjNbtjWLnpPMqiABSrTay6whmNa8L1+19292VtHJ+BWyPUuLCwF0tcQlhEWd1CLB2a1nsQ==
lucide-static@1.60.0
Y8/IYjBYST40TuqzknGt1XJOcZp83UH5fLndgXlQCQ4lM97eZO3EQo6cIyDcBrprredIGDVxge8yslRyM4wD0Q==
```

## Read-Only Preview and Verification

Run `node -r ts-node/register/transpile-only scripts/preview-btc-spot-chart.ts`.
This separate server binds loopback on port 4318 (override with
`SPOT_CHART_PREVIEW_PORT`) and deliberately never imports the main controller or
exchange initialization. Do not start the production entry point for UI tests.

Unit tests: `test/binanceSpotChart.test.ts`.
Browser QA: `node scripts/check-btc-spot-chart.cjs <preview URL> <output dir>`
with Playwright and pngjs available in the test runtime. Set
`SPOT_CHART_BROWSER_CHANNEL=msedge` to use installed Edge instead of a downloaded
Chromium test binary. QA checks actual green/red canvas pixels, desktop/mobile
layout, pagination, tools, real websocket reception and absence of write requests.
