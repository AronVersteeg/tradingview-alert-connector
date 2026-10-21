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
- The Spot view has no grid, volume histogram or volume legend, and no Volume/
  Grid toolbar checkboxes. Candles use the full available price area. Underlying
  Binance volume/quote-volume data and all strategy/storage inputs are unchanged.

## Pan and Zoom

- Like the histomap, the Spot chart uses one pointer/wheel gesture controller
  with pointer capture, `touch-action: none`, anchored zoom and frame-coalesced
  movement. Lightweight Charts still owns all rendering and scale calculations;
  its native scroll/pinch/axis handlers and inertia are disabled to avoid
  competing gestures or post-zoom snap corrections.
- Axes retain their candlestick orientation: time horizontal, price vertical.
  One finger/mouse drag pans both; two fingers pinch both scales around their
  midpoint and may translate that midpoint. Two-to-one-finger transitions rebase
  the gesture to avoid jumps. Lifting/cancelling releases capture and stops motion.
- Wheel zoom uses the histomap's .84/1.18 steps; +/- uses .8/1.25. A drag on
  the right price axis or bottom time axis scales only that axis. Mouse hover
  retains the native crosshair; tapping selects a crosshair, double-tap/click
  and Reset restore the default time view and automatic price fit.
- Manual pan freezes live following and price auto-fit until a time preset or
  Reset is chosen. REST refreshes and websocket ticks do not undo that view.
  History prepends rebase an in-progress gesture. The live 50-slot projection
  buffer is applied in the gesture calculation, not by a second correcting loop.
- Only the canvas consumes gestures. Controls and surrounding dashboard content
  remain normally scrollable. No trading or source/data behavior is changed.

## Extension Point

After first data load, `window.btcSpotChart` exposes the chart and series,
source identity, and `getCandles()` (defensive copies). The browser emits
`btc-spot-chart:ready`. Future custom visual layers can attach here without
coupling rendering to a trading executor. No strategy checkbox is added.

## Daily / Weekly / Monthly Projections

- Same source as the BTC fractal tables: confirmed Binance Spot BTCUSDT Daily,
  Weekly and Monthly Williams fractals (two closed candles on either side).
- Daily yellow, Weekly orange, Monthly red. DC/WC/MC denote the latest confirmed high and low
  for each timeframe; older levels use D/W. These are exact prices, no buffer.
- A line starts at the first exact matching 1H wick inside its pivot day/week.
  This is a retrospective visual anchor: availability is still `confirmedAt`,
  after the two right-side higher-timeframe candles close. It is not an earlier entry signal.
- Intact levels extend right. A strict high/low wick crossing breaks the level;
  an exact touch does not. Only closed 1H candles count. The whole broken segment
  becomes dashed and ends at the first breaking hour, with no extension past it.
- The existing D/W/M history supplies older breaks; closed 1H candles resolve
  their precise hour and any new breaks in the unfinished day/week/month. This layer
  does not mutate the higher-timeframe tables or any trading source.
- Origins before loaded history are clipped at the left edge. Partly loaded
  pivot/break periods are not guessed; scrolling back resolves their exact hours.
  A complete loaded pivot-period tail with no matching wick proves the origin
  lies to its left, so the line stays clipped rather than disappearing.
  No per-level historical downloads or additional disk histories are introduced.
- `GET /open-liquidity/v2/btc-spot-fractal-projections` bundles the existing
  shared D/W/M caches once per closed hour. The visible chart refreshes on the
  closing websocket candle, with a five-second watchdog and shared REST recovery
  if the final websocket candle was missed. It never treats an old open candle
  as final just because wall-clock time advanced.
- Missing/stale D/W/M data or gaps in closed 1H coverage leave the previous
  verified projections in place with an explicit pending/stale status. No
  promise of a fixed refresh latency during an upstream outage is made.
- Daily, Weekly, Monthly and Broken visibility controls are independent. `Fit levels`
  optionally includes current intact levels in the price scale. Older distant
  levels cannot squeeze the normal candle scale. At the exact same price,
  Monthly takes precedence over Weekly, which takes precedence over Daily.
  Only the highest enabled/renderable timeframe's line and label render.
  Underlying data stays unchanged and lower-timeframe lines reappear if the
  higher timeframe is switched off.
- `getFractalProjections()` exposes defensive copies of verified projections
  and their closed-hour cutoff for read-only inspection. PNG export includes
  the primitive layer. No entry, SL, TP or trailing logic uses these projections.

## Monthly Tables and Calendar Rules

- `GET /research/binance-monthly-fractals?market=BTC-USD` uses Binance's `1M`
  klines and the same shared Williams engine as D/W. The open month is excluded;
  two complete right-side months must close before confirmation. Calendar-month
  ends use UTC dates, including February/leap years and December/January, never
  a fixed 30-day duration.
- Monthly high/low history and a separate intact list use the existing D/W
  table renderer, state rules, exact decimals and mobile scrolling. They appear
  on the same seven dashboards. Crypto uses Spot; GOLD/XAUUSDT and SILVER/XAGUSDT
  retain their existing explicit Futures source because those symbols have no
  Spot market. The candle-chart Monthly projection is BTC Spot only.
- Monthly history is shared/cached until the next monthly close, with the same
  cooldown, backoff and stale fallback as D/W. No new disk history, environment
  variable, entry condition or trading automation is added.

## 1H Williams Boxes

- Enabled by default through the independent `1H boxes` checkbox. Translucent
  light-gray stepped price ranges render behind candles. Higher-timeframe
  projection styling stays independent.
- Range bounds are the last confirmed 1H Williams high and low, using the same
  two-left/two-right rule and newest-equal-pivot tie handling as the existing
  `confirmedWilliamsFractals` implementation (covered by a parity test).
- Box transitions are anchored retrospectively at the original pivot wick,
  matching the reference illustration. A transition is only added after both
  right-side candles have closed. `confirmedAt` retains the actual availability
  time; a historical box is not evidence that its pivot was known in advance.
- The last confirmed range extends across the live candle, but live high/low
  excursions cannot confirm a pivot. Missing hourly data resets the range;
  no confirmed bounds are inferred across a gap.
- Computation runs locally on already loaded Spot candles when closed candles
  or history change. No extra REST/WebSocket connection, storage, trading rule,
  alert, SL, TP or trailing change. `getHourlyFractalBoxes()` exposes defensive
  copies of the ranges and pivots. PNG export includes the boxes.

## Public V2 Gap Rectangles

- Default-on light-green outlines use exactly `payload.gaps` from the BTC
  histomap, without recalculating gaps from candles, querying Decentrader or
  using a different Binance source. The existing histomap refresh publishes a
  compact, validated timeline to the candle chart; there is no additional gap
  fetch, Binance request or history file. This also works when the chart is
  opened after the map has loaded, and is independent of the map's frame slider.
- Each pair of unchanged lower/upper edges is one rectangle. Changes to either
  edge close that rectangle and start the next at the same source timestamp.
  Source frames are candle-open timestamps; the edges become available at
  candle-open + 1H. New transitions use that close, never the browser fetch time.
- The active rectangle ends ten 1H candle slots beyond the latest candle by
  default. The `+ h` numeric control adjusts this to 1-72 candle slots without
  inserting fake candles or moving historical transitions. `Fit levels` can
  include the current gap bounds alongside current D/W/M fractals.
- Live presets, reset and zoom retain at least 50 empty logical 1H candle slots
  beyond that projected right edge (not merely beyond the latest candle).
  Wheel, pinch, button and time-axis zoom are constrained while viewing live
  data. At maximum zoom at least 12 actual
  candle intervals remain alongside the reserved future area. Historical panning
  and zooming remain available without snapping a historical view back to live.
  Live updates, resizing and changes to the edge offset preserve the buffer.
- The 500 retained frames do not in themselves establish the first gap's
  original birth. Unknown starts remain open on the left. The current
  79,400-87,400 gap has the user's explicitly supplied start at 2026-09-21
  10:00 Europe/Amsterdam (08:00 UTC), tagged `startSource: user-confirmed`.
  Its applicability is bounded by the observed continuity through Oct 21
  08:00 UTC, so an unrelated future gap at the same prices is not backdated.
- Missing frames/null gaps interrupt the outline. A stale active gap is dashed
  and stops at the last verified close instead of projecting an unverified
  current range into the future. The displayed cutoff makes source lag visible.
- `getGapProjections()` exposes copied boxes, source cutoff and start provenance.
  Gap visibility, price formatting and PNG export work like the other layers.
  The two active edges also have native light-green right-price-axis labels,
  using the series price formatter. Labels update with the edges and disappear
  when Gap is hidden, no current gap exists or the source is stale. Historical
  rectangles never receive active price labels.
  No entries, plans, alerts, SL, TP or trailing settings are changed.

## Manual Entry and TP Overlays

- BTC Public V2 mirrors the existing manual plan snapshot, without an extra
  API poll, Binance read, token, order or plan mutation. Only ARMED/EXECUTING
  plans are drawn; historical/terminal plans disappear. This is a plan overlay,
  not a view of actual exchange fills or dynamic exchange TP orders.
- Blue solid entry lines and dashed individual explicit TP lines have native
  price-axis labels. Titles identify long/short, the parent entry and TP number.
  Equal-price levels share a label. Plans without explicit TPs get only an
  entry line; no targets are inferred. No SL or risk area is drawn.
- All planned blue boxes share a compact display width. The first column spans
  12 candle slots before the latest candle to the configured right offset,
  aligned with the gap edge.
  The display window rolls forward with candles; old arming timestamps cannot
  stretch a box across weeks of history. Original arming metadata is unchanged.
  Overlapping entry-to-furthest-TP price zones are assigned separate columns in
  existing plan order. The first column retains the gap-edge alignment; later
  conflicting plans sit to its right, with the same width and a two-candle gutter.
  Disjoint price zones can share the first column; touching endpoints alone do
  not count as overlap. Changes/cancellation recompute the display layout.
  Live presets/zoom automatically reserve enough future space for extra columns
  while retaining at least 50 candle slots beyond the actual gap edge. Explicit
  historical pans remain undisturbed. `getManualProjections()` exposes copied
  price intervals and logical column bounds for read-only inspection.
  Consecutive profit-side
  bands run entry-to-TP1, TP1-to-TP2, etc., darkest near entry and progressively
  lighter. Targets are ordered by price in the trade direction for shading.
  The fills use 50% opacity with stronger dark-blue/blue/sky/ice-blue steps;
  solid 2px light-blue separators mark each exact TP boundary within its box.
- Existing successful refreshes publish a compact entry/TP-only event. A
  save/edit/cancel already immediately refreshes plans; remote changes follow
  the dashboard's existing refresh cadence. Failed reads preserve last known
  levels with an explicit stale status. The event/cache also handles lazy chart
  initialization and an initially empty plan list.
- The default-on Manual / TP checkbox toggles lines, labels and boxes together;
  Fit levels optionally includes them. PNG exports include both lines and bands.
  All entry execution, emails, SL, TP allocation and trailing behavior is unchanged.

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

Unit tests: `test/binanceSpotChart.test.ts`, `test/btcFractalProjections.test.ts`,
`test/btcHourlyFractalBoxes.test.ts`, `test/btcGapBoxes.test.ts`,
`test/btcManualLevels.test.ts`, `test/btcSpotViewport.test.ts`,
`test/btcSpotInteractions.test.ts`.
Browser QA: `node scripts/check-btc-spot-chart.cjs <preview URL> <output dir>`
with Playwright and pngjs available in the test runtime. Set
`SPOT_CHART_BROWSER_CHANNEL=msedge` to use installed Edge instead of a downloaded
Chromium test binary. QA checks actual green/red canvas pixels, desktop/mobile
layout, pagination, tools, real websocket reception and absence of write requests.
It also checks exact fractal origins, yellow/orange pixels and PNG export, and
uses a separate synthetic hour-rollover fixture to verify automatic refresh,
ignoring open-candle excursions, the exact broken-line endpoint, and a 1H box
confirming only after its second right candle closes. An on/off pixel comparison
checks the actual gray fill on desktop and mobile.

Manual overlay QA: `node scripts/check-btc-manual-levels.cjs <preview URL> <output dir>`.
This fully intercepted desktop/mobile fixture serves synthetic plans and candles,
uses the actual dashboard read/render bridge, and blocks all non-read requests.
It checks long/short TP bands and their progressive blue shades, native axis
labels, edits, removals, stale reads, lazy initialization, toggling and PNG export.
It never submits or alters a production plan.
Real browser mouse/touch events also check two-axis dragging, wheel and pinch
zoom, axis-only zoom, two-to-one-finger transitions, cancellation, reset,
view preservation across refreshes and scrolling outside the chart.
