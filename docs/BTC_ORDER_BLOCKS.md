# BTC Spot Order Blocks

Chart-only overlay on BTC Public V2. No changes to entry models, manual plans,
exchange execution, SL, TP, trailing or existing data collection. ETH's chart
retains its existing controls and behavior.

## Provenance

Adapted from LuxAlgo's publicly published **Order Block Detector** Pine source:
https://www.luxalgo.com/library/indicator/order-block-detector/
TradingView publication: https://www.tradingview.com/script/KvGhxGxY-Order-Block-Detector-LuxAlgo/

Copyright LuxAlgo. The derivative `public/btc-spot-chart/order-blocks.js` is
licensed CC BY-NC-SA 4.0: https://creativecommons.org/licenses/by-nc-sa/4.0/.
Keep its attribution and license notice when sharing it. This is not a change
to the license of unrelated application files and is not an official LuxAlgo
product. Public/commercial redistribution must respect the source license.

The supplied background conversation was read before implementation:
https://chatgpt.com/share/6adf5404-b644-83eb-a4e3-a33a3f1e1f00
Its core distinction is retained: OHLCV-derived zones, **not resting orders**.

## Rules And Intentional Differences

- Actual Binance Spot BTCUSDT 1H OHLC and base `volume`, not cohort heights,
  quote volume, Futures, Decentrader or an order-book feed.
- Default volume pivot length 5. Confirmation requires five closed candles on
  each side. The zone is drawn at its original pivot; it was only known at
  `confirmedAt`, five candles later. Open/future candles cannot confirm it.
- Volume pivot: candidate at least as large as older neighbors and strictly
  larger than newer neighbors; zero volume does not form a block.
- Price state and high-branch precedence follow Pine's `os` calculation.
  Bullish zone: low to HL2; bearish: HL2 to high. Average line: zone midpoint.
- Default Wick mitigation uses rolling lowest low/highest high over the pivot
  length. Close uses rolling lowest/highest close. Strict beyond-edge crossing;
  an exact touch does not mitigate. This is not a separate breaker-block model.
- Original LuxAlgo hides mitigated zones and displays only the latest three
  active zones per side. This requested adaptation keeps **all** reconstructed
  zones. Broken zones end at the first observed mitigation candle close and
  remain visible, with fill, outline and midpoint intensity multiplied by 0.5.
- A missing hour terminates coverage, not a proven break. No pivot or state is
  carried across missing data. Such zones are marked `coverage-gap`, not broken.

## History And Updating

The checkbox is on by default. The numeric pivot input and Wick/Close selector
recompute the same loaded OHLCV history. Detection updates after closed hours,
REST corrections and history prepends, not on unfinished-candle excursions.

Visible, enabled BTC charts gradually reuse the existing candle pagination,
at least 2.5 seconds between automatic pages, to the existing **12,000-candle**
browser bound (about 500 days). This preserves the existing shared Binance
request limiter and bounded server cache. Background/hidden/offline charts and
unchecked overlays stop automatic history loading. Failed pages retain valid
history and use the existing retry delay. Chart panning remains available.

This is reconstruction from loaded candles, not a new persistent block archive
or an all-time history guarantee. Older active zones born before loaded history
cannot be inferred; settings and the beginning of history can affect results.
The first 2 x pivot-length candles of each contiguous segment are warmup.
`window.btcSpotChart.getOrderBlocks()` exposes defensive copies, coverage,
origin/confirmation/break times and settings for read-only inspection.

## Active Cluster Labels

Only intact blocks contribute to price-range labels. Overlapping or touching
price intervals form one cluster, including nested blocks and transitive chains.
Each label shows the complete cluster range (lowest bottom to highest top), not
the narrower intersection or the midpoint. Bullish/bearish colors are muted;
mixed-direction clusters use neutral text. Broken and coverage-gap blocks never
widen the range or receive a label.

Labels sit 10 pixels to the right of the current green gapbox edge, using the
same exact coordinate as that box, including its adjustable candle lead and
stale-history endpoint. They track pan, zoom, refresh and new closed hours. They
hide with the orderblock checkbox or when the gapbox is hidden/offscreen/absent.
Narrow columns use two lines; vertical packing prevents text collisions, and
labels without sufficient room are omitted rather than clipped across the axis.
Prices retain fractional-cent midpoint precision, with up to eight decimals.
`getOrderBlockClusters()` and `getOrderBlockLabels()` expose read-only snapshots.
Desktop cluster price/volume text uses 15px (1.5x its previous 10px); the existing
680px mobile breakpoint retains 10px. Crossing that breakpoint refreshes labels
without reloading data. Wrapping and collision packing follow the actual font
size, with limited shrink-to-fit only when the right column is constrained.

### Cluster Volume

Labels also show `PV ~1.2K BTC #1`: approximate compact **pivot-candle volume**
and the rank among all active clusters in the loaded history (#1 is largest).
The cluster's price position is unchanged. Equal volumes share a rank; ranks
recompute when blocks appear, break or older candles are loaded. Labels with
larger PV take priority if the pane cannot fit every label without collisions.

`pivotVolumeBtc` sums actual Binance Spot base volumes from unique block-origin
1H candles. A candle appearing twice inside a cluster is counted once. Broken
or unknown blocks do not contribute. Missing, negative or contradictory volume
produces `PV n/a`, not zero. API snapshots retain the unabridged numeric total,
`volumeRank`, `volumeSharePct`, `volumeCandleCount` and the explicit source
`origin-candle-base-volume`.

PV is a contextual proxy for relative historical traded activity. It is **not**
volume traded specifically inside the block's price band, cumulative volume
since block creation, current order-book depth, unfilled institutional orders,
or a proven measure of support/resistance strength. No allocation of the whole
candle's volume to individual price levels is assumed.

## Verification

- `test/btcOrderBlocks.test.ts`: coordinates, confirmation, volume ties, price
  state, strict Wick/Close, historical retention, causal replay, missing hours,
  invalid inputs and exact half-opacity rendering.
- `node scripts/check-order-block-source.cjs`: independent comparison with the
  vendor's public preview coordinates. The reference chart is equities/Daily;
  its OHLCV bar sequence is compacted only for algorithm comparison. This is
  not a live BTC source or proof of complete TradingView event-by-event parity.
- `node scripts/check-btc-order-blocks.cjs <read-only preview URL> <output dir>`:
  intercepted desktop/mobile fixture, canvas pixels and half-strength checks,
  checkbox, parameters, hourly update, pagination and no write requests.

Use `scripts/preview-btc-spot-chart.ts` for QA. Do not start the trading
application or submit orders to test a visual overlay.

Verified on 2026-10-26: the vendor preview's three displayed active zones matched
our latest-three-per-side comparison over its 260 QQQ Daily reference candles
(price tolerance 1e-10 for binary floating-point). Nineteen additional broken
zones were retained. Source SHA-256:
`35aca7f1e5e376531f752b44ccdea0c24bd11db45b1f5cf9f9584114b11aa8e4`.
Desktop/mobile fixture pixels measured green-channel contributions of 25 for
active zones and 13 for broken zones; the small difference from exactly half is
8-bit pixel rounding. No write requests or panel overflow occurred.
