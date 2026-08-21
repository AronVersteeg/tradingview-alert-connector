# Tradingview-Alert-Connector

Tradingview-Alert-Connector is a free and noncustodial tool for you to Integrate tradingView alert and execute automated trading for perpetual futures DEXes.

Currently supports [dYdX v3](https://dydx.exchange), [dYdX v4](https://dydx.trade/?ref=LawfulBalletF7U), [Perpetual Protocol v2](https://perp.com/), [GMX v2](https://app.gmx.io/#/trade/) and [Bluefin](https://trade.bluefin.io).

# Docs

https://tv-connector.gitbook.io/docs/

# Snoek AI tools route

This Render app can also host small non-trading tools under separate routes. The first one is Snoek AI Scout:

```text
GET /snoek
GET /snoek/api/scout
POST /snoek/api/scout
GET /snoek/api/weather
GET /snoek/api/structures
GET /snoek/api/current
```

`/snoek` serves a mobile-friendly fishing scout for Velsen/Spaarnwoude and nearby roofvis water. It scores simple weather inputs such as wind, cloud cover, pressure trend, rain, temperature and time of day, then returns a practical fishing recommendation, an offline map seed layer, source catalog and community-review seed data. It supports separate modes for snoek, snoekbaars/dropshot, method feeder and witvis.

`/snoek/api/weather?location=Velsen-Zuid` uses Open-Meteo geocoding and forecast data to fill live open weather inputs without an API key.

`/snoek/api/structures` loads the selected layers from all 18 official PDOK Waterschappen Kunstwerken IMWA layers, preserving their exact source names and geometries. Point features stay points and line features such as culverts, siphons and hevels stay lines. The API paginates complete layers and returns the argued Snoek scout spots separately from the raw PDOK control objects. Culverts remain a low-priority hotspot signal and only qualify near stronger flow or local-practice evidence.

The map is Kadaster-first: PDOK BRT and TOP10NL define the geometry around the current PDOK aerial-photo tiles. The optional Rijkswaterstaat `WNN_n_NAP` raster adds official 1 m bottom-elevation classes for IJmuiden, the Noordzeekanaal and covered side channels. Water and background fills stay below that raster, while TOP10NL roads, railways, buildings and Kadaster labels stay above it. The raster is requested as EPSG:3857 ArcGIS export tiles, with high-DPI tiles where supported, so it stays aligned and sharp while panning and zooming. Values are bottom elevations in metres relative to NAP, not live water depths. The default extent runs from the IJmuiden locks and Oud Velsen through Spaarnwoude to Westzaan and Haarlem-Noord. AHN object extraction and live community imports can be embedded later without changing the Render route.

`/snoek/api/current` combines fresh Rijkswaterstaat WaterWebservices speed and bearing observations into measured current vectors. Only accepted Waterinfo quality codes and observations no older than 90 minutes are returned. The map scales arrow length and width with current speed, rotates it to the measured bearing and never interpolates a field between sensors.

Current manual roofvis advice is seeded as local practice input: Zijkanaal C, the A9 bridge, Sluis Spaarndam, Pontje Velsen-Zuid, the steiger toward Oud Velsen and Pontje Buitenhuizen. For snoekbaars the tactics favor dropshot or small shads around pontstroming, sluices, kades, talud and low-light windows.

The Snoek map supports native pan/zoom, clickable scout and local-practice spots, a detail panel with coordinates and reasoning, and layer toggles for Kadaster BRT/TOP10NL, the PDOK aerial-photo coloring and major GIS spot classes such as pumping stations, weirs, locks and bridges.

Example API call:

```json
{
  "target": "snoek",
  "temperatureC": 16,
  "windBft": 3,
  "cloudCoverPct": 85,
  "pressureTrend": "falling",
  "rain": "light",
  "timeOfDay": "evening"
}
```

The route is intentionally isolated from TradingView and Decentrader routes, so it can be hosted on the same Render service without touching the trading flow.

# Decentrader BTC liquidity gap monitor

This connector can also run a Decentrader BTC liquidity-gap monitor in the background. It checks the latest hourly Decentrader liquidity map and sends an SMTP email when new active histogram zones appear inside the previous clean gap around price.

Enable it on Render with environment variables:

```text
DECENTRADER_GAP_MONITOR_ENABLED=true
DECENTRADER_GAP_SYMBOL=btcusdt
DECENTRADER_GAP_POLL_MINUTES=10
DECENTRADER_GAP_ALERT_JOB_NAME=Decentrader BTC gap monitor
```

The monitor uses the same SMTP variables:

```text
SMTP_HOST=
SMTP_PORT=587
SMTP_USERNAME=
SMTP_PASSWORD=
SMTP_FROM=
SMTP_TO=
SMTP_USE_TLS=true
SMTP_USE_SSL=false
```

Scanner-v3 aliases also work: `SMTP_USER`, `SMTP_APP_PASSWORD`, `NOTIFY_FROM`, and `NOTIFY_EMAIL`.

Status:

```text
GET /decentrader/gap-status
```

When auto-trading is enabled, TP prices come from the latest qualifying liquidity-map zones. TP1 front-runs the opposite gap edge; TP2+ is selected from active Decentrader histogram clusters by historical liquidity peak strength, overlap across leverage bands, 10x participation, freshness, and CoinGlass orderbook confluence before the final levels are ordered along the trade path. The selector does not use fixed max-distance staging for TP1/TP2/TP3; it applies a minimum spacing between selected analytical zones so nearby ladder noise does not crowd out stronger historical/CG levels. While a BTC position opened by this monitor is active, every monitor poll can replace only the dYdX take-profit ladder when the map changes. Unknown/manual positions, position size, direction, and stop orders are not changed by this TP-only sync.

```text
DECENTRADER_TRADE_RISK_PCT=0.0075
DECENTRADER_TRADE_RISK_USD=
DECENTRADER_TRADE_RISK_USD_CAP_BY_PCT=false
DECENTRADER_TP_MAX_LEVELS=6
DECENTRADER_TP1_EDGE_FRONT_RUN_USD=50
DECENTRADER_TP1_EDGE_FRONT_RUN_PCT=0.0005
DECENTRADER_TP_MIN_SPACING_PCT=0.025
DECENTRADER_TP_BEYOND_EDGE_ONLY=true
DECENTRADER_TP_SIZE_FRACTIONS=
DECENTRADER_DYNAMIC_TP_ENABLED=true
DECENTRADER_DYNAMIC_SL_ENABLED=true
DECENTRADER_DYNAMIC_SL_MIN_IMPROVEMENT_PCT=0.0025
DECENTRADER_INTRUSION_CANDLE_FILTER_ENABLED=false
DECENTRADER_REGULAR_INTRUSION_EMAIL_ENABLED=false
DECENTRADER_INTRUSION_CANDLE_SOURCE=binance-futures
DECENTRADER_INTRUSION_BINANCE_SYMBOL=BTCUSDT
DECENTRADER_INTRUSION_VOLUME_DELTA_ENABLED=true
```

Set `DECENTRADER_TRADE_RISK_USD` to target a fixed dollar risk per trade, such as `2`. With `DECENTRADER_TRADE_RISK_USD_CAP_BY_PCT=false`, that dollar value is leading. Set `DECENTRADER_TRADE_RISK_USD_CAP_BY_PCT=true` if you also want the fixed dollar value capped by `DECENTRADER_TRADE_RISK_PCT` of live equity. Leave `DECENTRADER_TRADE_RISK_USD` empty to use pure equity-percentage risk.

Leave `DECENTRADER_TP_SIZE_FRACTIONS` empty for map/peak-weighted allocation. TP1 front-runs the opposite gap edge by the larger of `DECENTRADER_TP1_EDGE_FRONT_RUN_USD` and `DECENTRADER_TP1_EDGE_FRONT_RUN_PCT`; TP2+ prefers continuation clusters beyond that edge when `DECENTRADER_TP_BEYOND_EDGE_ONLY=true` and keeps at least `DECENTRADER_TP_MIN_SPACING_PCT` spacing from already selected zones unless a nearby CoinGlass confluence justifies tighter grouping. The actual number of TP orders is limited by the remaining position size and the dYdX market minimum.

The dynamic SL is a confirmed-fractal ratchet for positions opened by this monitor. For LONG positions it only moves upward; for SHORT positions it only moves downward. After a newer trailing stop is submitted, older visible/Render-managed stops are cancelled best-effort. If dYdX conditional order visibility is incomplete, the bot keeps protection conservative and logs what it could verify.

Set `WEBHOOK_TOKEN` for the root TradingView-compatible webhook and `DECENTRADER_API_TOKEN` for mutating monitor endpoints such as manual checks and simulations. These are connector authentication secrets; they are unrelated to a dYdX API passphrase. Mutating endpoints reject requests when no secret is configured. Set `DYDX_V4_MANAGED_ORDERS_STATE_FILE` to a Render persistent-disk path so managed SL and TP client IDs survive deploys.

Set `DECENTRADER_INTRUSION_CANDLE_FILTER_ENABLED=true` to gate BTC, ETH, INJ and Gold auto-trades behind an all-closed-candles confirmation. Reviews use matching Binance Futures 1H candles and, with `DECENTRADER_INTRUSION_VOLUME_DELTA_ENABLED=true`, every reviewed candle must also have same-direction Binance taker quote-volume delta (taker-buy quote volume minus taker-sell quote volume). With the default `DECENTRADER_REGULAR_INTRUSION_EMAIL_ENABLED=false`, raw intrusion emails are suppressed and their would-be send time fixes the internal end of The Delay window; only a passing asset-labelled `FILTERED ...` email is delivered before the entry flow. Set it to `true` to restore both raw and filtered mails. A right-edge intrusion requires every fully closed candle before that cutoff, plus every corresponding delta, to be red; a left-edge intrusion requires all of them to be green. At least one fully closed candle is required, an open candle at the cutoff is excluded, and one mismatching candle or delta fails the filter. Missing authoritative candle or delta data keeps the alert in a pending review queue; it does not silently fall back to another feed. The Binance reader paginates from the oldest available map frame and caches the result, so replay/backtest coverage is no longer limited to the latest 1,000 hours and includes dates before 2 June when present in the map. The filtered mail and The Delay table expose the reviewed sequences for verification. Existing stored Delay/DOM history is retained as-is. When a delayed review passes, the historical frame proves the signal only: entry, gap, fractal SL, TP ladder and sizing are rebuilt from the current map and dYdX state. `DECENTRADER_INTRUSION_MAX_EXECUTION_DELAY_HOURS` and `DECENTRADER_INTRUSION_MAX_PRICE_DRIFT_PCT` bound The Delay before a live order can be placed. Leave the filter `false` to keep the original immediate intrusion-alert setup. The map also has a local `Candle on/off` toggle for replay comparison.

With `DECENTRADER_DELAY_CG_REVIEW_ENABLED=true`, The Delay also snapshots CoinGlass whale/orderbook levels of at least `DECENTRADER_DELAY_CG_MIN_USD` inside the gap. The execution log compares first-observed and pre-entry levels, then classifies persistent/new/removed levels as directional support, forward friction or other. This is logged as entry context and does not hard-block a trade by itself because visible whale orders can move or disappear.

TP2+ candidates also carry an observe-only gap Fibonacci confluence marker for the objective extensions `1.272`, `1.618`, `2.000`, and `2.618`. The anchor is always the clean gap width, and only existing Decentrader TP zones outside the opposite edge can match. Fibonacci does not create, rank, move, or place a TP. The TP backtest reports hit rates for TP2+ zones with and without Fibonacci overlap so its incremental value can be evaluated before it is allowed to affect execution.

The map can also show a CoinGlass large-orderbook/whale overlay. The monitor reads the public CoinGlass page feed, keeps levels above the configured dollar threshold, and draws them as vertical `CG` lines on the Decentrader map. Materially changed snapshots are stored with their actual observation time, map-frame timestamp, current price and gap edges. Replay therefore shows only levels known at that point, plus an influence table for inside/below/above-gap volume, additions/removals and later price tests. Entries and stops remain Decentrader/fractal based; TP2+ selection can use CoinGlass as a bounded confluence boost when a nearby same-side whale level has enough volume and especially when its duration is above the configured long-duration threshold. Set `COINGLASS_WHALE_HISTORY_FILE` to a Render persistent-disk path, or place `DECENTRADER_GAP_ALERT_STATE_FILE` on that disk so the CoinGlass history follows it across deploys.

The read-only decentralized DOM research collector records public BTC-USD L2 and trade-flow observations from dYdX and Hyperliquid. It requires no API key, account or trading permission and is not imported by the entry, sizing, SL or TP execution path. Public trade WebSockets capture aggressive flow while periodic L2 snapshots are aggregated into append-only one-minute NDJSON records. Stored fields include depth at fixed basis-point bands, visible adds/removals inside a comparable 25-bps band, book imbalance ranges, microprice, taker volume and cross-venue consensus. The map panel follows live time or the selected replay frame. Put `DECENTRALIZED_DOM_HISTORY_DIR` on the Render persistent disk; `/app/data/decentralized-dom` matches a disk mounted at `/app/data`. Public HTTP and WebSocket gateway URLs are environment-overridable, so a community or self-hosted endpoint can replace a blocked gateway without changing the storage schema.

```text
DECENTRALIZED_DOM_COLLECTOR_ENABLED=true
DECENTRALIZED_DOM_POLL_SECONDS=15
DECENTRALIZED_DOM_BUCKET_SECONDS=60
DECENTRALIZED_DOM_RETENTION_DAYS=120
DECENTRALIZED_DOM_HISTORY_DIR=/app/data/decentralized-dom
```

Raw status and replay-window data are available from `GET /research/dom-collector/status` and `GET /research/dom-collector/history?from=<ISO>&to=<ISO>`.

The pair tabs expose separate reconstructions of the Decentrader-style histogram for BTC/USD, ETH/USD, INJ/USD, Gold and Silver without stacking maps vertically. BTC retains a compact Decentrader/Public V2 source selector. The crypto V2 maps use free Binance Spot `BTCUSDT`, `ETHUSDT` and `INJUSDT` 1H OHLC data; Gold and Silver use Binance Futures `XAUUSDT` and `XAGUSDT`. Every closed hour creates six 3x, 5x and 10x long/short cohorts from OHLC4 with the reconstructed fixed multipliers, rounded to $100 for BTC, $5 for ETH and Gold, $0.01 for INJ and $0.10 for Silver. Later candle lows/highs remove crossed cohorts and only the latest 8,760 birth hours remain active. Histogram height is therefore a relative count of still-active hourly cohorts, not USD volume, open interest or exact account inventory. Replay is causal and never uses later candles to alter an earlier frame. The gap is the empty corridor between the nearest active rounded levels below and above price. Every V2 market has a separate persistent history store.

BTC V2 remains observe-only. ETH, INJ, Gold and Silver V2 monitor new or expanded cohorts inside the previous clean gap, send asset-labelled `FILTERED ETH`/`FILTERED INJ`/`FILTERED GOLD`/`FILTERED SILVER` emails, and can independently manage `ETH-USD`, `INJ-USD`, `PAXG-USD` and `XAG-USD` dYdX positions alongside BTC. Gold uses Binance Futures `XAUUSDT` for the causal map and Delay candle/delta filter, with `PAXGUSDT` confirmation and dYdX `PAXG-USD` execution. Silver uses Binance Futures `XAGUSDT` with dYdX `XAG-USD` execution. The tradable pair monitors inherit the existing `DECENTRADER_*` Delay-filter, fixed USD/equity risk, Williams-fractal entry SL, dynamic trailing SL and dynamic TP settings. Their Delay confirmation matches BTC: authoritative Binance USD-M Futures 1H candle colors and signed taker quote-volume delta must agree for every fully closed candle in the SMTP window. TP1 uses the opposite gap edge; TP2+ uses the matching histogram with available CoinGlass and Fibonacci confluence, with at most `DECENTRADER_TP_MAX_LEVELS` targets. Each monitor stores separate signatures and managed-position state, so pairs cannot suppress one another. INJ and Gold live execution are explicitly opt-in; Silver inherits the shared BTC auto-trade switch and has a dedicated override.

```text
OPEN_LIQUIDITY_V2_ENABLED=true
OPEN_LIQUIDITY_V2_ETH_ENABLED=true
OPEN_LIQUIDITY_V2_INJ_ENABLED=true
OPEN_LIQUIDITY_V2_GOLD_ENABLED=true
OPEN_LIQUIDITY_V2_SILVER_ENABLED=true
OPEN_LIQUIDITY_V2_POLL_MINUTES=60
OPEN_LIQUIDITY_V2_HISTORY_DIR=/app/data/open-liquidity-v2
OPEN_LIQUIDITY_V2_ETH_HISTORY_DIR=/app/data/open-liquidity-v2-eth
OPEN_LIQUIDITY_V2_INJ_HISTORY_DIR=/app/data/open-liquidity-v2-inj
OPEN_LIQUIDITY_V2_GOLD_HISTORY_DIR=/app/data/open-liquidity-v2-gold
OPEN_LIQUIDITY_V2_SILVER_HISTORY_DIR=/app/data/open-liquidity-v2-silver
# Optional ETH-specific kill switches; when omitted, monitoring is on and
# auto-trading inherits DECENTRADER_AUTO_TRADE_ENABLED.
OPEN_LIQUIDITY_V2_ETH_INTRUSION_MONITOR_ENABLED=true
OPEN_LIQUIDITY_V2_ETH_AUTO_TRADE_ENABLED=true
OPEN_LIQUIDITY_V2_ETH_TRADE_STATE_FILE=/app/data/open-liquidity-v2-eth-trade-state.json
OPEN_LIQUIDITY_V2_INJ_INTRUSION_MONITOR_ENABLED=true
# Explicit live-order opt-in for the new market.
OPEN_LIQUIDITY_V2_INJ_AUTO_TRADE_ENABLED=false
OPEN_LIQUIDITY_V2_INJ_TRADE_STATE_FILE=/app/data/open-liquidity-v2-inj-trade-state.json
OPEN_LIQUIDITY_V2_GOLD_INTRUSION_MONITOR_ENABLED=true
# Explicit live-order opt-in for dYdX PAXG-USD.
OPEN_LIQUIDITY_V2_GOLD_AUTO_TRADE_ENABLED=false
OPEN_LIQUIDITY_V2_GOLD_TRADE_STATE_FILE=/app/data/open-liquidity-v2-gold-intrusion-state.json
OPEN_LIQUIDITY_V2_SILVER_INTRUSION_MONITOR_ENABLED=true
# When omitted, Silver inherits DECENTRADER_AUTO_TRADE_ENABLED. Set false for
# an independent Silver kill switch without stopping the other markets.
OPEN_LIQUIDITY_V2_SILVER_AUTO_TRADE_ENABLED=true
OPEN_LIQUIDITY_V2_SILVER_TRADE_STATE_FILE=/app/data/open-liquidity-v2-silver-intrusion-state.json
OPEN_LIQUIDITY_V2_SILVER_TP1_EDGE_FRONT_RUN_USD=0.10
```

The old V2 price-step, minimum-cluster and gap-cleanliness variables are no longer used. The V2 payload and collector status accept `market=BTC-USD`, `ETH-USD`, `INJ-USD`, `PAXG-USD` or `XAG-USD`. All collectors infer separate persistent directories from `DECENTRALIZED_DOM_HISTORY_DIR`, so explicit paths are optional when the existing Render disk is mounted at `/app/data`.

```text
COINGLASS_WHALE_LEVELS_ENABLED=true
COINGLASS_WHALE_SYMBOL=Binance_BTCUSDT
COINGLASS_WHALE_INTERVAL=m1
COINGLASS_WHALE_LEVEL_MIN_USD=10000000
COINGLASS_WHALE_LEVEL_STRONG_USD=20000000
COINGLASS_WHALE_POLL_MINUTES=10
COINGLASS_WHALE_HISTORY_RETENTION_HOURS=720
COINGLASS_WHALE_HISTORY_MAX_RECORDS=1500
COINGLASS_WHALE_OBSERVATION_MAX_RECORDS=1000
COINGLASS_WHALE_HISTORY_FILE=/var/data/coinglass-whale-history.json
COINGLASS_WHALE_ETH_ENABLED=true
COINGLASS_WHALE_ETH_SYMBOL=Binance_ETHUSDT
COINGLASS_WHALE_ETH_LEVEL_MIN_USD=10000000
COINGLASS_WHALE_ETH_LEVEL_STRONG_USD=20000000
COINGLASS_WHALE_ETH_HISTORY_FILE=/app/data/coinglass-whale-history-eth.json
COINGLASS_WHALE_INJ_ENABLED=true
COINGLASS_WHALE_INJ_SYMBOL=Binance_INJUSDT
COINGLASS_WHALE_INJ_LEVEL_MIN_USD=250000
COINGLASS_WHALE_INJ_LEVEL_STRONG_USD=1000000
COINGLASS_WHALE_INJ_HISTORY_FILE=/app/data/coinglass-whale-history-inj.json
COINGLASS_TP_CONFLUENCE_ENABLED=true
COINGLASS_TP_CONFLUENCE_MIN_USD=10000000
COINGLASS_TP_CONFLUENCE_MAX_DISTANCE_USD=200
COINGLASS_TP_CONFLUENCE_ETH_MAX_DISTANCE_USD=15
COINGLASS_TP_CONFLUENCE_INJ_MAX_DISTANCE_USD=0.05
COINGLASS_TP_CONFLUENCE_LONG_DURATION_HOURS=336
```

The ETH/INJ-specific variables are optional. Omitted history paths become `-eth` and `-inj` siblings beside `COINGLASS_WHALE_HISTORY_FILE`, keeping all assets on the same persistent disk without sharing records. The INJ public CoinGlass stream can be intermittent; a timeout preserves the last cached snapshot and never blocks alerts, sizing or protective orders.

The map also includes an experimental dYdX RSI study layer for gap intrusions. It fetches 4H and 1D BTC-USD candles from the dYdX indexer, calculates RSI locally, and annotates replay frames when RSI14 is near the configured 50-zone or freshly crosses 50. The master scanner is Daily-only: when Daily RSI enters the configured master zone, the first configured number of future gap-intrusion histogram bars are armed as fertile. Those fertile slots stay armed even after Daily RSI leaves the zone; leaving the zone only sends a "Master RSI zone deactivated" notification for the RSI-zone state. Every master-scanner email includes the current state and next action so the zone state cannot be confused with the armed fertile scanner state. After the configured number of fertile histos has been used, or when price touches the armed clean-gap edge/TP1 edge first, the scanner is disarmed until Daily RSI touches the master zone again. Set `DECENTRADER_MASTER_RSI_SCANNER_ENABLED=false` to disable and reset only the Daily master scanner and its emails while retaining the RSI study data and normal intrusion flow.

```text
DECENTRADER_RSI_STUDY_ENABLED=true
DECENTRADER_RSI_MARKET=BTC-USD
DECENTRADER_RSI_PERIOD=14
DECENTRADER_RSI_ZONE_LOW=45
DECENTRADER_RSI_ZONE_HIGH=55
DECENTRADER_MASTER_RSI_SCANNER_ENABLED=true
DECENTRADER_MASTER_RSI_ZONE_LOW=48
DECENTRADER_MASTER_RSI_ZONE_HIGH=52
DECENTRADER_MASTER_RSI_MAX_INTRUSIONS=3
DECENTRADER_RSI_STUDY_CACHE_SECONDS=600
```

Manual check:

```text
POST /decentrader/gap-check
```

Safe edge simulation using live map, equity, SL, sizing and TP logic without placing an order:

```text
POST /decentrader/simulate-edge
{"edge":"left","market":"BTC-USD"}
```

Use `left`/`long` for a simulated LONG signal or `right`/`short` for a simulated SHORT signal.

If `TRADINGVIEW_PASSPHRASE` is configured, the manual check and simulation require that same value as `X-Webhook-Token`, `passphrase`, or `token`.

An explicit live end-to-end test can place a temporary `0.001 BTC` position with a temporary 1% SL/TP and automatically flatten it after 5-60 seconds. This test deliberately overrides a normal map-plan SL/size skip, but still refuses to start when a BTC position is already open. It requires `DECENTRADER_LIVE_TEST_TOKEN` and the exact confirmation `PLACE_AND_FLAT`:

```text
POST /decentrader/live-test-edge
X-Decentrader-Live-Test-Token: <DECENTRADER_LIVE_TEST_TOKEN>
{"edge":"right","holdSeconds":20,"confirm":"PLACE_AND_FLAT"}
```

For duplicate-alert protection across Render restarts, use a persistent path for:

```text
DECENTRADER_GAP_ALERT_STATE_FILE=/var/data/decentrader-gap-alert-state.json
```

# Video Tutorial

dYdX v3:
https://www.youtube.com/watch?v=I8hB2O2-xx4

Perpetual Protocol:
https://youtu.be/YqrOZW_mnUM

# Prerequisites

- TradingView Account at least Pro plan

https://www.tradingview.com/gopro/

- DEX(e.g. dYdX v4) account with collateral already in place

# Installation

```bash
git clone https://github.com/junta/tradingview-alert-connector.git
cd tradingview-alert-connector
npm install --force
```

# Quick Start

- rename .env.sample to .env
- fill environment variables in .env (see [full tutorial](https://tv-connector.gitbook.io/docs/setuup/running-on-local-pc#steps))

### with Docker

```bash
docker-compose build
docker-compose up -d
```

### without Docker

```bash
yarn start
```

## Disclaimer

This project is hosted under an MIT OpenSource License. This tool does not guarantee users’ future profit and users have to use this tool on their own responsibility.
