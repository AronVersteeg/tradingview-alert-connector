# Tradingview-Alert-Connector

Tradingview-Alert-Connector is a free and noncustodial tool for you to Integrate tradingView alert and execute automated trading for perpetual futures DEXes.

Currently supports [dYdX v3](https://dydx.exchange), [dYdX v4](https://dydx.trade/?ref=LawfulBalletF7U), [Perpetual Protocol v2](https://perp.com/), [GMX v2](https://app.gmx.io/#/trade/) and [Bluefin](https://trade.bluefin.io).

# Docs

https://tv-connector.gitbook.io/docs/

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
```

Set `DECENTRADER_TRADE_RISK_USD` to target a fixed dollar risk per trade, such as `2`. With `DECENTRADER_TRADE_RISK_USD_CAP_BY_PCT=false`, that dollar value is leading. Set `DECENTRADER_TRADE_RISK_USD_CAP_BY_PCT=true` if you also want the fixed dollar value capped by `DECENTRADER_TRADE_RISK_PCT` of live equity. Leave `DECENTRADER_TRADE_RISK_USD` empty to use pure equity-percentage risk.

Leave `DECENTRADER_TP_SIZE_FRACTIONS` empty for map/peak-weighted allocation. TP1 front-runs the opposite gap edge by the larger of `DECENTRADER_TP1_EDGE_FRONT_RUN_USD` and `DECENTRADER_TP1_EDGE_FRONT_RUN_PCT`; TP2+ prefers continuation clusters beyond that edge when `DECENTRADER_TP_BEYOND_EDGE_ONLY=true` and keeps at least `DECENTRADER_TP_MIN_SPACING_PCT` spacing from already selected zones unless a nearby CoinGlass confluence justifies tighter grouping. The actual number of TP orders is limited by the remaining position size and the dYdX market minimum.

The dynamic SL is a confirmed-fractal ratchet for positions opened by this monitor. For LONG positions it only moves upward; for SHORT positions it only moves downward. After a newer trailing stop is submitted, older visible/Render-managed stops are cancelled best-effort. If dYdX conditional order visibility is incomplete, the bot keeps protection conservative and logs what it could verify.

The map can also show a CoinGlass large-orderbook/whale overlay. The monitor reads the public CoinGlass page feed, keeps levels above the configured dollar threshold, and draws them as vertical `CG` lines on the Decentrader map. Successful snapshots are also stored as a forward-looking timelapse history so replay can show which CG levels were active during each map frame. Entries and stops remain Decentrader/fractal based; TP2+ selection can use CoinGlass as a bounded confluence boost when a nearby same-side whale level has enough volume and especially when its duration is above the configured long-duration threshold.

```text
COINGLASS_WHALE_LEVELS_ENABLED=true
COINGLASS_WHALE_SYMBOL=Binance_BTCUSDT
COINGLASS_WHALE_INTERVAL=m1
COINGLASS_WHALE_LEVEL_MIN_USD=10000000
COINGLASS_WHALE_LEVEL_STRONG_USD=20000000
COINGLASS_WHALE_POLL_MINUTES=10
COINGLASS_WHALE_HISTORY_RETENTION_HOURS=720
COINGLASS_WHALE_HISTORY_MAX_RECORDS=1500
COINGLASS_TP_CONFLUENCE_ENABLED=true
COINGLASS_TP_CONFLUENCE_MIN_USD=10000000
COINGLASS_TP_CONFLUENCE_MAX_DISTANCE_USD=200
COINGLASS_TP_CONFLUENCE_LONG_DURATION_HOURS=336
```

The map also includes an experimental dYdX RSI study layer for gap intrusions. It fetches 4H and 1D BTC-USD candles from the dYdX indexer, calculates RSI locally, and annotates replay frames when RSI14 is near the configured 50-zone or freshly crosses 50. The master scanner is Daily-only: when Daily RSI enters the configured master zone, the first configured number of future gap-intrusion histogram bars are armed as fertile. Those fertile slots stay armed even after Daily RSI leaves the zone; leaving the zone only sends a "Master RSI zone deactivated" notification for the RSI-zone state. Every master-scanner email includes the current state and next action so the zone state cannot be confused with the armed fertile scanner state. After the configured number of fertile histos has been used, or when price touches the armed clean-gap edge/TP1 edge first, the scanner is disarmed until Daily RSI touches the master zone again. This is visual research only until promoted into a trade filter.

```text
DECENTRADER_RSI_STUDY_ENABLED=true
DECENTRADER_RSI_MARKET=BTC-USD
DECENTRADER_RSI_PERIOD=14
DECENTRADER_RSI_ZONE_LOW=45
DECENTRADER_RSI_ZONE_HIGH=55
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
