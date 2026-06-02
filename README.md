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

Manual check:

```text
POST /decentrader/gap-check
```

If `TRADINGVIEW_PASSPHRASE` is configured, the manual check requires that same value as `X-Webhook-Token`, `passphrase`, or `token`.

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
