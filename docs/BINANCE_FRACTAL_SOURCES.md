# Dashboard Daily and Weekly fractal sources

The Daily and Weekly history tables, including their intact-level summaries,
use these explicit sources:

| Dashboard market | Binance symbol | Venue |
| --- | --- | --- |
| BTC-USD | BTCUSDT | Spot |
| ETH-USD | ETHUSDT | Spot |
| INJ-USD | INJUSDT | Spot |
| SOL-USD | SOLUSDT | Spot |
| ZEC-USD | ZECUSDT | Spot |
| PAXG-USD (GOLD) | XAUUSDT | Futures |
| XAG-USD (SILVER) | XAGUSDT | Futures |

Binance exchangeInfo was checked on 2026-10-20: XAUUSDT and XAGUSDT are not
Spot markets. GOLD and SILVER therefore retain their existing Futures source,
explicitly identified in each table heading and API response. PAXGUSDT Spot
exists but is not silently substituted for XAUUSDT.

Spot uses `/api/v3/klines` on api.binance.com; Futures uses `/fapi/v1/klines`
on fapi.binance.com. Daily uses `1d`; Weekly uses `1w`. Both still require
closed candles and two right-hand Williams confirmation candles. Exact price
strings, first-later-break and intact calculations are unchanged; their
historical values are recomputed from the selected venue's candles.

Caches, retries and stale fallbacks are separated by venue, market and interval.
A Spot outage never silently falls back to Futures.

This change is limited to dashboard fractals. Manual close triggers, SL and
trailing remain unchanged. The existing shadow entry model explicitly retains
Futures for both its 1H and Daily fractals, so the dashboard switch cannot mix
Spot Daily levels with Futures hourly entry signals.
