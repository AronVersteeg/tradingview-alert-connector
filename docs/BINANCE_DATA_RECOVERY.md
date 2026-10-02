# Binance data recovery

The 2026-10-02 incident was an outbound-IP Binance Futures REST ban (HTTP 418,
code -1003), not a dYdX order failure. Render's shared outbound IP may include
traffic outside this process. Local throttling alone cannot guarantee that the
IP will never be banned.

## Protection

- Public REST requests are serialized per host, with a default 750 ms spacing.
- Estimated endpoint weights and Binance's `x-mbx-used-weight-1m` header both
  limit usage. Default safety budget: 1200 weight/minute, plus 500 requests/5 min.
- Identical candle/OI reads share an in-flight request and a bounded 2-second
  cache. Depth/orderbook reads are never reused by this transport.
- HTTP 418/429 cooldowns honor the longest Retry-After or banned-until value.
  Cooldowns persist in `binance-rest-cooldowns.json`, alongside the configured
  DOM history directory (otherwise `data/`). Keep that directory on the existing
  persistent disk. Separate processes still need shared rate-limit coordination.
- After a cooldown, requests wait two additional seconds and use at least
  three-second spacing for the first minute, preventing a queued catch-up burst.
- Daily/Weekly endpoints return HTTP 503 with `Retry-After` and `retryAt` when
  no snapshot is available during a known cooldown. Cached failures retain their
  typed cooldown error, so dashboard polling does not emit repeated stack traces.
  Unexpected errors still log normally; stale snapshots remain explicitly marked.
- Confirmed Daily/Weekly fractals remain cached until a new UTC closed candle
  is expected; failed refreshes expose `stale`, `lastError`, and `retryAt`.
- Manual candle checks retry after data failure; armed plans are not cancelled.
  Only the latest closed hour can trigger. The existing 15-minute entry freshness
  limit remains: missed old signals are never replayed as delayed trades.
- One reconnecting Binance Futures WebSocket collects confirmed 1H closes for
  all seven pairs. Manual entries prefer its latest closed candle, using REST
  if that exact hour is unavailable. This preserves the same Futures source,
  price, and close-based rule. Only final (`x: true`) candles are accepted.
  `binance-hourly-closes.json` stores the latest close per symbol across restarts.
  A new stream cannot recover a missed past close without REST; it must wait for
  the next confirmed close during a REST ban. No Spot/dYdX price substitution.
- Shadow still requires REST 1H history and Daily fractals; unavailable or stale
  data defers entry. dYdX SL/TP/trailing calculations are unchanged and do not
  use this Binance REST transport.

## Diagnosis

Read `GET /research/binance/status` for host bans, observed IP weight, local
request count, the latest in-process rate-limit response (HTTP/code, triggering
endpoint/symbol and counters), recovery pacing, and per-symbol WebSocket close
freshness. Rate-limit diagnostics reset on restart; cooldowns persist. This endpoint does not
refresh data or place orders. Ban logs now include endpoint/symbol and counters.
CoinGlass timeouts separately report whether connect/upgrade or snapshot delivery
failed, with received frame/message counts; they are not fixed by Binance changes.

No new environment variables are required. Existing optional tuning:
`BINANCE_REQUEST_MIN_INTERVAL_MS`. New optional tuning:
`BINANCE_REQUEST_WEIGHT_BUDGET_PER_MINUTE` (default 1200, clamped 50..2000).
Do not shorten cooldowns, rotate IPs to evade bans, or run duplicate collectors
as a recovery action. An active external ban cannot be lifted by deployment.

References:
- https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info
- https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market
- https://render.com/docs/outbound-ip-addresses
