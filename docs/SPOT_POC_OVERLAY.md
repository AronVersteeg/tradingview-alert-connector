# Purple candle Spot POC crossbar

BTC Public V2 and ETH Spot use the existing purple intrusion/D checkbox. Each
completed profile adds a 16 CSS-pixel horizontal purple crossbar at that candle's
volume Point of Control. It is a chart annotation only, not an entry/SL/TP input.

## Definition and source

- Binance Spot `BTCUSDT` / `ETHUSDT`, the purple signal's exact UTC 1H window.
- `GET /api/v3/aggTrades`: sum executed base quantity at each exact traded price.
- POC is the price with the greatest base volume; ties choose the lower price.
- This is an exact-price POC, not an OHLC approximation or a configurable binned
  TradingView volume profile. Different price-bin widths may yield a different POC.
- Historical profiles use all trades in the closed hour, including its final
  millisecond. No Futures, dYdX, next-hour data or current-orderbook substitution.

## Completeness and load bounds

One independently fetched closed Spot 1H kline supplies the expected base volume
and raw trade count. Aggregated trade pages must have contiguous IDs and monotonic
timestamps. Fixed eight-decimal integer quantities reconcile exactly against BOTH
kline totals before any POC is published. Missing or inconsistent data is not a
zero/midpoint estimate: no crossbar is rendered.

The browser prioritizes visible purple candles, then loads other purple candles
newest first while the chart panel is active. It continues a partial profile even
when panning away before starting another; progress is shown beside the POC count.
This also warms the historical cache when the initial viewport has no purple
candles. Two bounded partial profiles can coexist (one per active chart); a shared
fair scheduler admits at most one 1000-trade page per five seconds via the
existing Binance host limiter, so a long BTC profile cannot starve ETH or vice versa.
The official Spot aggregate-trades weight of 4 is accounted for. Cooldowns are
respected before enqueueing work. High-volume historical hours may take minutes
to complete (the 21 September 2026 08:00 UTC BTC candle needs 152 trade pages).
Transport timeouts, temporary server errors and rate-limit failures retain the
validated partial profile and retry the same next aggregate ID after exponential
backoff (15 seconds to 5 minutes, or longer shared cooldown). Invalid trade data
still discards the profile and publishes no POC. Backoff does not expire a partial
profile before its retry becomes due. Hidden tabs and an unchecked purple overlay stop browser requests;
abandoned partial jobs expire after two minutes. There are no startup collectors,
archive ZIP downloads, bulk backfills, orders or emails.

Memory is bounded at 100,000 price levels across both profiles, with at most
2,000,000 aggregate trades per profile (only per-price sums remain in memory).
Completed-only results (up to 5000 per symbol) are atomically stored alongside the
DOM-history directory, in `btcusdt-intrusion-spot-poc.json` and
`ethusdt-intrusion-spot-poc.json`. Corrupt/mixed-symbol history is preserved and
writes disabled rather than replacing evidence. Interrupted partial profiles can
be fetched again; completed profiles are reused after restart.

Read-only endpoints:

`GET /open-liquidity/v2/{btc|eth}-intrusion-spot-poc?hour=<candle-open Unix seconds>`

Only one closed hour is accepted. `pending` / `unavailable` includes `retryAt`;
`ok` includes `poc.price`, exact volume strings and reconciliation counts.

Official source: https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints
