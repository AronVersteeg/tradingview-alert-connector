# Intrusion + Daily Break: Final 5m Futures Delta

BTC Public V2 and ETH Spot display an `F5m` label above their purple intrusion +
Daily-break markers. The existing purple overlay checkbox controls both lines
and labels. No environment variable is required.

## Source and Candle Identity

- The chart and signal logic remain Binance Spot BTCUSDT / ETHUSDT.
- Labels use the matching Binance USD-M Futures BTCUSDT / ETHUSDT market,
  through `/fapi/v1/klines`, interval `5m`.
- For a signal candle opening at 11:00 UTC and closing at 12:00 UTC, the
  measurement is exactly 11:55:00 through 11:59:59.999 UTC. It is not the
  next hour, SMTP time, or a later Delay candle.
- Only fully closed candles are eligible. The endpoint validates timestamps,
  matching market metadata, volumes and derived values.

Raw delta percentage is `(2 * takerBuyBaseVolume / totalBaseVolume - 1) * 100`.
For long signals this is displayed unchanged; for short signals the sign is
reversed. Thus `F5m +30.44%` means aggressive flow aligned with the signal,
including net aggressive selling for a short. It is a volume imbalance, not a
price return or a probability of success. Base volume is BTC or ETH respectively.

`F5m ...` means pending, `F5m n/a` means unavailable, and `F5m +0.00%` means a
real measured near-zero imbalance. Missing/zero-volume candles and failed
requests never become measured zero. Labels pack into separate lanes; at dense
zoom, labels without space are omitted until zoomed in. Signal lines remain.
Labels are included in chart PNG exports.

## Read and Storage Bounds

The chart requests at most eight signal hours at a time, newest first, only
while visible and the overlay is enabled. Historical labels fill progressively.
There are no startup fetches or background collectors in the delta feed itself.
The ETH gap/Daily entry model also reads the same ETH cache when evaluating a
fresh candidate, independently from the browser; see `ETH_GAP_DAILY_ENTRY.md`.
At most one new annotation read across BTC and ETH enters the existing shared
Binance host queue at a time. Existing throttling and host cooldowns apply.
Failures back off; just-closed candles can retry after 15 seconds, older failures
after five minutes, or later if the host cooldown requires it.

Each market has a separate lazy, bounded 5,000-record persistent cache:
`btc-intrusion-futures-delta.json` / `eth-intrusion-futures-delta.json`. Files live
beside `DECENTRALIZED_DOM_HISTORY_DIR` when configured, otherwise in `data/`.
Writes are atomic. Invalid or mixed-market cache files are preserved and further
disk writes disabled rather than overwriting the evidence; in-memory reads can
continue. Responses expose `persistenceAvailable`.

## Scope and Interpretation

The labels themselves do not execute orders. BTC remains observe-only; the ETH
gap/Daily experiment now additionally requires this same directional measurement
to be strictly > 0. There is no 25% cutoff. No notification, stop-loss, trailing
or TP behavior is changed by the delta check. The BTC exploratory
finding is not proof that the same feature predicts ETH outcomes. Historical
measurements retain their fetch timestamp; reconstruction does not prove that
the historical intrusion was available at candle-close, and upstream historical
data can be revised.

## Verification

`test/intrusionFuturesDelta.test.ts` covers candle identity, normalization,
missing data, source isolation, request validation, bounded admission,
persistence/restart and failure recovery. The existing
`scripts/check-btc-gap-weekly-breakouts.cjs` tests both BTC and ETH on desktop and
mobile with fixture-backed APIs, label pixels, checkbox behavior and PNG export.
The fixtures do not place trades or send mail.
