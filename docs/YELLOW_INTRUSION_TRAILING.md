# Yellow Intrusion Trailing

## Entries Stay Separate

- BTC `BTC_ENTRY_MODE=purple` (`purple-trail` remains a compatibility alias): same closed Binance Spot BTCUSDT 1H candle
  has a directional intrusion, a fresh gap-box close break and a fresh confirmed
  Daily-level close break. The historical setup id remains `purple-green-trail`.
- ETH, SOL and ZEC: Green remains the legacy automatic entry choice. Each now
  accepts `<PAIR>_ENTRY_MODE=purple|green|manual|off`. Green needs an intrusion
  and same-hour fresh gap-box close break; Purple also needs a fresh Daily
  close break, using only that pair's Spot data. See [pair selectors](PAIR_ENTRY_MODES.md).
- Existing per-pair entry/auto-trading switches remain authoritative. This code
  change does not enable an explicitly disabled market or edit Render settings.

## Initial Stop

For new automatic entries, use the exact open of the closed dYdX 1H candle matching
the Binance signal hour, not the next candle or a wick. This is already the
body-open entry policy; it remains buffer-free, subject to exchange tick rounding.
The original confirmed dYdX fractal still determines sizing. Risk, collateral,
dynamic downsizing, entry freshness, ownership and duplicate protection do not
change. Automatic positions have no TP orders. Manual override management is not
changed by this policy.

## Later Stops

Each later closed-hour intrusion (Yellow) in the position's direction permits
one evaluation of the latest confirmed protective dYdX 1H fractal. Long uses a
low fractal; short uses a high fractal. Intrusion direction is the existing
left/right entrant-weight direction, not the subsequent candle color. Opposite
or directionless/mixed intrusions do not qualify. Neither a gap-box break nor a
Daily break is required for this trailing event.

The entry candle's own intrusion cannot immediately replace the initial stop.
A new fractal without an eligible Yellow does not move the stop. A candidate
must strictly improve the **current** stop and remain on the protective side of
the current oracle: higher for longs, lower for shorts. Equal/worse/invalid
candidates retain the old stop. At a subsequent eligible Yellow, the latest
already-confirmed fractal may qualify even when its pivot predates entry.

Only dYdX candles closed by the Yellow's close can confirm its fractal; later
candles cannot influence a retry. The selection reference is that dYdX hour's
close, then the actual candidate is checked against the live oracle. Existing
Williams confirmation/window, wick guard and fractal buffer remain in use.
There is no extra newer-pivot count or fractal-delay requirement in this policy.

## Persistence And Protection

`managedPosition.intrusionTrail` persists the entry close, scanned hour, accepted
intrusion close, evaluated intrusion close and any unverified candidate. A
pending exchange replacement retries the same frozen candidate. State advances
only on confirmed stop synchronization; stop coverage repair and breach handling
remain independent of Yellow detection and the entry switches.

Existing monitor-owned automatic positions adopt this trailing policy without
changing their current stop, initial size, TP policy or ownership. Legacy BTC
Green-gate/pending metadata is retired; the current exchange stop is retained,
and a later eligible Yellow is required. Already-submitted legacy intents retain
their recorded initial stop during recovery. No historical event replay opens
an entry or retroactively loosens/replaces a stop.

Fresh Yellow scanning uses the existing closed-hour Public V2 refresh and its
bounded five-minute retry window, without extra Spot, Daily or Futures reads.
Protective checks still run independently around hour-close +2 seconds. Without
an unevaluated Yellow they repair the existing stop without reading dYdX candles;
`trailingCandle.required=false` is not a claim of fresh candle coverage. With a
Yellow awaiting its matching dYdX hour, `trailingCandle.pending=true` requests
bounded retries. Source/exchange latency is still possible.

Inspect `execution.gapDailyEntry.initialStop.trailingGate` for
`later-same-direction-intrusion`, `trailingSignals: ["yellow"]`, and the managed
position's `intrusionTrail`. The chart checkbox controls rendering only; hiding
Yellows does not disable protective management. No new environment variable is
required.

Tests are offline with mocked exchange/SMTP and temporary position states. No
live/test order, real email or production setting change is needed.
