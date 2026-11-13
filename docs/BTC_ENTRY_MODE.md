# BTC Public V2 Entry Mode

```dotenv
BTC_ENTRY_MODE=manual
```

This is the single authoritative BTC entry selector:

| Value | New BTC entries |
| --- | --- |
| `manual` | Saved manual plans only (default when absent). |
| `green` | Binance Spot Public V2 intrusion plus a fresh close beyond the previous gap edge on the same closed 1H candle. |
| `purple` | Legacy alias for `green`; the Purple overlay no longer opens trades. |
| `off` | No new BTC entries. Existing positions keep their original management. |

Values are case-insensitive. Unknown values fail closed as `off`; status reports
`entryMode.valid=false`. This is a mode, not a boolean. Deployment never selects
automatic entries when the mode is absent. Change the value in Render and choose **Save and deploy**.
The new value takes effect in the deployed process; it is not an instant
in-process switch.

## Sources And Execution

Public V2 is the main BTC trading map in every mode. Native BTC intrusion records
and the public Spot chart overlays use Binance **Spot BTCUSDT**, not
Decentrader or Futures. Intrusions are derived by our causal Public V2 cohort
model from Spot candles; they are not an official Binance indicator.

BTC manual triggers also read Binance Spot BTCUSDT closed 1H candles. They never
use the Futures close stream. Their planner is explicitly wired to the existing
BTC Public V2 collector. A missing Public V2 planner fails closed; it cannot
silently fetch Decentrader data. When manual TPs are omitted, new native manual
positions keep using Public V2 zones for subsequent automatic TP refreshes.

Execution remains on dYdX BTC-USD. Both manual and Green use the original
dYdX confirmed 1H fractal for sizing and the exact same-hour closed dYdX body for
the initial stop. Green stops at `open`; manual retains
`open + 0.25 * (close - open)`, both without a buffer. Fractal trailing
only tightens. Existing positions and already-submitted intents retain their
recorded stops. The existing fixed-USD risk, collateral, depth, step/tick and
dynamic-downsizing checks still apply.

Green uses NO_TP / trailing-only. There is no Daily, FD or OI entry gate. Only the latest
closed hour within five minutes and closing after activation is eligible. No
historical signal is dispatched when enabling Green or migrating from Purple.
Spot candles and replica OHLC must agree for both hours, and the close must
newly cross the gap edge already known at candle open. The market/model must
match. The chart's simulated positions are visual only: live entry checks
the actual dYdX position and persisted ownership.

The existing `DECENTRADER_AUTO_TRADE_ENABLED` remains the BTC execution master
kill switch. Manual also respects `MANUAL_ENTRY_TP_OVERRIDE_ENABLED`; Green
requires its monitor and protective execution to be enabled. Existing deployed
values need not change when toggling between modes. The old, previously unused
`OPEN_LIQUIDITY_V2_BTC_AUTO_TRADE_ENABLED` is not a second mode switch: explicit
`BTC_ENTRY_MODE=green` is the BTC opt-in. ETH, SOL and ZEC retain their existing
`OPEN_LIQUIDITY_V2_<PAIR>_GAP_DAILY_ENTRY_ENABLED` and
`OPEN_LIQUIDITY_V2_<PAIR>_AUTO_TRADE_ENABLED` flags, now selecting Green. The
existing `OPEN_LIQUIDITY_V2_PURPLE_TRIGGER_EMAIL_ENABLED` switch also controls
new Green trigger emails.
Legacy BTC Delay and Shadow entry handlers cannot compete with these models.

## Plans, Ownership And Restart Safety

Pausing manual entries does not rewrite, cancel or trigger their stored plans.
They become eligible again in manual mode, under the existing 15-minute manual
freshness policy. Merely having ARMED manual plans does not veto Green.

Neither route adds to or reverses an existing account position. Separate durable
owner states prevent a new route from overtaking a position still managed by
the other route, even if an indexer read appears temporarily flat. Pending automatic
submission state blocks manual; unfinished EXECUTING manual state blocks Green.
Unreadable companion evidence blocks new entries and is never deleted to force
a trade. Existing managers continue reconciling and protecting their positions
regardless of the selected entry mode. No stop, TP ladder or legacy position is
migrated at a switch.

The Public V2 manager also checks existing protection at roughly 1H close +2
seconds, independently of the slower map/entry scan. Missing dYdX candle coverage
or failed checks trigger bounded retries for five minutes; the regular poll is
retained. This does not change the separate manual owner's scheduling or transfer
ownership. Fractal confirmation and tighten-only rules remain unchanged.

Green includes the same durable PREPARING/BROADCASTING checkpoint, fresh unsent
revalidation, uncertain-send duplicate protection and queued signal notifications
as ETH/SOL/ZEC. BTC's manual ownership is checked again at the Green broadcast
boundary. A mode switch, process recovery or apparently flat indexer does not
grant permission to blindly repeat an uncertain order.

The Decentrader map's observations, chart and historical records are not converted
or deleted. The Purple overlay remains visual only, including its separate
Binance Spot reconstruction. Switching entry mode does not change its source. Informational BTC
intrusion emails remain controlled independently by their existing switch.

## Read-only Verification

- `GET /open-liquidity/v2/status?market=BTC-USD`: `execution.entryMode`, Green
  readiness, source, initial-stop policy, latest execution attempt and continuity.
- `GET /decentrader/manual-override?market=BTC-USD`: the legacy URL now reports
  `candleSource=Binance Spot BTCUSDT 1H`, the same entry mode, paused status,
  saved plans and the last observed Spot candle. The URL is not a data dependency
  on the Decentrader website.

Tests are offline with mocked exchange mutations and SMTP. No test order, live
mode change, paid infrastructure change or deliberate production memory stress
is required to verify the selector. A single-instance/provider outage or exchange
rejection can still interrupt trading; the switch does not guarantee every fill.
