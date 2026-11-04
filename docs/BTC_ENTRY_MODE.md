# BTC Public V2 Entry Mode

```dotenv
BTC_ENTRY_MODE=manual
```

This is the single authoritative BTC entry selector:

| Value | New BTC entries |
| --- | --- |
| `manual` | Saved manual plans only (default when absent). |
| `purple` | Binance Spot Public V2 intrusion plus a fresh Daily close break on the same closed 1H candle. |
| `off` | No new BTC entries. Existing positions keep their original management. |

Values are case-insensitive. Unknown values fail closed as `off`; status reports
`entryMode.valid=false`. This is a mode, not a boolean. Deployment never selects
`purple` implicitly. Change the value in Render and choose **Save and deploy**.
The new value takes effect in the deployed process; it is not an instant
in-process switch.

## Sources And Execution

Public V2 is the main BTC trading map in every mode. Native BTC intrusion records
and the public Spot chart's purple overlay use Binance **Spot BTCUSDT**, not
Decentrader or Futures. Intrusions are derived by our causal Public V2 cohort
model from Spot candles; they are not an official Binance indicator.

BTC manual triggers also read Binance Spot BTCUSDT closed 1H candles. They never
use the Futures close stream. Their planner is explicitly wired to the existing
BTC Public V2 collector. A missing Public V2 planner fails closed; it cannot
silently fetch Decentrader data. When manual TPs are omitted, new native manual
positions keep using Public V2 zones for subsequent automatic TP refreshes.

Execution remains on dYdX BTC-USD. Both manual and purple use the original
dYdX confirmed 1H fractal for sizing and the exact same-hour closed dYdX body for
the initial stop: `open + 0.25 * (close - open)`, with no buffer. Fractal trailing
only tightens. The existing fixed-USD risk, collateral, depth, step/tick and
dynamic-downsizing checks still apply.

Purple uses NO_TP / trailing-only. There is no FD or OI entry gate. Only the latest
closed hour within five minutes and closing after activation is eligible. No
historical signal is dispatched when enabling purple. Spot candles, replica
OHLC, market/model and confirmed Daily levels must agree, using the same pure
evaluation functions as the public overlay.

The existing `DECENTRADER_AUTO_TRADE_ENABLED` remains the BTC execution master
kill switch. Manual also respects `MANUAL_ENTRY_TP_OVERRIDE_ENABLED`; purple
requires its monitor and protective execution to be enabled. Existing deployed
values need not change when toggling between modes. The old, previously unused
`OPEN_LIQUIDITY_V2_BTC_AUTO_TRADE_ENABLED` is not a second mode switch: explicit
`BTC_ENTRY_MODE=purple` is the BTC opt-in. ETH/SOL and other pair flags are unchanged.
Legacy BTC Delay and Shadow entry handlers cannot compete with these models.

## Plans, Ownership And Restart Safety

Pausing manual entries does not rewrite, cancel or trigger their stored plans.
They become eligible again in manual mode, under the existing 15-minute manual
freshness policy. Merely having ARMED manual plans does not veto purple.

Neither route adds to or reverses an existing account position. Separate durable
owner states prevent a new route from overtaking a position still managed by
the other route, even if an indexer read appears temporarily flat. Pending purple
submission state blocks manual; unfinished EXECUTING manual state blocks purple.
Unreadable companion evidence blocks new entries and is never deleted to force
a trade. Existing managers continue reconciling and protecting their positions
regardless of the selected entry mode. No stop, TP ladder or legacy position is
migrated at a switch.

Purple includes the same durable PREPARING/BROADCASTING checkpoint, fresh unsent
revalidation, uncertain-send duplicate protection and queued signal notifications
as ETH/SOL. BTC's manual ownership is checked again at the purple broadcast
boundary. A mode switch, process recovery or apparently flat indexer does not
grant permission to blindly repeat an uncertain order.

The Decentrader map's observations, chart and historical records are not converted
or deleted. Public V2 no longer displays Decentrader-derived purple history as if
it were Spot-native. Its purple history is limited to available native recorded
intrusions. Switching entry mode does not change that source. Informational BTC
intrusion emails remain controlled independently by their existing switch.

## Read-only Verification

- `GET /open-liquidity/v2/status?market=BTC-USD`: `execution.entryMode`, purple
  readiness, source, initial-stop policy, latest execution attempt and continuity.
- `GET /decentrader/manual-override?market=BTC-USD`: the legacy URL now reports
  `candleSource=Binance Spot BTCUSDT 1H`, the same entry mode, paused status,
  saved plans and the last observed Spot candle. The URL is not a data dependency
  on the Decentrader website.

Tests are offline with mocked exchange mutations and SMTP. No test order, live
mode change, paid infrastructure change or deliberate production memory stress
is required to verify the selector. A single-instance/provider outage or exchange
rejection can still interrupt trading; the switch does not guarantee every fill.
