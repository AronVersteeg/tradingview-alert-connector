# Purple Line Setup

This is the shared ETH and SOL setup. "Purple line setup" means this complete
configuration, not the old Delay or Shadow entry model. BTC manual trading is
unchanged. The SOL tab remains `/decentrader/map?source=open-v2-sol`.

## Sources And Dashboard

| Pair | Signal and chart | Execution | Replica grid |
| --- | --- | --- | --- |
| ETH | Binance Spot ETHUSDT | dYdX ETH-USD | $5 |
| SOL | Binance Spot SOLUSDT | dYdX SOL-USD | $0.10 |

Both use the same UI and mobile interactions: Spot 1H candles, confirmed 1H
fractal boxes, D/W/M fractal tables and intact overviews, projected fractal lines,
green gap boxes, Gap/D and Gap/W break markers, purple intrusion/D markers and
POC crossbars, plus read-only manual/TP overlays. Order blocks remain hidden,
exactly as on ETH. The loading label and WebSocket subscription follow the pair.

Candles, D/W/M fractals, gap/intrusion reconstruction and POC use that pair's
Binance Spot source, never Decentrader or another pair. F5m is deliberately the
matching USD-M **Futures** directional delta label, as on ETH. It is observation
only, not an entry requirement. POC may require paginated historical loading;
missing POC or FD never delays an otherwise valid trade or hides its purple line.
All annotation feeds retain the shared request pacing and resource bounds.

SOL keeps its existing replica model, histories, gap/observation journals and
trade-state file. No historical data or manual plans are reset or migrated.
Separate market caches and disk files prevent BTC/ETH/SOL contamination.
Historical chart coverage remains limited to actually available source data.

## Entry And Protection

- A newly closed Binance Spot 1H candle must have both a directional intrusion
  and a fresh close break of a previously confirmed, intact Daily fractal in
  the same direction. The server uses the chart's same pure purple evaluator.
- Long closes strictly above the Daily HIGH; short strictly below the Daily LOW.
  Equal touches, prior wick breaks, mixed-side intrusions and unconfirmed levels
  do not qualify. Alert time identifies the candle open, not email time.
- W/M lines do not substitute for a missing eligible Daily break. Their
  underlying Daily level qualifies only under these same confirmed/intact rules.
- Only the latest closed hour, within five minutes, after activation/restart
  can trigger. Historical purples never become retrospective live entries.
- Closed-hour monitoring starts around five seconds after the hour; source
  availability, shared throttling and retries can still introduce latency.
- Binance triggers the entry only. The exact same-hour closed **dYdX** candle
  supplies execution OHLC. Original dYdX fractal risk sizing and dynamic
  downsizing remain unchanged. Existing $15 risk configuration is reused.
- Initial SL is `(dydx.open + dydx.close) / 2`, with no added buffer. The original
  confirmed dYdX fractal remains the sizing/trailing anchor. Later stops only
  tighten; a worse/equal fractal does not widen the midpoint stop.
- No TP orders: `NO_TP`, empty ladder and persisted `trailing-only` management,
  including after restart. Profit/loss exits therefore depend on protective
  stops. Missing protective execution support blocks new entries.
- No additions/reversals when the pair already has a position. Durable intent
  is written before submission; uncertain fills reconcile rather than retrying
  the entry. Entry switches do not disable protection of an existing position.
- While enabled, purple owns that pair's entry routing: old Delay, Shadow and
  manual execution are suppressed. Stored manual plans remain intact.

## SOL Activation

After deploying the code, set both in Render:

```dotenv
OPEN_LIQUIDITY_V2_SOL_GAP_DAILY_ENTRY_ENABLED=true
OPEN_LIQUIDITY_V2_SOL_AUTO_TRADE_ENABLED=true
```

The new strategy switch defaults false, exactly like ETH. Explicit existing
pair-off settings are never bypassed. The SOL intrusion monitor defaults true;
if overridden, `OPEN_LIQUIDITY_V2_SOL_INTRUSION_MONITOR_ENABLED` must also be true.
Existing `DECENTRADER_DYNAMIC_SL_ENABLED`,
`DECENTRADER_DYNAMIC_SL_LIVE_UPDATES_ENABLED` and
`DYDX_V4_FRACTAL_TRAIL_ENABLED` must remain enabled.

Read `/open-liquidity/v2/status?market=SOL-USD`: both
`execution.autoTradeEnabled` and `execution.gapDailyEntry.enabled` must be true,
with `hasTradeExecutor=true` and the SOL source. `gapDailyEntry.setup` identifies
`purple-line`; its activation/decision and execution outcome are persisted.
Deployment alone is not evidence that Render's environment is armed.

ETH keeps its existing switches and all other pairs remain untouched. Turning
off purple restores other entry models only if their own switches permit it;
turn the pair's autotrade switch off as well to stop all new entries.

## Verification

Shared ETH/SOL regression cases cover exact Spot source identity, both entry
directions, stale/mixed data rejection, SL/risk separation, tighten-only trailing,
partial fills, no duplicate entry, activation/restart, no-TP recovery and old
entry-route suppression. Browser fixtures cover desktop/mobile overlay pixels,
POC crossbars, symbol-correct loading, same-candle matching, checkbox controls,
failure retention and absence of write requests. These tests place no real orders
or emails and cannot guarantee future exchange execution.
