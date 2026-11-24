# Per-Pair Entry Modes

BTC, ETH, INJ, SOL, WLD and ZEC each have an independent selector:

| Pair | Environment variable |
| --- | --- |
| BTC | `BTC_ENTRY_MODE` |
| ETH | `ETH_ENTRY_MODE` |
| INJ | `INJ_ENTRY_MODE` |
| SOL | `SOL_ENTRY_MODE` |
| WLD | `WLD_ENTRY_MODE` |
| ZEC | `ZEC_ENTRY_MODE` |

| Value | New entries |
| --- | --- |
| `purple` | Directional Spot intrusion + fresh gap-box close break + fresh confirmed Daily close break, on the same closed 1H candle. |
| `green` | Spot intrusion + fresh gap-box close break on the same closed 1H candle, in the direction of the most recent Purple on an earlier candle. |
| `atr` | Green plus a same-hour dYdX body in the trade direction at least 2x the native Binance Spot ATR. ATR is the simple mean of the prior 14 hourly true ranges, excluding the break hour. Missing input never falls back to Green. |
| `atr-purple` | Purple plus the same 2x ATR filter. A qualifying Green or a qualifying ATR candle without a confirmed same-hour Daily break is not enough. |
| `manual` | Existing manual entry plans only. Plans may still be edited or armed in other modes, but do not trigger there. |
| `off` | No new automatic or manual entries for this pair. Existing positions keep their protection. |

`purple-trail` is accepted as an alias for `purple`. Values are case-insensitive;
invalid explicit values fail closed as `off` and report `valid=false`.

For example, to retain the three currently used Green entry models explicitly:

```dotenv
ETH_ENTRY_MODE=green
SOL_ENTRY_MODE=green
ZEC_ENTRY_MODE=green
```

Set any one value to `purple`, `atr`, `atr-purple`, `manual` or `off` without changing the other pairs.
Use Save and deploy in Render; these are process environment settings, not a
dashboard toggle. This code change does not write production environment values.

## Compatibility And Kill Switches

When an ETH/SOL/ZEC selector is absent or blank, the existing
`OPEN_LIQUIDITY_V2_<PAIR>_GAP_DAILY_ENTRY_ENABLED` boolean still selects Green
when true; otherwise the selector reports Manual fallback. Legacy Delay/Shadow
routing keeps its pre-selector behavior while the new selector is absent.
An explicit selector overrides that old entry boolean and excludes legacy
Delay/Shadow entries, including in Manual and Off. BTC retains its existing
default of Manual when its selector is absent.
INJ and WLD default to `off` with no legacy entry fallback; this preserves
monitoring and protection of existing positions. To make new entries eligible,
explicitly select a mode and enable the pair's auto-trade switch.

The existing `OPEN_LIQUIDITY_V2_<PAIR>_AUTO_TRADE_ENABLED` remains the execution
kill switch for ETH/INJ/SOL/WLD/ZEC. Selecting Purple, Green, ATR, ATR Purple or Manual never overrides
an explicit false auto-trade value. BTC retains
`DECENTRADER_AUTO_TRADE_ENABLED` as its execution master. Automatic entry monitors
and protective SL/trailing settings must also remain enabled; Manual retains
`MANUAL_ENTRY_TP_OVERRIDE_ENABLED`.

## Sources And Stops

Green and ATR are continuation modes. The latest earlier Purple sets the direction
for that pair until an opposite Purple closes. ATR Purple also counts because it
meets the same Purple conditions. The current candle cannot establish its own
direction for Green. This does not depend on whether the Purple mode was selected
or whether a trade filled. With no verified earlier Purple direction, Green and
ATR do not signal; an unverified history gap is not bridged. The Purple, ATR Purple
and Yellow rules, existing positions and all stop management are unchanged.

All automatic modes use only the selected pair's native Binance Spot USDT 1H
intrusions/gaps. Purple additionally reads that same pair's Binance Spot Daily
fractals. Both ATR modes use dYdX for the matching closed break body and do not
substitute Spot for that measurement. The dark-green ATR and pink ATR Purple
overlays show eligible signals, not fills. Both retests apply the one-position rule.
Neither dashboard control enables live entries.

Automatic sizing and protection stay on the matching dYdX market: original
confirmed fractal sizing, exact same-hour body-open initial SL, no TPs, and
[later same-direction Yellow-gated fractal trailing](YELLOW_INTRUSION_TRAILING.md).
The internal automatic Purple setup id remains `purple-green-trail` for all
pairs; this is not an extra user-facing selector value.

Manual retains its existing planner/source, quarter-body initial SL, TP ladder
and normal dYdX fractal management. BTC manual triggers remain Binance Spot;
the existing non-BTC manual triggers remain Binance Futures. The selector does
not convert their strategy, source or stop policy into the automatic model.

## Ownership, Restart And Mail

Switching changes only new entry eligibility. It never resets a running SL,
changes position size, cancels a saved manual plan, adds to/reverses a position,
or transfers its owner. The current automatic/manual manager keeps running.
An uncertain automatic submission blocks manual execution even if the indexer
temporarily appears flat. An unfinished EXECUTING manual plan blocks an explicit
automatic route; an ARMED plan does not. Unreadable manual ownership evidence
fails closed. Ownership and mode are checked again before automatic broadcast.

Switching automatic setups starts a new activation instead of entering
an older signal from the previous model. Restart continuity remains keyed by
pair, model and setup; the five-minute automatic entry freshness bound is not
extended. Pending exchange reconciliation/stop repair continues independently.

Mail uses the global entries-only policy: only confirmed automatic and manual
entries are delivered. Skipped, detected-only, failed, uncertain or flattened
signals are not mailed, including old queued signals after a restart. Confirmed
automatic messages retain the existing trigger-mail switch, retry and deduplication.
Intrusion, filtered, observational Shadow, TP-hit and scanner mail is suppressed,
even if an old informational email env is true. This does not disable monitoring,
stored intrusion evidence or any overlay. Off/Manual do not flush queued automatic
entry mail. Displaying a Yellow does not depend on the selected entry mode.

Inspect `execution.entryMode` and `execution.gapDailyEntry` on
`/open-liquidity/v2/status?market=<PAIR>-USD`. Manual status also exposes
`entryMode` and `entriesPaused`; its stored plans remain present.

Tests use temporary state files and mocked exchange/SMTP only. No order or
production mode switch is required to verify this routing.
