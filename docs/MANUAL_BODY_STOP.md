# Manual Entry Initial Stop

New manual entries use the same initial-stop formula as ETH/SOL purple entries:

```text
initial SL = dYdX signal candle open + 0.25 * (close - open)
```

This uses only the body, not wicks, and no added buffer. The candle must be the
fully closed dYdX 1H candle with exactly the same opening timestamp as the manual
Binance trigger candle. Both the Binance inclusive end millisecond and the hour
boundary are accepted as representations of its closing time. Binance prices
do not supply the stop. Current dYdX oracle price must still be on the protective
side of the SL; invalid/mismatched input does not create an unprotected order.

The original valid confirmed dYdX fractal remains the sizing stop and trailing
anchor. No size is recalculated from the shorter body stop. Shared execution
risk, collateral, tick/step and dynamic-downsizing checks still apply. Manual
TP ladders, automatic TP fallback when no manual levels are supplied, and the
fractal trailing logic are unchanged. Trailing can only tighten protection.

The rule applies to the BTC Decentrader-backed manual handler and the shared V2
manual handlers. It does not enable any disabled pair, bypass ETH/SOL purple
ownership, change Shadow/Delay entries or move a stop on an existing position.
Existing armed plans do not need to be re-entered; new executions after deployment
use the new rule. There is no new environment variable or plan-file migration.

If dYdX's latest closed candle has not reached the signal hour, the handler reports
`tradeDeferred` / `tradePendingData` before setting an attempted signature or
submitting anything. The plan remains ARMED and the existing data-retry scheduler
checks again (5 seconds near the hour, otherwise 60 seconds, plus existing market
offset/backoff). The original 15-minute manual-entry freshness limit is unchanged.
No trigger mail is sent for the interim wait; normal completion mail is unchanged.
Malformed, wrong-source, future or non-protective stops are not treated as data
that will certainly recover.

Submitted alert metadata includes the original `sizingStop` and `initialStopPolicy`.
The execution decision records the policy for auditability. Old managed positions
keep their recorded stop; restart reconciliation never replaces it merely because
the software's initial-stop policy changed.
