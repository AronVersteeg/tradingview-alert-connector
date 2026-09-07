# Managed position safety

## September 2026 priority changes

BTC and all Public Perp V2 monitors run position management independently of
entry scanning, on the existing `DECENTRADER_GAP_POLL_MINUTES` cadence. This does
not shorten the Delay or alter candle, delta, OI, TP selection or TP weights.

- Disabling a pair's `*_AUTO_TRADE_ENABLED` blocks new entries, not management
  of already monitor-owned positions. The BTC global auto-entry switch follows
  the same policy. Disabling intrusion scanning also leaves management running.
- Stop management runs before TP planning/submission and does not require a
  liquidity map. A TP exception cannot skip the preceding stop check.
  TP preparation runs outside the position lock, so a stalled map request also
  cannot block subsequent stop cycles. Plans taking longer than one management
  interval are discarded, and ownership is checked again before submission.
- Scanner and management share state while active. Position-changing work is
  serialized, preventing an entry and trailing cycle from racing and preventing
  a slow scanner from restoring an older stop when it saves its state.
- Only monitor-owned positions are managed. Existing ownership/direction/size
  checks remain; unrelated manually opened positions are not adopted by this
  change. Existing dynamic SL/TP and live-trailing switches are still honored.
- The initial entry plan uses the confirmed Williams wick plus configured
  buffer, just like trailing: it no longer silently widens the stop to the
  legacy `DECENTRADER_SL_MIN_DISTANCE_PCT`. No new env is required. The legacy
  variable no longer influences generated initial plans; existing stops are
  not arbitrarily loosened during deployment.

`positionManagement` in the BTC gap status and V2 execution status reports the
running state, last start/finish, last successful stop check and SL/TP outcomes.
A failed or disabled check must not be interpreted as proof of stop coverage.

## Entry risk guard

Generated managed entry alerts carry the configured `riskBudgetUsd`. Before
cancelling existing orders, the dYdX executor fetches current market metadata
and the orderbook. It rejects an invalid/breached stop, missing book/metadata,
or insufficient visible liquidity within the risk-compatible entry price.

For a quantity Q, trigger S and budget B, the adverse entry boundary is
`S + B/Q` for a long and `S - B/Q` for a short, rounded conservatively to market
ticks, including an allowance for adverse trigger tick rounding. The normal
market-order slippage boundary is also respected. Every entry correction keeps
the risk boundary even if the reference price changes. Reduce-only protective
exits are never restricted by the entry boundary.

This is a price-to-trigger risk guard, not a guarantee of maximum net loss.
Fees, funding, stop-execution slippage and gaps remain outside that budget.
The book can change after preflight; IOC partial execution and existing target
reconciliation/fail-safe handling still apply. No blind transaction retries or
new automatic stop widening/repositioning are introduced.

Logs expose preflight expected fill, price limit, expected trigger risk, and
actual entry-to-trigger risk when the protective stop is installed. Rejection
can result in fewer entries, including a STRONG alert whose executable price
does not fit the budget. This is intentional; the signal classifier is unchanged.

## Verification after deploy

1. Check `positionManagement.lastFinishedAt` advances on the configured cadence.
2. Check `lastSuccessfulStopCheckAt` and actual SL outcomes for an owned position.
3. Confirm a disabled auto-entry pair still has management activity if it owns
   a position. Do not open a live test position merely to verify this setting.
4. Inspect entry preflight passes/rejections and actual fill-risk logs on the
   next naturally eligible signal. Compare exchange orders to the desired stop.
5. Continue monitoring dYdX read recovery and TP errors separately. Successful
   compilation/tests do not verify live exchange state or guarantee availability.
