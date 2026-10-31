# Runtime Recovery And Memory Diagnostics

## What The Render Events Mean

Exit 134 is consistent with SIGABRT (128 + 6). A fatal V8 heap allocation failure
can produce it, but the exit number alone does not prove memory exhaustion.
The October 30/31 crash stacks were not available locally during this change;
the Render CLI was not authenticated. Check the newest export for `FATAL ERROR`,
`Reached heap limit`, `Allocation failed` and the garbage-collection lines.
Use [the saved export procedure](RENDER_LOG_EXPORT.md), not credentials from chat.

Render already restarts failed health checks automatically. `Service recovered`
does not mean the application can never crash again. A larger persistent disk
does not increase RAM. No heap limit or paid instance-size setting is changed here.

## Automatic Process Recovery

The production Docker command now runs `node scripts/start-server.cjs` directly,
also used by `yarn start` / `npm start`. This is a small separate supervisor:

- Exactly one application child runs. No new child starts until the previous
  process has emitted `close`; an `exit` event alone is not enough.
- Unexpected process exits, including 134/SIGABRT, restart after 2 seconds.
  Repeated failures back off through 4/8/16/32 seconds to at most 60 seconds.
  Ten healthy minutes reset that backoff.
- Healthy processes are never periodically recycled or killed for memory use.
- Render deployment/shutdown signals are forwarded to the child and disable
  all recovery retries. Only a child that refuses an intentional shutdown is
  force-killed after 25 seconds.
- Persistent state, trading switches, order sizes and strategies are not altered.
  Existing restart reconciliation remains responsible for pending submissions,
  stop coverage and prevention of duplicate entries. Exchange-hosted orders are
  not cancelled by the supervisor. Trailing computation is unavailable during
  the crash/restart/collector bootstrap interval; recovery is not zero downtime.
- The supervisor adds a small Node-process memory overhead. It does not make a
  memory leak harmless. A whole-container OOM can still kill the supervisor;
  Render's own recovery and a suitable RAM limit remain necessary.

## HTTP Health Check

After deploying, set **Render -> service Settings -> Health Check Path** to:

```text
/healthz
```

This is a lightweight, no-store HTTP liveness check before Sentry request tracing.
It makes no exchange, Binance, CoinGlass, account or disk-history requests, so
a temporary provider outage does not cause a restart loop. An unresponsive
event loop fails the HTTP probe, unlike a TCP socket that merely stays open.
Render documents automatic restart after 60 seconds of failed running-instance
health checks. The health-check setting itself is not changed by this commit.

The JSON shows uptime, child PID, Node version, `supervised`, restart count and
RSS/heap/external/array-buffer usage plus the V8 heap limit. `ok:true` proves only
that the process can answer HTTP, not that every market is ready to trade.
Inspect each pair's existing status for source readiness, activation and entry
switches. No credentials, account values, environment dumps or heap snapshots
are exposed. Runtime memory diagnostics are also logged once per minute.
High heap pressure is logged but never silently triggers a trading restart.

## Bounded DOM History Reads

Previously DOM history reads loaded each entire daily NDJSON file, split every
line and retained every matching record before sampling down to `maxPoints`.
Coverage reads also loaded whole first/last day files to inspect only two rows.
That creates avoidable transient memory peaks as history grows. It is a concrete
code-level risk, not proof that it caused these particular Render crashes.

History now reads 64 KiB chunks. Coverage reads only the first/last record;
the last record is found by scanning backward. Two-pass range sampling keeps
the exact former stride and last-record rule, retaining at most `maxPoints + 1`
objects rather than the complete queried range. No stored data is removed,
rewritten or shortened, and no trading filter or sampling rule changes.
Malformed/oversized rows fail explicitly with the source file preserved; an
individual row is limited to 4 MiB. Very broad queries still take disk/CPU time.

## Verification

Tests simulate 134 and SIGABRT exits, check capped backoff, no overlapping children,
intentional shutdown, pending-retry cancellation and launch failures. A real
isolated dummy child exits with 134 once, is recovered, then shuts down without
a third launch. It imports no application, exchanges, collectors or SMTP.
Reader tests cover UTF-8/chunk boundaries, CRLF, empty/unfinished files, descriptor
cleanup, oversized rows, multi-megabyte histories and exact old/new sampling.

Sources:
- <https://render.com/docs/health-checks>
- <https://nodejs.org/docs/latest-v20.x/api/process.html#exit-codes>
