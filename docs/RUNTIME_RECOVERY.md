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
does not increase RAM. No paid instance-size setting is changed here.

The November 2 export does prove a heap OOM: SOL's candle closed at 18:00 UTC,
the replica refreshed at 18:00:08, entry intake and risk preflight passed at
18:00:14-15, and V8 aborted at 18:00:26 with a roughly 259 MiB heap limit. The
supervisor restarted the child two seconds later. The native stack includes
JSON.parse, but does not identify the JavaScript caller. There is no logged
entry broadcast confirmation before the crash; absence of that log alone is
not proof that nothing was submitted. Legacy uncertain intents remain protected
against blind re-entry. See [entry continuity](ENTRY_CONTINUITY.md).

## Container-Aware Heap Budget

On Render only, the supervisor now reads the actual cgroup memory limit. When a
finite 256-2048 MiB limit is proven and NODE_OPTIONS does not already specify a
heap maximum, it assigns 60% of the container limit to V8 old space, capped at
1024 MiB. For a proven 512 MiB container this is 307 MiB, leaving 205 MiB for
native allocations, buffers, the supervisor and other overhead. This is not a
guarantee against a container OOM. No limit is guessed from host RAM and no
override is applied when cgroup data is unavailable or unbounded.

Explicit `--max-old-space-size`, underscore variants and `--max-heap-size` in
NODE_OPTIONS are respected. The effective V8 heap limit can differ from the old
space setting. Check the startup `Service memory budget` log and health JSON
`memoryBudget` alongside actual `memoryMb.heapLimit` and RSS after deployment.

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

Monthly gap and liquidity-observation archive verification/index rebuilds now
use the same bounded reader, with event-loop yields every 128 rows. Coverage
audits retain hour counters and signature indexes, not all history objects.
Append capture clones one evidence record at a time instead of a whole history
batch; oversized pending captures are explicitly rejected. The append-only
files, fingerprints and revision-chain validation remain intact. Full-history
read APIs still retain the rows explicitly requested by a caller. This removes
identified allocation peaks; it does not prove which JSON.parse caused the OOM.

## Verification

Tests simulate 134 and SIGABRT exits, check capped backoff, no overlapping children,
intentional shutdown, pending-retry cancellation and launch failures. A real
isolated dummy child exits with 134 once, is recovered, then shuts down without
a third launch. It imports no application, exchanges, collectors or SMTP.
Reader tests cover UTF-8/chunk boundaries, CRLF, empty/unfinished files, descriptor
cleanup, oversized rows, multi-megabyte histories and exact old/new sampling.

The November 2 archive stress check generated 6,000 valid fingerprinted records
(74,716,890 bytes) and verified them with an actual measured 67 MiB V8 heap limit.
Peak sampled heap was 7.5 MiB and RSS 54.6 MiB in the isolated reader process.
This is an archive-allocation test, not a measurement of full trading-service
memory and not a production uptime guarantee. Checkpoint tests use mocked SDK
submissions only, including fresh PREPARING recovery, uncertain-send refusal,
expiry, disabled entries and persistence failure immediately before submission.

Sources:
- <https://render.com/docs/health-checks>
- <https://nodejs.org/docs/latest-v20.x/api/process.html#exit-codes>
