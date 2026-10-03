# CoinGlass Whale Feed Recovery

The whale collector reads the public CoinGlass `largeTakerOrder` WebSocket feed,
not the Binance REST API. Binance cooldown fixes do not repair this feed.

On 2026-10-03, read-only subscriptions returned valid gzip snapshots for BTC,
ETH, INJ, SOL and ZEC. A five-symbol check received snapshots 11.5 to 15.7 seconds
after connection. The previous default deadline was only 12 seconds; a healthy
subscription could therefore be closed before its first snapshot arrived.
This explains a reproduced failure mode, not necessarily every historic timeout.
The repaired client was subsequently tested on the same five symbols with the
legacy 12-second setting: all returned valid snapshots. INJ took 25.6 seconds and
SOL 30.0 seconds, confirming that the old timeout is too short for healthy feeds.

The shared transport now uses the existing `ws` dependency instead of custom
WebSocket frame handling. It assembles fragmented messages, handles protocol
pings, bounds message/decompressed sizes and decodes JSON, gzip, zlib and raw
deflate payloads. Only whale snapshots for the requested symbol/interval are
accepted; subscription acknowledgements and heartbeats are not empty snapshots.

Connect/upgrade has a 10-second deadline. Snapshot delivery has a separate
75-second minimum deadline, allowing a full m1 push window plus margin.
`COINGLASS_WHALE_TIMEOUT_MS` and existing per-pair timeout overrides may extend
this to at most 120 seconds; older shorter values are clamped to 75 seconds.
No new environment settings or paid API credentials are required.

Single-flight requests and exponential failure backoff remain in place. Backoff
starts when the failed attempt finishes, so a long snapshot wait cannot consume
its own retry delay. BTC gap checks refresh CoinGlass in the background rather
than delaying entry/alert checks while waiting for whale data.

Snapshot status now includes `freshness` (`MISSING`, `FRESH`, `STALE`) and
`ageSeconds`. After 15 minutes, cached levels are excluded from the current
`levels` array; historical levels and observations remain preserved. Restarting
does not promote historical walls into a current snapshot.

Whale observations use the actual snapshot receipt time and are recorded at most
once per received snapshot. A failed or stale refresh cannot relabel a cached
wall as a newly observed wall. Previously saved research records are unchanged.

After deployment, check `/decentrader/gap-status` and
`/open-liquidity/v2/status?market=ETH-USD` (also INJ, SOL, ZEC): new `fetchedAt`,
`freshness: FRESH`, cleared errors and reset failure counters confirm recovery.
A valid empty snapshot can contain zero walls above the configured threshold;
zero levels alone is not proof of failure. This repair does not change trading
enable flags, manual plans, stop-loss/trailing logic or manually locked TP prices.
External outages can still occur; local smoke tests do not certify Render's
outbound connectivity. Verify the deployed collectors separately.
