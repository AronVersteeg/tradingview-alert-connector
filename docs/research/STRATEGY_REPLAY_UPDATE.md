# Active Pair Strategy Replay Update

`npm run update` refreshes and compares Green, Purple, ATR and ATR Purple for
BTC, ETH, INJ, SOL and ZEC. The generated summary is written to:

- `research-output/active-pair-strategy-replay.md`
- `research-output/active-pair-strategy-replay.json`

The first run downloads the longest usable Binance Spot USDT and dYdX hourly
history from November 2023 onward. Later runs replace a 24-hour overlap and add
only newly closed candles. Source files stay outside Git in the operating-system
temporary directory by default. Set `STRATEGY_REPLAY_DATA_DIR` to keep them in a
different persistent local directory.

For a deterministic replay from already downloaded source data:

```powershell
npm run update -- --offline
```

An exact historical cutoff can be selected for audit work:

```powershell
npm run update -- --through 2026-11-20T07:00:00Z
```

Each strategy is simulated independently, with no more than one position per
pair. The report ranks realized net P&L at `$15` risk per entry and hypothetical
fees of `5 bps` per side. It does not include funding or additional slippage.
Open P&L is shown in JSON but excluded from the ranking. Missing dYdX candles
produce explicit incomplete trades and replay restarts; the updater never
invents an exit across a data gap.

The ATR modes require the directional dYdX signal-candle body to be at least
twice the arithmetic mean true range of the preceding 14 fully closed Binance
Spot hours. The signal hour is excluded from that ATR window.
