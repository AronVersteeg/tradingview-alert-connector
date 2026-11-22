# Active Pair Strategy Replay

Updated: 2026-11-22T15:19:07.728Z

Assumptions: $15 risk per entry, 5 bps hypothetical fees per side, no funding or extra slippage. Every strategy is replayed separately with at most one open position per pair. Open P&L is excluded from the ranking.

| Pair | Green | Purple | ATR | ATR Purple | Historical best |
| --- | ---: | ---: | ---: | ---: | --- |
| BTC | +$165.71 | +$268.13 | +$370.15 | +$271.54 | ATR |
| ETH | -$84.49 | +$168.23 | +$138.06 | +$262.45 | ATR Purple |
| INJ | -$56.56 | -$55.65 | +$6.96 | +$86.71 | ATR Purple |
| SOL | -$341.82 | +$73.76 | +$33.77 | +$55.16 | Purple |
| ZEC | +$517.86 | +$411.29 | +$212.88 | +$193.47 | Green |

## Coverage

| Pair | From | Through | Missing dYdX hours | Replay restarts |
| --- | --- | --- | ---: | ---: |
| BTC | 2023-11-16T15:00:00.000Z | 2026-11-22T15:00:00.000Z | 77 | 0 |
| ETH | 2023-11-16T13:00:00.000Z | 2026-11-22T15:00:00.000Z | 70 | 1 |
| INJ | 2024-03-07T16:00:00.000Z | 2026-11-22T15:00:00.000Z | 646 | 16 |
| SOL | 2023-11-17T06:00:00.000Z | 2026-11-22T15:00:00.000Z | 189 | 1 |
| ZEC | 2024-11-27T15:00:00.000Z | 2026-11-22T15:00:00.000Z | 118 | 0 |

## Interpretation

- “Historical best” means the highest realized net result inside that pair’s own tested period; periods differ by listing and usable dYdX coverage.
- Incomplete trades around missing dYdX candles are excluded from realized P&L and remain visible in the JSON report.
- Historical ranking is descriptive, not a guarantee of future profitability.

Run `npm run update` to refresh market data and rebuild this report. Use `npm run update -- --offline` to recalculate only from the local cache.
