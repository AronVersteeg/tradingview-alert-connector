import {
  buildCausalSnapshots,
  detectStrictMultiVenueGap,
  inferredLongShare,
  MultiVenueLiquidityZone
} from '../src/services/openLiquidityV2MultiSource';

function zone(input: Partial<MultiVenueLiquidityZone>): MultiVenueLiquidityZone {
  return {
    side: 'L',
    leverage: 10,
    price: 60_000,
    positionCount: 2,
    notionalUsd: 2_000_000,
    weightedUsd: 1_400_000,
    confidence: 0.7,
    uncertaintyUsd: 100,
    sourceCount: 2,
    sources: ['binance', 'okx'],
    exactInventoryUsd: 0,
    inferredInventoryUsd: 2_000_000,
    ...input
  };
}

describe('Open Liquidity V2 multi-source model', () => {
  test('uses taker flow and candle body instead of account ratios', () => {
    const bullish = inferredLongShare({
      timestampMs: 1,
      open: 100,
      high: 112,
      low: 98,
      close: 110,
      quoteVolumeUsd: 1_000,
      takerBuyUsd: 800
    });
    const bearish = inferredLongShare({
      timestampMs: 2,
      open: 110,
      high: 111,
      low: 98,
      close: 100,
      quoteVolumeUsd: 1_000,
      takerBuyUsd: 200
    });

    expect(bullish.longShare).toBeGreaterThan(0.5);
    expect(bearish.longShare).toBeLessThan(0.5);
    expect(bullish.confidence).toBeGreaterThan(0.7);
  });

  test('builds and later sweeps cohorts without using future candles at birth', () => {
    const referenceCandles = [
      { timestampMs: 1_000, open: 100, high: 102, low: 99, close: 101, quoteVolumeUsd: 1_000 },
      { timestampMs: 2_000, open: 101, high: 103, low: 100, close: 102, quoteVolumeUsd: 1_000 },
      { timestampMs: 3_000, open: 102, high: 103, low: 89, close: 91, quoteVolumeUsd: 1_000 }
    ];
    const venueHours = [[
      { ...referenceCandles[0], venue: 'test', oiUsd: 10_000_000, flowPrecision: 'venue-taker' as const, takerBuyUsd: 700 },
      { ...referenceCandles[1], venue: 'test', oiUsd: 12_000_000, flowPrecision: 'venue-taker' as const, takerBuyUsd: 700 },
      { ...referenceCandles[2], venue: 'test', oiUsd: 12_000_000, flowPrecision: 'venue-taker' as const, takerBuyUsd: 300 }
    ]];
    const snapshots = buildCausalSnapshots({
      referenceCandles,
      venueHours,
      marketFlowCandles: referenceCandles
    });

    expect(snapshots[1].zones.length).toBeGreaterThan(0);
    expect(snapshots[2].zones.length).toBeLessThan(snapshots[1].zones.length);
  });

  test('only accepts a corridor whose combined interior density is genuinely low', () => {
    const clean = detectStrictMultiVenueGap([
      zone({ side: 'L', price: 59_000, weightedUsd: 4_000_000 }),
      zone({ side: 'L', price: 62_000, weightedUsd: 50_000 }),
      zone({ side: 'S', price: 65_000, weightedUsd: 60_000 }),
      zone({ side: 'S', price: 68_000, weightedUsd: 3_500_000 })
    ], 64_000, { minClusterUsd: 1_000_000 });
    const narrowed = detectStrictMultiVenueGap([
      zone({ side: 'L', price: 59_000, weightedUsd: 4_000_000 }),
      zone({ side: 'L', price: 62_000, weightedUsd: 1_500_000 }),
      zone({ side: 'S', price: 65_000, weightedUsd: 1_250_000 }),
      zone({ side: 'S', price: 68_000, weightedUsd: 3_500_000 })
    ], 64_000, { minClusterUsd: 1_000_000 });
    const filled = detectStrictMultiVenueGap([
      zone({ side: 'L', price: 63_800, weightedUsd: 1_500_000 }),
      zone({ side: 'S', price: 64_200, weightedUsd: 1_250_000 })
    ], 64_000, { minClusterUsd: 1_000_000 });

    expect(clean?.status).toBe('confirmed');
    expect(narrowed?.left).toBe(62_000);
    expect(narrowed?.right).toBe(65_000);
    expect(filled).toBeUndefined();
  });
});
