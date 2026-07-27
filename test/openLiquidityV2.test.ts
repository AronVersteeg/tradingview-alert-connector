import {
  aggregateGmxPositionZones,
  calculateGmxLiquidationEstimate,
  detectV2LiquidityGap,
  GmxV2PositionInput,
  V2LiquidityZone
} from '../src/services/openLiquidityV2';

const USDC = '0xaf88d065e77c8cC2239327C5EDb3A432268e5831';

function position(input: Partial<GmxV2PositionInput> = {}): GmxV2PositionInput {
  return {
    positionKey: 'position-1',
    market: 'btc-market',
    collateralToken: USDC,
    isLong: true,
    sizeInUsd: '1000000000000000000000000000000000000',
    entryPrice: '650000000000000000000000000',
    leverage: '100000',
    openedAt: 1_700_000_000,
    ...input
  };
}

function zone(input: Partial<V2LiquidityZone>): V2LiquidityZone {
  return {
    side: 'L',
    leverage: 10,
    price: 60_000,
    positionCount: 4,
    notionalUsd: 1_000_000,
    weightedUsd: 750_000,
    confidence: 0.75,
    uncertaintyUsd: 400,
    sourceCount: 1,
    ...input
  };
}

describe('Open Liquidity V2', () => {
  test('derives liquidation distance from current GMX leverage instead of entry price', () => {
    const long = calculateGmxLiquidationEstimate(position(), 65_000);
    const short = calculateGmxLiquidationEstimate(position({ isLong: false }), 65_000);

    expect(long?.leverage).toBe(10);
    expect(long?.liquidationPrice).toBeCloseTo(58_987.5, 1);
    expect(short?.liquidationPrice).toBeCloseTo(71_012.5, 1);
    expect(long?.confidence).toBeGreaterThan(0.7);
    expect(long?.uncertaintyHigh).toBeGreaterThan(long?.liquidationPrice || 0);
  });

  test('aggregates real position notional into deterministic price bins', () => {
    const result = aggregateGmxPositionZones(
      [
        position({ positionKey: 'a' }),
        position({ positionKey: 'b', sizeInUsd: '500000000000000000000000000000000000' })
      ],
      65_000,
      50
    );

    expect(result.accepted).toBe(2);
    expect(result.zones).toHaveLength(1);
    expect(result.zones[0].price).toBe(59_000);
    expect(result.zones[0].positionCount).toBe(2);
    expect(result.zones[0].notionalUsd).toBe(1_500_000);
  });

  test('detects a gap from strong opposite-side edge clusters and reports one-source confidence', () => {
    const gap = detectV2LiquidityGap(
      [
        zone({ side: 'L', price: 59_000, weightedUsd: 1_500_000 }),
        zone({ side: 'L', price: 62_000, weightedUsd: 20_000 }),
        zone({ side: 'S', price: 68_000, weightedUsd: 1_200_000 }),
        zone({ side: 'S', price: 65_500, weightedUsd: 25_000 })
      ],
      64_000,
      { minClusterUsd: 250_000, sourceAgreement: 1 }
    );

    expect(gap?.left).toBe(59_000);
    expect(gap?.right).toBe(68_000);
    expect(gap?.cleanliness).toBeGreaterThan(0.9);
    expect(gap?.status).toBe('candidate');
    expect(gap?.sourceAgreement).toBe(1);
  });

  test('promotes a clean gap only after two independent position sources agree', () => {
    const gap = detectV2LiquidityGap(
      [
        zone({ side: 'L', price: 59_000, weightedUsd: 1_500_000, sourceCount: 2 }),
        zone({ side: 'S', price: 68_000, weightedUsd: 1_200_000, sourceCount: 2 })
      ],
      64_000,
      { minClusterUsd: 250_000, sourceAgreement: 2 }
    );

    expect(gap?.status).toBe('confirmed');
    expect(gap?.confidence).toBeGreaterThan(0.7);
  });
});
