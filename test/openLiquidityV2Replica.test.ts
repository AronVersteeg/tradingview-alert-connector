import {
  buildReplicaSnapshots,
  cohortLevelsForOhlc4,
  detectReplicaGap,
  ReplicaLiquidityZone,
  summarizeGoldConfirmation,
  SpotCandle
} from '../src/services/openLiquidityV2Replica';

function candle(
  timestampMs: number,
  open: number,
  high: number,
  low: number,
  close: number
): SpotCandle {
  return {
    timestampMs,
    closeTimeMs: timestampMs + 3_599_999,
    open,
    high,
    low,
    close
  };
}

function zone(input: Partial<ReplicaLiquidityZone>): ReplicaLiquidityZone {
  return {
    side: 'L',
    leverage: 10,
    price: 60_000,
    positionCount: 1,
    relativeCount: 1,
    weightedUsd: 1,
    notionalUsd: 0,
    confidence: 1,
    uncertaintyUsd: 50,
    sourceCount: 1,
    sources: ['binance-spot'],
    ...input
  };
}

describe('Public Perp V2 Binance Spot replica', () => {
  test('reproduces the reconstructed Decentrader multipliers and $100 rounding', () => {
    const levels = cohortLevelsForOhlc4(63_051.6425);
    expect(levels.map((level) => [level.side, level.leverage, level.price])).toEqual([
      ['L', 3, 47_300],
      ['S', 3, 94_600],
      ['L', 5, 52_500],
      ['S', 5, 78_400],
      ['L', 10, 57_600],
      ['S', 10, 69_700]
    ]);
    expect(levels[4].rawPrice).toBeCloseTo(57_584.686785, 6);
    expect(levels[5].rawPrice).toBeCloseTo(69_660.904822, 6);
  });

  test('uses ETH-scale $5 bins without changing the cohort multipliers', () => {
    const levels = cohortLevelsForOhlc4(3_500, 5);
    expect(levels.map((level) => [level.side, level.leverage, level.price])).toEqual([
      ['L', 3, 2_625],
      ['S', 3, 5_250],
      ['L', 5, 2_915],
      ['S', 5, 4_355],
      ['L', 10, 3_195],
      ['S', 10, 3_865]
    ]);
  });

  test('uses INJ-scale $0.01 bins without changing the cohort multipliers', () => {
    const levels = cohortLevelsForOhlc4(5, 0.01);
    expect(levels.map((level) => [level.side, level.leverage, level.price])).toEqual([
      ['L', 3, 3.75],
      ['S', 3, 7.5],
      ['L', 5, 4.17],
      ['S', 5, 6.22],
      ['L', 10, 4.57],
      ['S', 10, 5.52]
    ]);
  });

  test('uses SOL-scale $0.10 bins without changing the cohort multipliers', () => {
    const levels = cohortLevelsForOhlc4(150, 0.1);
    expect(levels.map((level) => [level.side, level.leverage, level.price])).toEqual([
      ['L', 3, 112.5],
      ['S', 3, 225],
      ['L', 5, 125],
      ['S', 5, 186.6],
      ['L', 10, 137],
      ['S', 10, 165.7]
    ]);
  });

  test('uses ZEC-scale $1 bins without changing the cohort multipliers', () => {
    const levels = cohortLevelsForOhlc4(840, 1);
    expect(levels.map((level) => [level.side, level.leverage, level.price])).toEqual([
      ['L', 3, 630],
      ['S', 3, 1_260],
      ['L', 5, 700],
      ['S', 5, 1_045],
      ['L', 10, 767],
      ['S', 10, 928]
    ]);
  });

  test('uses Gold-scale $5 bins without changing the cohort multipliers', () => {
    const levels = cohortLevelsForOhlc4(4_350, 5);
    expect(levels.map((level) => [level.side, level.leverage, level.price])).toEqual([
      ['L', 3, 3_265],
      ['S', 3, 6_525],
      ['L', 5, 3_625],
      ['S', 5, 5_410],
      ['L', 10, 3_975],
      ['S', 10, 4_805]
    ]);
  });

  test('uses Silver-scale $0.10 bins without changing the cohort multipliers', () => {
    const levels = cohortLevelsForOhlc4(69.2, 0.1);
    expect(levels.map((level) => [level.side, level.leverage, level.price])).toEqual([
      ['L', 3, 51.9],
      ['S', 3, 103.8],
      ['L', 5, 57.6],
      ['S', 5, 86.1],
      ['L', 10, 63.2],
      ['S', 10, 76.5]
    ]);
  });

  test('summarizes aligned XAU and PAXG candle confirmation without mixing timestamps', () => {
    const primary = [
      candle(1, 4_300, 4_340, 4_290, 4_330),
      candle(2, 4_330, 4_350, 4_310, 4_320),
      candle(3, 4_320, 4_360, 4_315, 4_350)
    ];
    const confirmation = [
      candle(1, 4_310, 4_350, 4_300, 4_340),
      candle(3, 4_335, 4_350, 4_320, 4_325)
    ];

    const summary = summarizeGoldConfirmation(primary, confirmation);
    expect(summary?.alignedHours).toBe(2);
    expect(summary?.directionAgreementPct).toBe(50);
    expect(summary?.latest.timestamp).toBe(new Date(3).toISOString());
    expect(summary?.latest.directionMatch).toBe(false);
    expect(summary?.latest.basisUsd).toBeCloseTo(-3.75, 4);
  });

  test('does not let a birth candle liquidate its own newly-created cohorts', () => {
    const snapshots = buildReplicaSnapshots([
      candle(1, 60_000, 90_000, 30_000, 60_000)
    ], { frameLimit: 1 });

    expect(snapshots[0].activeCohortCount).toBe(6);
  });

  test('removes only cohorts crossed by a later candle', () => {
    const snapshots = buildReplicaSnapshots([
      candle(1, 60_000, 61_000, 59_000, 60_000),
      candle(2, 60_000, 67_000, 54_000, 60_000)
    ], { frameLimit: 2 });

    expect(snapshots[0].activeCohortCount).toBe(6);
    // L10 at 54,800 and S10 at 66,300 are crossed; four original cohorts
    // survive and six fresh cohorts are added on the second hour.
    expect(snapshots[1].activeCohortCount).toBe(10);
  });

  test('expires cohorts outside the rolling birth window', () => {
    const snapshots = buildReplicaSnapshots([
      candle(1, 60_000, 60_500, 59_500, 60_000),
      candle(2, 60_000, 60_500, 59_500, 60_000),
      candle(3, 60_000, 60_500, 59_500, 60_000)
    ], { cohortWindowHours: 2, frameLimit: 3 });

    expect(snapshots.map((snapshot) => snapshot.activeCohortCount)).toEqual([6, 12, 12]);
  });

  test('encodes the complete histogram as one seed followed by compact deltas', () => {
    const snapshots = buildReplicaSnapshots([
      candle(1, 60_000, 60_500, 59_500, 60_000),
      candle(2, 60_000, 60_500, 59_500, 60_000),
      candle(3, 60_000, 67_000, 54_000, 60_000)
    ], { frameLimit: 3 });
    const state = new Map<string, number>();
    const apply = (tuples: Array<['L' | 'S', 3 | 5 | 10, number, number]>) => {
      for (const [side, leverage, price, count] of tuples) {
        const key = `${side}|${leverage}|${price}`;
        if (count > 0) state.set(key, count);
        else state.delete(key);
      }
    };

    expect(snapshots[0].zoneSeed).toHaveLength(6);
    expect(snapshots[0].zoneDeltas).toEqual([]);
    apply(snapshots[0].zoneSeed || []);
    expect([...state.values()].reduce((sum, count) => sum + count, 0)).toBe(6);
    apply(snapshots[1].zoneDeltas);
    expect([...state.values()].reduce((sum, count) => sum + count, 0)).toBe(12);
    apply(snapshots[2].zoneDeltas);
    expect([...state.values()].reduce((sum, count) => sum + count, 0))
      .toBe(snapshots[2].activeCohortCount);
  });

  test('uses the nearest occupied prices as gap edges regardless of side', () => {
    const gap = detectReplicaGap([
      zone({ side: 'S', leverage: 3, price: 59_000, relativeCount: 4, positionCount: 4 }),
      zone({ side: 'L', leverage: 10, price: 62_000, relativeCount: 2, positionCount: 2 }),
      zone({ side: 'S', leverage: 10, price: 68_000, relativeCount: 3, positionCount: 3 }),
      zone({ side: 'L', leverage: 3, price: 71_000, relativeCount: 5, positionCount: 5 })
    ], 65_000);

    expect(gap?.left).toBe(62_000);
    expect(gap?.right).toBe(68_000);
    expect(gap?.width).toBe(6_000);
    expect(gap?.interiorRelativeCount).toBe(0);
  });
});
