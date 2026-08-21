import {
  buildReplicaTradeZones,
  capTakeProfitsForStatefulOrderCapacity,
  openLiquidityV2GoldIntrusionMonitor,
  openLiquidityV2InjTradeMonitor,
  openLiquidityV2SilverIntrusionMonitor,
  reconstructReplicaIntrusions
} from '../src/services/openLiquidityV2EthTradeMonitor';
import { Gap, LiquidityBar } from '../src/services/decentraderGapMonitor';

function replicaGap(left: number, right: number) {
  return {
    left,
    right,
    width: right - left,
    leftEdge: { side: 'L', leverage: 10, price: left, positionCount: 4, relativeCount: 4 },
    rightEdge: { side: 'S', leverage: 10, price: right, positionCount: 3, relativeCount: 3 }
  };
}

describe('ETH Public Perp V2 intrusion execution inputs', () => {
  test('reserves one stateful-order slot for the stop and caps the TP ladder', () => {
    const levels = Array.from({ length: 6 }, (_, index) => ({
      label: `L TP${index + 1}`,
      price: 70 + index
    }));

    expect(capTakeProfitsForStatefulOrderCapacity(levels, {
      limit: 20,
      openOrders: 18,
      marketOpenOrders: 0
    })).toMatchObject({
      requested: 6,
      allowed: 1,
      reservedStopSlots: 1,
      takeProfits: [{ label: 'L TP1', price: 70 }]
    });

    expect(capTakeProfitsForStatefulOrderCapacity(levels, {
      limit: 20,
      openOrders: 20,
      marketOpenOrders: 2
    })).toMatchObject({
      requested: 6,
      allowed: 1,
      reservedStopSlots: 1,
      takeProfits: [{ label: 'L TP1', price: 70 }]
    });
  });

  test('detects only a cohort increase inside the previous gap', () => {
    const payload = {
      frames: [
        { i: 0, t: '2026-08-01 10:00:00', price: 3_000 },
        { i: 1, t: '2026-08-01 11:00:00', price: 3_030 }
      ],
      gaps: [replicaGap(2_800, 3_200), replicaGap(2_850, 3_200)],
      zoneSeed: [
        ['L', 10, 2_800, 4],
        ['S', 10, 3_200, 3]
      ],
      zoneDeltas: [
        [],
        [['L', 10, 2_900, 1]]
      ]
    };

    const result = reconstructReplicaIntrusions(payload, '2026-08-01 10:00:00');

    expect(result.alerts).toHaveLength(1);
    expect(result.alerts[0].left).toHaveLength(1);
    expect(result.alerts[0].right).toHaveLength(0);
    expect(result.alerts[0].left[0]).toMatchObject({
      side: 'L',
      leverage: 10,
      price: 2_900,
      newCount: 1,
      gapSide: 'left'
    });
  });

  test('does not treat removals or unchanged cohorts as intrusions', () => {
    const payload = {
      frames: [
        { i: 0, t: '2026-08-01 10:00:00', price: 3_000 },
        { i: 1, t: '2026-08-01 11:00:00', price: 3_030 }
      ],
      gaps: [replicaGap(2_800, 3_200), replicaGap(2_800, 3_200)],
      zoneSeed: [
        ['L', 10, 2_900, 2],
        ['L', 10, 2_800, 4],
        ['S', 10, 3_200, 3]
      ],
      zoneDeltas: [[], [['L', 10, 2_900, 1]]]
    };

    expect(reconstructReplicaIntrusions(payload, '2026-08-01 10:00:00').alerts).toEqual([]);
  });

  test('uses the opposite gap edge as TP1 and never returns more than six targets', () => {
    const previousBuffer = process.env.DECENTRADER_TP1_EDGE_FRONT_RUN_USD;
    const previousMax = process.env.DECENTRADER_TP_MAX_LEVELS;
    const previousSpacing = process.env.DECENTRADER_TP_MIN_SPACING_PCT;
    process.env.DECENTRADER_TP1_EDGE_FRONT_RUN_USD = '5';
    process.env.DECENTRADER_TP_MAX_LEVELS = '6';
    process.env.DECENTRADER_TP_MIN_SPACING_PCT = '0.005';
    try {
      const bar = (side: 'L' | 'S', price: number, count: number, leverage = 10): LiquidityBar => ({
        key: `${side}|${leverage}|${price}`,
        side,
        leverage,
        price,
        count
      });
      const bars = [
        bar('L', 2_800, 8),
        bar('S', 3_200, 7),
        bar('S', 3_300, 12),
        bar('S', 3_400, 9),
        bar('S', 3_500, 15),
        bar('S', 3_600, 10),
        bar('S', 3_700, 13),
        bar('S', 3_800, 8),
        bar('L', 2_700, 11),
        bar('L', 2_600, 14),
        bar('L', 2_500, 9)
      ];
      const gap: Gap = {
        left: 2_800,
        right: 3_200,
        width: 400,
        price: 3_000,
        leftEdge: bar('L', 2_800, 8),
        rightEdge: bar('S', 3_200, 7),
        leftToPrice: 200,
        rightToPrice: 200
      };

      const zones = buildReplicaTradeZones(bars, 3_000, gap);

      expect(zones.longTp[0]).toMatchObject({ price: 3_195, edge: true, edgePrice: 3_200 });
      expect(zones.shortTp[0]).toMatchObject({ price: 2_805, edge: true, edgePrice: 2_800 });
      expect(zones.longTp.length).toBeLessThanOrEqual(6);
      expect(zones.shortTp.length).toBeLessThanOrEqual(6);
    } finally {
      if (previousBuffer === undefined) delete process.env.DECENTRADER_TP1_EDGE_FRONT_RUN_USD;
      else process.env.DECENTRADER_TP1_EDGE_FRONT_RUN_USD = previousBuffer;
      if (previousMax === undefined) delete process.env.DECENTRADER_TP_MAX_LEVELS;
      else process.env.DECENTRADER_TP_MAX_LEVELS = previousMax;
      if (previousSpacing === undefined) delete process.env.DECENTRADER_TP_MIN_SPACING_PCT;
      else process.env.DECENTRADER_TP_MIN_SPACING_PCT = previousSpacing;
    }
  });

  test('supports low-priced INJ zones and keeps live execution explicitly opt-in', () => {
    const previousAutoTrade = process.env.OPEN_LIQUIDITY_V2_INJ_AUTO_TRADE_ENABLED;
    delete process.env.OPEN_LIQUIDITY_V2_INJ_AUTO_TRADE_ENABLED;
    try {
      const bar = (side: 'L' | 'S', price: number, count: number, leverage = 10): LiquidityBar => ({
        key: `${side}|${leverage}|${price}`,
        side,
        leverage,
        price,
        count
      });
      const gap: Gap = {
        left: 4.5,
        right: 5.5,
        width: 1,
        price: 5,
        leftEdge: bar('L', 4.5, 8),
        rightEdge: bar('S', 5.5, 7),
        leftToPrice: 0.5,
        rightToPrice: 0.5
      };
      const zones = buildReplicaTradeZones([
        gap.leftEdge,
        gap.rightEdge,
        bar('S', 5.8, 12),
        bar('S', 6.2, 9),
        bar('L', 4.2, 11),
        bar('L', 3.8, 14)
      ], 5, gap, {
        priceStep: 0.01,
        edgeBufferEnv: 'OPEN_LIQUIDITY_V2_INJ_TP1_EDGE_FRONT_RUN_USD'
      });

      expect(zones.longTp[0]).toMatchObject({ edge: true, edgePrice: 5.5 });
      expect(zones.shortTp[0]).toMatchObject({ edge: true, edgePrice: 4.5 });
      expect(zones.longTp[0].price).toBeGreaterThan(5);
      expect(zones.shortTp[0].price).toBeLessThan(5);
      expect(zones.longTp.length).toBeLessThanOrEqual(6);
      expect(zones.shortTp.length).toBeLessThanOrEqual(6);
      expect(openLiquidityV2InjTradeMonitor.getStatus()).toMatchObject({
        market: 'INJ-USD',
        autoTradeEnabled: false
      });
    } finally {
      if (previousAutoTrade === undefined) delete process.env.OPEN_LIQUIDITY_V2_INJ_AUTO_TRADE_ENABLED;
      else process.env.OPEN_LIQUIDITY_V2_INJ_AUTO_TRADE_ENABLED = previousAutoTrade;
    }
  });

  test('requires explicit opt-in before the Gold monitor can trade PAXG-USD', () => {
    const previousAutoTrade = process.env.OPEN_LIQUIDITY_V2_GOLD_AUTO_TRADE_ENABLED;
    try {
      delete process.env.OPEN_LIQUIDITY_V2_GOLD_AUTO_TRADE_ENABLED;
      expect(openLiquidityV2GoldIntrusionMonitor.getStatus()).toMatchObject({
        market: 'PAXG-USD',
        autoTradeEnabled: false,
        observeOnly: false
      });

      process.env.OPEN_LIQUIDITY_V2_GOLD_AUTO_TRADE_ENABLED = 'true';
      expect(openLiquidityV2GoldIntrusionMonitor.getStatus()).toMatchObject({
        market: 'PAXG-USD',
        autoTradeEnabled: true,
        observeOnly: false,
        intrusionCandleFilter: {
          source: 'binance-futures',
          symbol: 'XAUUSDT'
        }
      });
    } finally {
      if (previousAutoTrade === undefined) delete process.env.OPEN_LIQUIDITY_V2_GOLD_AUTO_TRADE_ENABLED;
      else process.env.OPEN_LIQUIDITY_V2_GOLD_AUTO_TRADE_ENABLED = previousAutoTrade;
    }
  });

  test('runs Silver on XAG-USD and inherits the shared live-trading switch', () => {
    const previousShared = process.env.DECENTRADER_AUTO_TRADE_ENABLED;
    const previousSilver = process.env.OPEN_LIQUIDITY_V2_SILVER_AUTO_TRADE_ENABLED;
    const previousGlobalBuffer = process.env.DECENTRADER_TP1_EDGE_FRONT_RUN_USD;
    const previousSilverBuffer = process.env.OPEN_LIQUIDITY_V2_SILVER_TP1_EDGE_FRONT_RUN_USD;
    try {
      process.env.DECENTRADER_AUTO_TRADE_ENABLED = 'true';
      delete process.env.OPEN_LIQUIDITY_V2_SILVER_AUTO_TRADE_ENABLED;
      process.env.DECENTRADER_TP1_EDGE_FRONT_RUN_USD = '50';
      delete process.env.OPEN_LIQUIDITY_V2_SILVER_TP1_EDGE_FRONT_RUN_USD;
      expect(openLiquidityV2SilverIntrusionMonitor.getStatus()).toMatchObject({
        market: 'XAG-USD',
        autoTradeEnabled: true,
        observeOnly: false,
        intrusionCandleFilter: {
          source: 'binance-futures',
          symbol: 'XAGUSDT'
        }
      });

      const bar = (side: 'L' | 'S', price: number, count: number): LiquidityBar => ({
        key: `${side}|10|${price}`,
        side,
        leverage: 10,
        price,
        count
      });
      const gap: Gap = {
        left: 63.1,
        right: 69.3,
        width: 6.2,
        price: 65,
        leftEdge: bar('L', 63.1, 8),
        rightEdge: bar('S', 69.3, 7),
        leftToPrice: 1.9,
        rightToPrice: 4.3
      };
      const zones = buildReplicaTradeZones([gap.leftEdge, gap.rightEdge], 65, gap, {
        priceStep: 0.1,
        edgeBufferEnv: 'OPEN_LIQUIDITY_V2_SILVER_TP1_EDGE_FRONT_RUN_USD'
      });
      expect(zones.longTp[0]).toMatchObject({ price: 69.2, edge: true, edgePrice: 69.3 });
      expect(zones.shortTp[0]).toMatchObject({ price: 63.2, edge: true, edgePrice: 63.1 });

      process.env.OPEN_LIQUIDITY_V2_SILVER_AUTO_TRADE_ENABLED = 'false';
      expect(openLiquidityV2SilverIntrusionMonitor.getStatus().autoTradeEnabled).toBe(false);
    } finally {
      if (previousShared === undefined) delete process.env.DECENTRADER_AUTO_TRADE_ENABLED;
      else process.env.DECENTRADER_AUTO_TRADE_ENABLED = previousShared;
      if (previousSilver === undefined) delete process.env.OPEN_LIQUIDITY_V2_SILVER_AUTO_TRADE_ENABLED;
      else process.env.OPEN_LIQUIDITY_V2_SILVER_AUTO_TRADE_ENABLED = previousSilver;
      if (previousGlobalBuffer === undefined) delete process.env.DECENTRADER_TP1_EDGE_FRONT_RUN_USD;
      else process.env.DECENTRADER_TP1_EDGE_FRONT_RUN_USD = previousGlobalBuffer;
      if (previousSilverBuffer === undefined) delete process.env.OPEN_LIQUIDITY_V2_SILVER_TP1_EDGE_FRONT_RUN_USD;
      else process.env.OPEN_LIQUIDITY_V2_SILVER_TP1_EDGE_FRONT_RUN_USD = previousSilverBuffer;
    }
  });
});
