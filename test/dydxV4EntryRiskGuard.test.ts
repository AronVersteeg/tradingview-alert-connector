import {
  assessEntryDepth,
  constrainEntryPrice,
  entryRiskLimit,
  findRiskCompatibleEntry
} from '../src/services/dydx_v4/entryRiskGuard';
import { DydxV4Client } from '../src/services/dydx_v4/dydxV4Client';

describe('managed entry stop-risk protection', () => {
  beforeEach(() => {
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'warn').mockImplementation(() => {});
  });
  afterEach(() => jest.restoreAllMocks());

  test('caps the historical INJ short at a budget-safe tick instead of accepting 5.176', () => {
    const limit = entryRiskLimit('SELL', 79.4, 5.386381, 15, 0.001);
    expect(limit.price).toBe(5.199);
    expect(79.4 * (5.386381 - limit.price)).toBeLessThanOrEqual(15);
    expect(constrainEntryPrice(5.04169, 'SELL', false, limit)).toBe(5.199);
    expect(() => assessEntryDepth({ bids: [{ price: 5.176, size: 100 }], asks: [{ price: 5.2, size: 100 }] }, 'SELL', 79.4, limit.price)).toThrow(/insufficient/);
  });

  test('rounds a long entry limit down and a short entry limit up', () => {
    expect(entryRiskLimit('BUY', 3, 90, 10, 0.1).price).toBe(93.3);
    expect(entryRiskLimit('SELL', 3, 110, 10, 0.1).price).toBe(106.7);
  });

  test('uses the minimum positive tick when a small short is safe across the complete price range', () => {
    expect(entryRiskLimit('SELL', 0.001, 2548.0455, 15, 0.1).price).toBe(0.1);
  });

  test('does not restrict protective reduce-only exits or orders on the opposite side', () => {
    const limit = entryRiskLimit('BUY', 1, 90, 10, 0.1);
    expect(constrainEntryPrice(120, 'BUY', true, limit)).toBe(120);
    expect(constrainEntryPrice(80, 'SELL', false, limit)).toBe(80);
    expect(constrainEntryPrice(99, 'BUY', false, limit)).toBe(99);
  });

  test('requires full depth within the price limit, not only an acceptable best quote', () => {
    const book = { bids: [['99', '10']], asks: [['101', '1'], ['100', '1'], ['102', '10']] };
    expect(assessEntryDepth(book, 'BUY', 2, 101).expectedFillPrice).toBe(100.5);
    expect(() => assessEntryDepth(book, 'BUY', 3, 101)).toThrow(/insufficient/);
    expect(() => assessEntryDepth({ bids: [], asks: [] }, 'BUY', 1, 101)).toThrow(/empty/);
  });

  test.each([
    {
      side: 'BUY' as const,
      stop: 90,
      book: { bids: [{ price: 104, size: 10 }], asks: [{ price: 105, size: 10 }] },
      expectedLimit: 106.6
    },
    {
      side: 'SELL' as const,
      stop: 110,
      book: { bids: [{ price: 95, size: 10 }], asks: [{ price: 96, size: 10 }] },
      expectedLimit: 93.4
    }
  ])('downsizes a $side entry to the largest risk-compatible market step', ({ side, stop, book, expectedLimit }) => {
    const result = findRiskCompatibleEntry(book, side, 1, stop, 10, 0.1, 0.1, side === 'BUY' ? 108 : 92);
    expect(result.downsized).toBe(true);
    expect(result.size).toBe(0.6);
    expect(result.limit.price).toBe(expectedLimit);
    expect(result.size * Math.abs(result.depth.expectedFillPrice - stop)).toBeLessThanOrEqual(10);
  });

  test('keeps the full requested size when it already fits the risk budget and book', () => {
    const result = findRiskCompatibleEntry(
      { bids: [{ price: 104, size: 10 }], asks: [{ price: 105, size: 10 }] },
      'BUY',
      1,
      90,
      16,
      0.1,
      0.1,
      108
    );
    expect(result.downsized).toBe(false);
    expect(result.size).toBe(1);
    expect(result.limit.price).toBe(106);
  });

  test('rejects when even the minimum dYdX size cannot fit inside the risk limit', () => {
    expect(() => findRiskCompatibleEntry(
      { bids: [{ price: 104, size: 10 }], asks: [{ price: 105, size: 10 }] },
      'BUY',
      1,
      90,
      1,
      0.1,
      0.1,
      108
    )).toThrow(/even after downsizing/);
  });

  test.each([NaN, Infinity, 0, -1])('rejects invalid risk budget %s', (budget) => {
    expect(() => entryRiskLimit('BUY', 1, 90, budget, 0.1)).toThrow();
  });

  function clientAndAlert() {
    const client = new DydxV4Client() as any;
    client.getMarketInfoBestEffort = jest.fn().mockResolvedValue({
      oraclePrice: 105,
      tickSize: '0.1',
      stepSize: 0.1
    });
    client.resolveCorrectionOrderPricing = jest.fn().mockResolvedValue({ price: 108, referencePrice: 105 });
    client.indexer = { markets: { getPerpetualMarketOrderbook: jest.fn().mockResolvedValue({
      bids: [{ price: 104, size: 10 }], asks: [{ price: 105, size: 10 }]
    }) } };
    client.cancelOpenOrders = jest.fn();
    client.reachTargetPositionOrFailsafeFlat = jest.fn().mockResolvedValue(undefined);
    const alert = { market: 'ETH-USD', desired_position: 'LONG', size: 1, price: 105,
      static_sl: 90, signal: 'LONG_ENTRY', profile: 'MANAGED', decentrader: { riskBudgetUsd: 10 } };
    return { client, alert };
  }

  test('rejects a quote that cannot support the minimum size before cancelling existing protection', async () => {
    const { client, alert } = clientAndAlert();
    alert.decentrader.riskBudgetUsd = 1;
    await expect(client.placeOrderForMarket('ETH-USD', alert)).rejects.toThrow(/even after downsizing/);
    expect(client.cancelOpenOrders).not.toHaveBeenCalled();
    expect(client.reachTargetPositionOrFailsafeFlat).not.toHaveBeenCalled();
  });

  test('uses the largest dynamically downsized target throughout execution', async () => {
    const { client, alert } = clientAndAlert();
    await expect(client.placeOrderForMarket('ETH-USD', alert)).resolves.toMatchObject({
      outcome: 'TARGET_FAILED_FLATTENED',
      market: 'ETH-USD',
      targetSize: 0.6
    });
    expect(client.reachTargetPositionOrFailsafeFlat).toHaveBeenCalledWith('ETH-USD', 0.6,
      expect.objectContaining({ entryRiskLimit: { side: 'BUY', price: 106.6 } }));
  });

  test('uses general sizing headroom while passing the full budget limit to every correction', async () => {
    const { client, alert } = clientAndAlert();
    alert.decentrader.riskBudgetUsd = 16;
    await expect(client.placeOrderForMarket('ETH-USD', alert)).resolves.toMatchObject({
      outcome: 'TARGET_FAILED_FLATTENED',
      market: 'ETH-USD',
      targetSize: 0.9
    });
    expect(client.reachTargetPositionOrFailsafeFlat).toHaveBeenCalledWith('ETH-USD', 0.9,
      expect.objectContaining({ entryRiskLimit: { side: 'BUY', price: 107.7 } }));
  });

  test('allows an explicit utilization override while preserving the full hard risk limit', async () => {
    const { client, alert } = clientAndAlert();
    alert.decentrader.riskBudgetUsd = 16;
    (alert.decentrader as any).entryRiskUtilization = 1;
    await expect(client.placeOrderForMarket('ETH-USD', alert)).resolves.toMatchObject({
      outcome: 'TARGET_FAILED_FLATTENED',
      market: 'ETH-USD',
      targetSize: 1
    });
    expect(client.reachTargetPositionOrFailsafeFlat).toHaveBeenCalledWith('ETH-USD', 1,
      expect.objectContaining({ entryRiskLimit: { side: 'BUY', price: 106 } }));
  });

  test('places managed protection for the smaller position actually filled', async () => {
    const { client, alert } = clientAndAlert();
    client.reachTargetPositionOrFailsafeFlat.mockResolvedValue(0.4);
    client.rebalanceStatefulOrderCapacity = jest.fn().mockResolvedValue(undefined);
    client.getExplicitTakeProfitLevels = jest.fn().mockReturnValue([]);
    client.placeStaticSafetyStopAfterEntry = jest.fn().mockResolvedValue({ triggerPrice: 90 });

    await expect(client.placeOrderForMarket('ETH-USD', alert)).resolves.toBeUndefined();

    expect(client.placeStaticSafetyStopAfterEntry).toHaveBeenCalledWith('ETH-USD', 0.4, alert);
  });

  test('retries an unfilled entry with a freshly reduced risk-compatible target', async () => {
    const client = new DydxV4Client() as any;
    client.MAX_ATTEMPTS = 2;
    client.TARGET_POLL_DELAY_MS = 0;
    client.POST_ORDER_SETTLE_MS = 0;
    client.sleep = jest.fn().mockResolvedValue(undefined);
    client.getCurrentSize = jest.fn().mockResolvedValue(0);
    client.placeCorrectionOrder = jest.fn().mockResolvedValue({
      market: 'ETH-USD', side: 'BUY', size: 1, price: 108, priceSource: 'test',
      slippagePct: 0.03, usedFallbackWorstPrice: false, goodTilBlockBuffer: 20,
      clientId: 1, reduceOnly: false, submittedAt: Date.now(), submitResult: { code: 1 }
    });
    client.waitForTargetProgress = jest.fn()
      .mockResolvedValueOnce({ kind: 'unchanged', currentSize: 0 })
      .mockResolvedValueOnce({ kind: 'target', currentSize: 0.5 });
    client.logOrderDiagnostics = jest.fn().mockResolvedValue(undefined);
    client.refreshRiskAdjustedEntryTarget = jest.fn().mockResolvedValue({
      targetSize: 0.5,
      priceReference: { source: 'risk-refresh', entryRiskLimit: { side: 'BUY', price: 110 } }
    });

    await expect(client.reachTargetPositionSafely('ETH-USD', 1, {
      source: 'risk', entryRiskLimit: { side: 'BUY', price: 106 }
    })).resolves.toBe(0.5);

    expect(client.placeCorrectionOrder).toHaveBeenNthCalledWith(
      2,
      'ETH-USD',
      'BUY',
      0.5,
      false,
      expect.objectContaining({ entryRiskLimit: { side: 'BUY', price: 110 } })
    );
  });

  test('a missing book or a breached stop blocks entry before cancellations', async () => {
    const { client, alert } = clientAndAlert();
    client.getMarketInfoBestEffort.mockResolvedValue({
      oraclePrice: 89,
      tickSize: '0.1',
      stepSize: 0.1
    });
    await expect(client.placeOrderForMarket('ETH-USD', alert)).rejects.toThrow(/no longer valid/);
    expect(client.cancelOpenOrders).not.toHaveBeenCalled();
    expect(client.indexer.markets.getPerpetualMarketOrderbook).not.toHaveBeenCalled();
  });
});
