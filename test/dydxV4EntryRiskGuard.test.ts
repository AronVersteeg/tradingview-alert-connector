import { assessEntryDepth, constrainEntryPrice, entryRiskLimit } from '../src/services/dydx_v4/entryRiskGuard';
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

  test.each([NaN, Infinity, 0, -1])('rejects invalid risk budget %s', (budget) => {
    expect(() => entryRiskLimit('BUY', 1, 90, budget, 0.1)).toThrow();
  });

  function clientAndAlert() {
    const client = new DydxV4Client() as any;
    client.getMarketInfoBestEffort = jest.fn().mockResolvedValue({ oraclePrice: 105, tickSize: '0.1' });
    client.resolveCorrectionOrderPricing = jest.fn().mockResolvedValue({ price: 108, referencePrice: 105 });
    client.indexer = { markets: { getPerpetualMarketOrderbook: jest.fn().mockResolvedValue({
      bids: [{ price: 104, size: 10 }], asks: [{ price: 105, size: 10 }]
    }) } };
    client.cancelOpenOrders = jest.fn();
    client.reachTargetPositionOrFailsafeFlat = jest.fn().mockResolvedValue(false);
    const alert = { market: 'ETH-USD', desired_position: 'LONG', size: 1, price: 105,
      static_sl: 90, signal: 'LONG_ENTRY', profile: 'MANAGED', decentrader: { riskBudgetUsd: 10 } };
    return { client, alert };
  }

  test('rejects a bad quote before cancelling existing protection or submitting an entry', async () => {
    const { client, alert } = clientAndAlert();
    await expect(client.placeOrderForMarket('ETH-USD', alert)).rejects.toThrow(/insufficient/);
    expect(client.cancelOpenOrders).not.toHaveBeenCalled();
    expect(client.reachTargetPositionOrFailsafeFlat).not.toHaveBeenCalled();
  });

  test('passes the budget limit to the target-position loop for every correction', async () => {
    const { client, alert } = clientAndAlert();
    alert.decentrader.riskBudgetUsd = 16;
    await expect(client.placeOrderForMarket('ETH-USD', alert)).resolves.toMatchObject({
      outcome: 'TARGET_FAILED_FLATTENED',
      market: 'ETH-USD',
      targetSize: 1
    });
    expect(client.reachTargetPositionOrFailsafeFlat).toHaveBeenCalledWith('ETH-USD', 1,
      expect.objectContaining({ entryRiskLimit: { side: 'BUY', price: 106 } }));
  });

  test('a missing book or a breached stop blocks entry before cancellations', async () => {
    const { client, alert } = clientAndAlert();
    client.getMarketInfoBestEffort.mockResolvedValue({ oraclePrice: 89, tickSize: '0.1' });
    await expect(client.placeOrderForMarket('ETH-USD', alert)).rejects.toThrow(/no longer valid/);
    expect(client.cancelOpenOrders).not.toHaveBeenCalled();
    expect(client.indexer.markets.getPerpetualMarketOrderbook).not.toHaveBeenCalled();
  });
});
