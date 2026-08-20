import { DydxV4Client } from '../src/services/dydx_v4/dydxV4Client';

const visibleTakeProfit = (clientId: number, triggerPrice: number, size: number) => ({
  id: String(clientId),
  clientId,
  market: 'BTC-USD',
  status: 'UNTRIGGERED',
  type: 'TAKE_PROFIT_MARKET',
  side: 'SELL',
  reduceOnly: true,
  triggerPrice,
  price: triggerPrice * 0.99,
  size: String(size),
  orderFlags: '32',
  goodTilBlockTime: 1900000000
});

describe('dYdX v4 take-profit replacement recovery', () => {
  test('retries cancellation when the indexer still exposes one old TP', async () => {
    const client = new DydxV4Client() as any;
    const initial = [
      visibleTakeProfit(101, 80000, 0.0002),
      visibleTakeProfit(102, 95950, 0.0005)
    ];
    const remaining = [visibleTakeProfit(102, 95950, 0.0005)];

    client.cancelSpecificOrders = jest.fn().mockResolvedValue(undefined);
    client.waitForTakeProfitsCleared = jest.fn()
      .mockResolvedValueOnce(remaining)
      .mockResolvedValueOnce([]);

    const result = await client.cancelTakeProfitsWithRetry('BTC-USD', initial);

    expect(result).toEqual([]);
    expect(client.cancelSpecificOrders).toHaveBeenNthCalledWith(1, 'BTC-USD', initial);
    expect(client.cancelSpecificOrders).toHaveBeenNthCalledWith(2, 'BTC-USD', remaining);
  });

  test('matches one remaining live TP to its exact desired ladder level', () => {
    const client = new DydxV4Client() as any;
    const remainingOrder = visibleTakeProfit(102, 95950, 0.0005);
    const levels = [
      { name: 'L TP1', price: 80000, size: 0.0002 },
      { name: 'L TP2', price: 95950, size: 0.0005 },
      { name: 'L TP3', price: 99950, size: 0.0002 },
      { name: 'L TP4', price: 119950, size: 0.0002 },
      { name: 'L TP5', price: 124900, size: 0.0002 }
    ];

    const result = client.matchTakeProfitOrderSubset(
      [remainingOrder],
      levels,
      'BTC-USD',
      { stepSize: '0.0001', minOrderSize: '0.0001' }
    );

    expect(result.unmatchedOrders).toEqual([]);
    expect(result.matched).toHaveLength(1);
    expect(result.matched[0].level).toMatchObject({ name: 'L TP2', price: 95950 });
    expect(result.unmatchedLevels.map((level: any) => level.price)).toEqual([
      80000,
      99950,
      119950,
      124900
    ]);
  });

  test('does not adopt a stale TP with a mismatching size', () => {
    const client = new DydxV4Client() as any;
    const staleOrder = visibleTakeProfit(102, 95950, 0.0004);

    const result = client.matchTakeProfitOrderSubset(
      [staleOrder],
      [{ name: 'L TP2', price: 95950, size: 0.0005 }],
      'BTC-USD',
      { stepSize: '0.0001', minOrderSize: '0.0001' }
    );

    expect(result.matched).toEqual([]);
    expect(result.unmatchedOrders).toEqual([staleOrder]);
  });

  test('restores only missing levels when one exact desired TP survives cancellation', async () => {
    const client = new DydxV4Client() as any;
    const remainingOrder = visibleTakeProfit(102, 95950, 0.0005);
    const levels = [
      { name: 'L TP1', price: 80000, size: 0.0002 },
      { name: 'L TP2', price: 95950, size: 0.0005 },
      { name: 'L TP3', price: 99950, size: 0.0002 },
      { name: 'L TP4', price: 119950, size: 0.0002 },
      { name: 'L TP5', price: 124900, size: 0.0002 }
    ];

    client.getCurrentPosition = jest.fn().mockResolvedValue({ size: 0.0013 });
    client.getTargetSize = jest.fn().mockReturnValue(0.0013);
    client.getExplicitTakeProfitLevels = jest.fn().mockReturnValue(levels);
    client.getMarketInfoBestEffort = jest.fn().mockResolvedValue({
      oraclePrice: '71900',
      stepSize: '0.0001',
      minOrderSize: '0.0001'
    });
    client.getMarketInfoReferencePrice = jest.fn().mockReturnValue(71900);
    client.getOpenOrdersForMarket = jest.fn().mockResolvedValue([remainingOrder]);
    client.cancelTakeProfitsWithRetry = jest.fn().mockResolvedValue([remainingOrder]);
    client.setManagedTakeProfits = jest.fn();
    client.placeExplicitTakeProfitsAfterEntry = jest.fn().mockResolvedValue(undefined);

    const result = await client.syncTakeProfitsForMarket('BTC-USD', {
      market: 'BTC-USD',
      desired_position: 'LONG',
      size: 0.0013,
      take_profits: levels
    });

    expect(result.outcome).toBe('RECOVERED');
    expect(result.adoptedTakeProfitCount).toBe(1);
    expect(result.restoredTakeProfitCount).toBe(4);
    expect(client.setManagedTakeProfits).toHaveBeenCalledWith(
      'BTC-USD',
      [expect.objectContaining({ clientId: 102, levelName: 'L TP2', size: 0.0005 })]
    );
    expect(client.placeExplicitTakeProfitsAfterEntry).toHaveBeenCalledWith(
      'BTC-USD',
      0.0013,
      expect.anything(),
      expect.arrayContaining([
        expect.objectContaining({ name: 'L TP1', price: 80000 }),
        expect.objectContaining({ name: 'L TP3', price: 99950 })
      ])
    );
  });
});
