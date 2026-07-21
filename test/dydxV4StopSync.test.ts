import { DydxV4Client } from '../src/services/dydx_v4/dydxV4Client';

const alert = {
  market: 'BTC-USD',
  desired_position: 'LONG',
  size: 0.001,
  trail_stop: 61242,
  price: 64424
} as any;

const managedStop = {
  market: 'BTC-USD',
  side: 'SELL',
  triggerPrice: 61242,
  clientId: 101,
  size: 0.001,
  source: 'TRAIL',
  updatedAt: Date.now() - 60 * 60 * 1000,
  goodTilBlockTime: 1900000000
};

const visibleStop = (clientId: number) => ({
  id: String(clientId),
  clientId,
  market: 'BTC-USD',
  status: 'UNTRIGGERED',
  type: 'STOP_MARKET',
  side: 'SELL',
  reduceOnly: true,
  triggerPrice: 61242,
  size: '0.001',
  goodTilBlockTime: 1900000000
});

describe('dYdX v4 trailing stop synchronization', () => {
  test('loads active conditional orders independently from paginated order history', async () => {
    const client = new DydxV4Client() as any;
    const getSubaccountOrders = jest.fn().mockImplementation(
      (_address, _subaccount, _ticker, _tickerType, _side, status) => {
        if (status === 'UNTRIGGERED') {
          return Promise.resolve({
            orders: [
              { ...visibleStop(101), ticker: 'BTC-USD', market: undefined },
              { ...visibleStop(102), ticker: 'BTC-USD', market: undefined }
            ]
          });
        }
        return Promise.resolve({ orders: [] });
      }
    );
    client.wallet = { address: 'dydx1test' };
    client.indexer = { account: { getSubaccountOrders } };

    const orders = await client.getOpenOrdersForMarket('BTC-USD');

    expect(orders.map((order: any) => order.clientId)).toEqual([101, 102]);
    expect(getSubaccountOrders).toHaveBeenCalledTimes(3);
    expect(getSubaccountOrders.mock.calls.map((call: any[]) => call[5])).toEqual([
      'UNTRIGGERED',
      'OPEN',
      'BEST_EFFORT_OPENED'
    ]);
    expect(getSubaccountOrders.mock.calls.every((call: any[]) => call[4] === undefined)).toBe(true);
    expect(getSubaccountOrders.mock.calls.every((call: any[]) => call[6] === undefined)).toBe(true);
  });

  test('parses indexer ISO good-til time for conditional order cancellation', () => {
    const client = new DydxV4Client() as any;
    const iso = '2026-08-05T23:18:35.000Z';

    expect(client.getOrderGoodTilBlockTime({ goodTilBlockTime: iso })).toBe(
      Math.floor(Date.parse(iso) / 1000)
    );
  });

  test('cancels an indexer conditional order with its parsed ISO expiry', async () => {
    const client = new DydxV4Client() as any;
    const iso = '2026-08-05T23:18:35.000Z';
    client.cancelOrderByFlags = jest.fn().mockResolvedValue(undefined);

    await client.cancelSpecificOrders('BTC-USD', [{
      ...visibleStop(1128172821),
      ticker: 'BTC-USD',
      market: undefined,
      orderFlags: '32',
      goodTilBlockTime: iso
    }]);

    expect(client.cancelOrderByFlags).toHaveBeenCalledWith(
      'BTC-USD',
      1128172821,
      32,
      undefined,
      Math.floor(Date.parse(iso) / 1000)
    );
  });

  test('does not replace an exact managed stop when the indexer cannot see it', async () => {
    const client = new DydxV4Client() as any;
    client.getCurrentPosition = jest.fn().mockResolvedValue({ size: 0.001 });
    client.getOpenOrdersForMarket = jest.fn().mockResolvedValue([]);
    client.placeSafetyStopOrder = jest.fn();
    client.cancelManagedStopBestEffort = jest.fn();
    client.managedStops.set('BTC-USD', { ...managedStop });

    const result = await client.syncTrailingStop(alert);

    expect(result.outcome).toBe('UNCHANGED');
    expect(result.visibility).toBe('RENDER_MEMORY_UNCONFIRMED');
    expect(client.placeSafetyStopOrder).not.toHaveBeenCalled();
    expect(client.cancelManagedStopBestEffort).not.toHaveBeenCalled();
  });

  test('keeps one exact managed stop and cleans every visible duplicate', async () => {
    const client = new DydxV4Client() as any;
    client.getCurrentPosition = jest.fn().mockResolvedValue({ size: 0.001 });
    client.getOpenOrdersForMarket = jest.fn().mockResolvedValue([
      visibleStop(101),
      visibleStop(102),
      visibleStop(103)
    ]);
    client.placeSafetyStopOrder = jest.fn();
    client.cancelOtherProtectiveStopsBestEffort = jest.fn().mockResolvedValue([]);
    client.managedStops.set('BTC-USD', { ...managedStop });

    const result = await client.syncTrailingStop(alert);

    expect(result.outcome).toBe('UNCHANGED');
    expect(client.placeSafetyStopOrder).not.toHaveBeenCalled();
    expect(client.cancelOtherProtectiveStopsBestEffort).toHaveBeenCalledWith(
      'BTC-USD',
      expect.anything(),
      101,
      expect.stringContaining('cleaning up')
    );
  });

  test('adopts a matching visible stop when the remembered client id is absent', async () => {
    const client = new DydxV4Client() as any;
    client.getCurrentPosition = jest.fn().mockResolvedValue({ size: 0.001 });
    client.getOpenOrdersForMarket = jest.fn().mockResolvedValue([
      visibleStop(102),
      visibleStop(103)
    ]);
    client.placeSafetyStopOrder = jest.fn();
    client.saveManagedOrdersState = jest.fn();
    client.cancelOtherProtectiveStopsBestEffort = jest.fn().mockResolvedValue([]);
    client.managedStops.set('BTC-USD', { ...managedStop });

    const result = await client.syncTrailingStop(alert);

    expect(result.outcome).toBe('UNCHANGED');
    expect(client.placeSafetyStopOrder).not.toHaveBeenCalled();
    expect(client.managedStops.get('BTC-USD').clientId).toBe(102);
    expect(client.cancelOtherProtectiveStopsBestEffort).toHaveBeenCalledWith(
      'BTC-USD',
      expect.anything(),
      102,
      expect.stringContaining('cleaning up')
    );
  });
});
