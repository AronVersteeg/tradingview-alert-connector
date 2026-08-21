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

  test('loads active conditional orders from the current bare-array indexer response', async () => {
    const client = new DydxV4Client() as any;
    const getSubaccountOrders = jest.fn().mockImplementation(
      (_address, _subaccount, _ticker, _tickerType, _side, status) =>
        Promise.resolve(status === 'UNTRIGGERED'
          ? [visibleStop(101), visibleStop(102)]
          : [])
    );
    client.wallet = { address: 'dydx1test' };
    client.indexer = { account: { getSubaccountOrders } };

    const orders = await client.getOpenOrdersForMarket('BTC-USD');

    expect(orders.map((order: any) => order.clientId)).toEqual([101, 102]);
    expect(getSubaccountOrders).toHaveBeenCalledTimes(3);
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

  test('cancels a conditional order with its exact absolute expiry', async () => {
    const client = new DydxV4Client() as any;
    const expiry = Math.floor(Date.parse('2026-09-19T04:52:58.000Z') / 1000);
    const cancelRawOrder = jest.fn().mockResolvedValue(undefined);
    const cancelOrder = jest.fn();
    client.client = { cancelRawOrder, cancelOrder };
    client.subaccount = { address: 'dydx1test', subaccountNumber: 0 };
    client.getMarketInfoBestEffort = jest.fn().mockResolvedValue({ clobPairId: '0' });

    await client.cancelOrderByFlags('BTC-USD', 3129379048, 32, undefined, expiry);

    expect(cancelRawOrder).toHaveBeenCalledWith(
      client.subaccount,
      3129379048,
      32,
      0,
      0,
      expiry
    );
    expect(cancelOrder).not.toHaveBeenCalled();
  });

  test('keeps the human-readable cancel route for short-term orders', async () => {
    const client = new DydxV4Client() as any;
    const cancelRawOrder = jest.fn();
    const cancelOrder = jest.fn().mockResolvedValue(undefined);
    client.client = { cancelRawOrder, cancelOrder };
    client.subaccount = { address: 'dydx1test', subaccountNumber: 0 };

    await client.cancelOrderByFlags('BTC-USD', 123, 0, 456, undefined);

    expect(cancelOrder).toHaveBeenCalledWith(
      client.subaccount,
      123,
      0,
      'BTC-USD',
      456,
      undefined
    );
    expect(cancelRawOrder).not.toHaveBeenCalled();
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

  test('replaces a legacy wick stop when the new buffered trigger is 0.1% away', async () => {
    const client = new DydxV4Client() as any;
    const legacyTrigger = 71059.1436939;
    const bufferedTrigger = 70988.94;
    const legacyOrder = {
      ...visibleStop(101),
      triggerPrice: legacyTrigger,
      size: '0.0013'
    };
    client.getCurrentPosition = jest.fn().mockResolvedValue({ size: 0.0013 });
    client.getOpenOrdersForMarket = jest.fn().mockResolvedValue([legacyOrder]);
    client.placeSafetyStopOrder = jest.fn().mockResolvedValue({
      clientId: 202,
      size: 0.0013,
      goodTilBlockTime: 1900000000
    });
    client.waitForSafetyStopVisibleBestEffort = jest.fn().mockResolvedValue(true);
    client.cancelSpecificOrders = jest.fn().mockResolvedValue(undefined);
    client.cancelOtherProtectiveStopsBestEffort = jest.fn().mockResolvedValue([]);
    client.cancelManagedStopBestEffort = jest.fn().mockResolvedValue(undefined);
    client.saveManagedOrdersState = jest.fn();
    client.managedStops.set('BTC-USD', {
      ...managedStop,
      triggerPrice: legacyTrigger,
      size: 0.0013
    });

    const result = await client.syncTrailingStop({
      ...alert,
      size: 0.0013,
      trail_stop: bufferedTrigger,
      price: 72623
    });

    expect(result.outcome).toBe('UPDATED');
    expect(client.placeSafetyStopOrder).toHaveBeenCalledWith(
      'BTC-USD',
      expect.anything(),
      0.0013,
      bufferedTrigger,
      expect.any(Number)
    );
    expect(client.cancelSpecificOrders).toHaveBeenCalledWith('BTC-USD', [legacyOrder]);
    expect(client.managedStops.get('BTC-USD').triggerPrice).toBe(bufferedTrigger);
  });

  test('frees the farthest TP before replacing a stop when all stateful slots are occupied', async () => {
    const client = new DydxV4Client() as any;
    const staleStop = {
      ...visibleStop(101),
      triggerPrice: 61242,
      size: '0.002'
    };
    const farthestTp = {
      id: 'tp-far',
      clientId: 303,
      market: 'BTC-USD',
      status: 'UNTRIGGERED',
      type: 'TAKE_PROFIT',
      side: 'SELL',
      reduceOnly: true,
      triggerPrice: 99950,
      size: '0.0002',
      goodTilBlockTime: 1900000000
    };
    client.initialized = true;
    client.getCurrentPosition = jest.fn().mockResolvedValue({ size: 0.001 });
    client.getOpenOrdersForMarket = jest.fn().mockResolvedValue([staleStop, farthestTp]);
    client.getStatefulOrderCapacity = jest.fn()
      .mockResolvedValueOnce({ limit: 20, openOrders: 20, marketOpenOrders: 2, availableSlots: 0 })
      .mockResolvedValueOnce({ limit: 20, openOrders: 19, marketOpenOrders: 1, availableSlots: 1 });
    client.sleep = jest.fn().mockResolvedValue(undefined);
    client.placeSafetyStopOrder = jest.fn().mockResolvedValue({
      clientId: 202,
      size: 0.001,
      goodTilBlockTime: 1900000000
    });
    client.waitForSafetyStopVisibleBestEffort = jest.fn().mockResolvedValue(true);
    client.cancelSpecificOrders = jest.fn().mockResolvedValue(undefined);
    client.cancelOtherProtectiveStopsBestEffort = jest.fn().mockResolvedValue([]);
    client.cancelManagedStopBestEffort = jest.fn().mockResolvedValue(undefined);
    client.saveManagedOrdersState = jest.fn();
    client.managedStops.set('BTC-USD', { ...managedStop, size: 0.002 });

    const result = await client.syncTrailingStop(alert);

    expect(result.outcome).toBe('UPDATED');
    expect(result.releasedCapacityOrder).toMatchObject({ type: 'FARTHEST_TAKE_PROFIT' });
    expect(client.cancelSpecificOrders).toHaveBeenNthCalledWith(1, 'BTC-USD', [farthestTp]);
    expect(client.placeSafetyStopOrder).toHaveBeenCalledWith(
      'BTC-USD',
      expect.anything(),
      0.001,
      61242,
      expect.any(Number)
    );
    expect(client.cancelSpecificOrders).toHaveBeenNthCalledWith(2, 'BTC-USD', [staleStop]);
  });
});
