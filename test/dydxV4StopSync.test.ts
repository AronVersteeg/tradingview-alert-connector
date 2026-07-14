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
