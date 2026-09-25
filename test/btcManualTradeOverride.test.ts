import fs from 'fs';
import os from 'os';
import path from 'path';

import {
  BtcManualTradeOverrideMonitor,
  BtcManualTradeOverrideStore,
  BtcManualTradeOverrideState,
  buildManualTakeProfitOrderLevels,
  btcManualTradeOverrideIsArmed,
  btcManualTriggerEmailSubject,
  manualTradeOverrideConfigForMarket,
  manualTradeOverrideIsArmed,
  matchingManualOverrideCandle,
  normalizeBtcManualTradeOverrideRequest,
  readBtcManualTradeOverrideStore,
  readManualTradeOverrideStore,
  recoverableManualOverrideRequest,
  upsertBtcManualTradeOverride
} from '../src/services/btcManualTradeOverride';

describe('BTC manual entry and TP override', () => {
  const now = Date.parse('2026-09-10T10:30:00.000Z');

  function store(overrides: BtcManualTradeOverrideState[] = []): BtcManualTradeOverrideStore {
    return {
      version: 3,
      market: 'BTC-USD',
      overrides,
      updatedAt: '2026-09-10T10:30:00.000Z'
    };
  }

  test.each([
    ['BTC-USD', 'BTCUSDT'],
    ['ETH-USD', 'ETHUSDT'],
    ['INJ-USD', 'INJUSDT'],
    ['SOL-USD', 'SOLUSDT'],
    ['ZEC-USD', 'ZECUSDT'],
    ['PAXG-USD', 'XAUUSDT'],
    ['XAG-USD', 'XAGUSDT']
  ])('configures %s manual triggers from %s closed candles', (market, symbol) => {
    expect(manualTradeOverrideConfigForMarket(market)).toMatchObject({ market, symbol });
  });

  test('arms a validated persistent long override', () => {
    const state = normalizeBtcManualTradeOverrideRequest({
      direction: 'long',
      closeTrigger: 80000,
      takeProfits: [
        { price: 81000, allocationPct: 60 },
        { price: 82500, allocationPct: 40 }
      ]
    }, now);

    expect(state).toMatchObject({
      market: 'BTC-USD',
      status: 'ARMED',
      direction: 'long',
      closeTrigger: 80000,
      armedAt: '2026-09-10T10:30:00.000Z'
    });
    expect(state.expiresAt).toBeUndefined();
  });

  test('builds the requested manual trigger email header', () => {
    expect(btcManualTriggerEmailSubject({
      market: 'BTC-USD',
      direction: 'long',
      signature: 'manual-test',
      signalCandleStartedAt: '2026-09-10T13:00:00.000Z',
      signalCandleClosedAt: '2026-09-10T13:59:59.999Z',
      signalClose: 80100,
      closeTrigger: 80000,
      takeProfits: []
    })).toBe('BTC MANUAL LONG TRIGGERED | 10-09-2026 15:00 NL');
  });

  test('uses the selected pair in the trigger email header', () => {
    expect(btcManualTriggerEmailSubject({
      market: 'PAXG-USD',
      direction: 'short',
      signature: 'manual-gold-test',
      signalCandleStartedAt: '2026-09-10T13:00:00.000Z',
      signalCandleClosedAt: '2026-09-10T13:59:59.999Z',
      signalClose: 4_500,
      closeTrigger: 4_550,
      takeProfits: []
    })).toBe('GOLD MANUAL SHORT TRIGGERED | 10-09-2026 15:00 NL');
  });

  test('keeps manual plans isolated per pair on the shared persistent-data directory', () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'pair-manual-storage-'));
    const previousBtcFile = process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE;
    const previousEthFile = process.env.ETH_MANUAL_TRADE_OVERRIDE_FILE;
    const previousEnabled = process.env.MANUAL_ENTRY_TP_OVERRIDE_ENABLED;
    process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE = path.join(directory, 'btc.json');
    delete process.env.ETH_MANUAL_TRADE_OVERRIDE_FILE;
    process.env.MANUAL_ENTRY_TP_OVERRIDE_ENABLED = 'true';
    try {
      const ethConfig = manualTradeOverrideConfigForMarket('ETH_USD')!;
      const ethMonitor = new BtcManualTradeOverrideMonitor(ethConfig);
      ethMonitor.arm({
        direction: 'long',
        closeTrigger: 3_000,
        takeProfits: [{ price: 3_200, allocationPct: 100 }]
      });

      expect(readManualTradeOverrideStore(ethConfig)).toMatchObject({
        market: 'ETH-USD',
        overrides: [expect.objectContaining({ market: 'ETH-USD', closeTrigger: 3_000 })]
      });
      expect(readBtcManualTradeOverrideStore().overrides).toEqual([]);
      expect(manualTradeOverrideIsArmed('ETH-USD')).toBe(true);
      expect(btcManualTradeOverrideIsArmed()).toBe(false);
      expect(fs.existsSync(path.join(directory, 'eth-manual-trade-override.json'))).toBe(true);
    } finally {
      if (previousBtcFile === undefined) delete process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE;
      else process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE = previousBtcFile;
      if (previousEthFile === undefined) delete process.env.ETH_MANUAL_TRADE_OVERRIDE_FILE;
      else process.env.ETH_MANUAL_TRADE_OVERRIDE_FILE = previousEthFile;
      if (previousEnabled === undefined) delete process.env.MANUAL_ENTRY_TP_OVERRIDE_ENABLED;
      else process.env.MANUAL_ENTRY_TP_OVERRIDE_ENABLED = previousEnabled;
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

  test('rejects TP allocations that do not total 100 percent', () => {
    expect(() => normalizeBtcManualTradeOverrideRequest({
      direction: 'long',
      closeTrigger: 80000,
      takeProfits: [
        { price: 81000, allocationPct: 50 },
        { price: 82000, allocationPct: 25 }
      ]
    }, now)).toThrow('must total 100%');
  });

  test('rejects take profits on the wrong side of the trigger', () => {
    expect(() => normalizeBtcManualTradeOverrideRequest({
      direction: 'short',
      closeTrigger: 78000,
      takeProfits: [{ price: 79000, allocationPct: 100 }]
    }, now)).toThrow('Every short TP must be below');
  });

  test('only accepts a matching candle that closed after arming', () => {
    const state: BtcManualTradeOverrideState = {
      version: 1,
      id: 'long',
      market: 'BTC-USD',
      status: 'ARMED',
      direction: 'long',
      closeTrigger: 80000,
      takeProfits: [],
      armedAt: '2026-09-10T10:30:00.000Z',
      expiresAt: '2026-09-10T10:45:00.000Z',
      updatedAt: '2026-09-10T10:30:00.000Z'
    };
    const candles = [
      { openTime: Date.parse('2026-09-10T09:00:00.000Z'), closeTime: Date.parse('2026-09-10T09:59:59.999Z'), close: '80500' },
      { openTime: Date.parse('2026-09-10T10:00:00.000Z'), closeTime: Date.parse('2026-09-10T10:59:59.999Z'), close: '79900' },
      { openTime: Date.parse('2026-09-10T11:00:00.000Z'), closeTime: Date.parse('2026-09-10T11:59:59.999Z'), close: '80100' }
    ];

    expect(matchingManualOverrideCandle(state, candles, Date.parse('2026-09-10T11:30:00.000Z'))).toBeUndefined();
    expect(matchingManualOverrideCandle(state, candles, Date.parse('2026-09-10T12:00:20.000Z'))?.close).toBe('80100');
  });

  test('does not execute an old matching close after service downtime', () => {
    const state: BtcManualTradeOverrideState = {
      version: 1,
      id: 'long',
      market: 'BTC-USD',
      status: 'ARMED',
      direction: 'long',
      closeTrigger: 80000,
      takeProfits: [],
      armedAt: '2026-09-10T10:30:00.000Z',
      expiresAt: '2026-09-11T10:30:00.000Z',
      updatedAt: '2026-09-10T10:30:00.000Z'
    };
    const candles = [
      { openTime: Date.parse('2026-09-10T11:00:00.000Z'), closeTime: Date.parse('2026-09-10T11:59:59.999Z'), close: '80100' }
    ];

    expect(matchingManualOverrideCandle(state, candles, Date.parse('2026-09-10T12:20:00.000Z'))).toBeUndefined();
  });

  test('only evaluates the newest closed candle', () => {
    const state: BtcManualTradeOverrideState = {
      version: 1,
      id: 'long',
      market: 'BTC-USD',
      status: 'ARMED',
      direction: 'long',
      closeTrigger: 80000,
      takeProfits: [],
      armedAt: '2026-09-10T10:30:00.000Z',
      expiresAt: '2026-09-11T10:30:00.000Z',
      updatedAt: '2026-09-10T10:30:00.000Z'
    };
    const candles = [
      { openTime: Date.parse('2026-09-10T11:00:00.000Z'), closeTime: Date.parse('2026-09-10T11:59:59.999Z'), close: '80100' },
      { openTime: Date.parse('2026-09-10T12:00:00.000Z'), closeTime: Date.parse('2026-09-10T12:59:59.999Z'), close: '79900' }
    ];

    expect(matchingManualOverrideCandle(state, candles, Date.parse('2026-09-10T13:00:20.000Z'))).toBeUndefined();
  });

  test('keeps multiple plans in the same direction plus the opposite plan', () => {
    const long = normalizeBtcManualTradeOverrideRequest({
      direction: 'long', closeTrigger: 70000,
      takeProfits: [{ price: 75000, allocationPct: 100 }]
    }, now);
    const secondLong = normalizeBtcManualTradeOverrideRequest({
      direction: 'long', closeTrigger: 80000,
      takeProfits: [{ price: 90000, allocationPct: 100 }]
    }, now + 500);
    const short = normalizeBtcManualTradeOverrideRequest({
      direction: 'short',
      closeTrigger: 69000,
      takeProfits: []
    }, now + 1000);

    const result = upsertBtcManualTradeOverride(
      upsertBtcManualTradeOverride(upsertBtcManualTradeOverride(store(), long), secondLong),
      short
    );

    expect(result.overrides.map((override) => [override.direction, override.closeTrigger])).toEqual([
      ['long', 70000],
      ['long', 80000],
      ['short', 69000]
    ]);
    expect(new Set(result.overrides.map((override) => override.id)).size).toBe(3);
    expect(result.overrides[0].takeProfits?.[0].price).toBe(75000);
    expect(result.overrides[1].takeProfits?.[0].price).toBe(90000);
  });

  test('edits one armed plan in place without changing another plan', () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'btc-manual-edit-'));
    const previousFile = process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE;
    const previousEnabled = process.env.MANUAL_ENTRY_TP_OVERRIDE_ENABLED;
    process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE = path.join(directory, 'state.json');
    process.env.MANUAL_ENTRY_TP_OVERRIDE_ENABLED = 'true';
    try {
      const monitor = new BtcManualTradeOverrideMonitor();
      const first = monitor.arm({
        direction: 'long', closeTrigger: 70000,
        takeProfits: [{ price: 75000, allocationPct: 100 }]
      });
      const second = monitor.arm({
        direction: 'long', closeTrigger: 80000,
        takeProfits: [{ price: 90000, allocationPct: 100 }]
      });

      const edited = monitor.update(first.id, {
        direction: 'long', closeTrigger: 72000,
        takeProfits: [{ price: 76000, allocationPct: 100 }]
      });
      const saved = readBtcManualTradeOverrideStore().overrides;

      expect(edited).toMatchObject({
        id: first.id,
        status: 'ARMED',
        closeTrigger: 72000,
        takeProfits: [{ price: 76000, allocationPct: 100 }]
      });
      expect(edited.expiresAt).toBeUndefined();
      expect(saved.find((plan) => plan.id === first.id)).toMatchObject({
        closeTrigger: 72000,
        takeProfits: [{ price: 76000, allocationPct: 100 }]
      });
      expect(saved.find((plan) => plan.id === second.id)).toMatchObject({
        closeTrigger: 80000,
        takeProfits: [{ price: 90000, allocationPct: 100 }]
      });
    } finally {
      if (previousFile === undefined) delete process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE;
      else process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE = previousFile;
      if (previousEnabled === undefined) delete process.env.MANUAL_ENTRY_TP_OVERRIDE_ENABLED;
      else process.env.MANUAL_ENTRY_TP_OVERRIDE_ENABLED = previousEnabled;
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

  test('keeps an existing armed plan active after its legacy expiry date', () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'btc-manual-no-expiry-'));
    const previousFile = process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE;
    process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE = path.join(directory, 'state.json');
    try {
      fs.writeFileSync(process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE, JSON.stringify(store([{
        version: 1,
        id: 'existing-long-plan',
        market: 'BTC-USD',
        status: 'ARMED',
        direction: 'long',
        closeTrigger: 80000,
        takeProfits: [],
        armedAt: '2026-09-10T10:30:00.000Z',
        expiresAt: '2026-09-11T10:30:00.000Z',
        updatedAt: '2026-09-10T10:30:00.000Z'
      }])), 'utf8');

      expect(readBtcManualTradeOverrideStore().overrides[0]).toMatchObject({
        id: 'existing-long-plan',
        status: 'ARMED'
      });
      expect(readBtcManualTradeOverrideStore().overrides[0].expiresAt).toBeUndefined();
      expect(btcManualTradeOverrideIsArmed()).toBe(true);
    } finally {
      if (previousFile === undefined) delete process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE;
      else process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE = previousFile;
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

  test('rejects overlapping long and short triggers', () => {
    const long = normalizeBtcManualTradeOverrideRequest({
      direction: 'long',
      closeTrigger: 78000,
      takeProfits: []
    }, now);
    const short = normalizeBtcManualTradeOverrideRequest({
      direction: 'short',
      closeTrigger: 79000,
      takeProfits: []
    }, now + 1000);

    expect(() => upsertBtcManualTradeOverride(upsertBtcManualTradeOverride(store(), long), short))
      .toThrow('Short close trigger must be below');
  });

  test('rejects a duplicate active trigger but does not replace another same-direction plan', () => {
    const long = normalizeBtcManualTradeOverrideRequest({
      direction: 'long',
      closeTrigger: 81000,
      takeProfits: []
    }, now);
    const anotherLong = normalizeBtcManualTradeOverrideRequest({
      direction: 'long',
      closeTrigger: 82000,
      takeProfits: []
    }, now + 1000);
    const duplicateLong = normalizeBtcManualTradeOverrideRequest({
      direction: 'long',
      closeTrigger: 81000,
      takeProfits: []
    }, now + 2000);
    const result = upsertBtcManualTradeOverride(
      upsertBtcManualTradeOverride(store(), long),
      anotherLong
    );

    expect(result.overrides.filter((override) => override.direction === 'long')).toHaveLength(2);
    expect(() => upsertBtcManualTradeOverride(result, duplicateLong)).toThrow('already uses close trigger');
  });

  test('allocates the exact manual TP ladder at dYdX step size', () => {
    const levels = buildManualTakeProfitOrderLevels('long', [
      { price: 81000, allocationPct: 50 },
      { price: 82000, allocationPct: 30 },
      { price: 83000, allocationPct: 20 }
    ], 0.01, 0.0001, 80050);

    expect(levels.map((level) => [level.price, level.size, level.manual_locked])).toEqual([
      [81000, 0.005, true],
      [82000, 0.003, true],
      [83000, 0.002, true]
    ]);
  });

  test('rejects a manual TP already behind the live entry price', () => {
    expect(() => buildManualTakeProfitOrderLevels(
      'long',
      [{ price: 80100, allocationPct: 100 }],
      0.01,
      0.0001,
      80200
    )).toThrow('not beyond the current long entry price');
  });

  test('keeps a matched plan armed without email when an existing BTC position defers entry', async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'btc-manual-override-'));
    const previousFile = process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE;
    process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE = path.join(directory, 'state.json');
    try {
      const state = normalizeBtcManualTradeOverrideRequest({
        direction: 'long', closeTrigger: 80000, takeProfits: []
      }, now);
      state.status = 'EXECUTING';
      const currentStore = store([state]);
      const sendEmail = jest.fn(async () => ({ sent: true }));
      const monitor = new BtcManualTradeOverrideMonitor();
      monitor.configureEntryHandler({
        executeManualBtcEntry: async () => ({
          tradePlaced: false,
          tradeDeferred: true,
          tradeSkipped: 'Existing BTC-USD position detected; manual plan remains armed.'
        }),
        sendManualBtcTriggerEmail: sendEmail
      });
      await (monitor as any).completeExecution(currentStore, state, {
        market: 'BTC-USD',
        direction: 'long',
        signature: 'manual-deferred-test',
        signalCandleStartedAt: '2026-09-10T11:00:00.000Z',
        signalCandleClosedAt: '2026-09-10T11:59:59.999Z',
        signalClose: 80100,
        closeTrigger: 80000,
        takeProfits: []
      });

      expect(readBtcManualTradeOverrideStore().overrides[0]).toMatchObject({
        id: state.id,
        status: 'ARMED',
        result: { tradeDeferred: true }
      });
      expect(sendEmail).not.toHaveBeenCalled();
    } finally {
      if (previousFile === undefined) delete process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE;
      else process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE = previousFile;
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

  test('keeps the opposite plan armed after a successful trigger', async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'btc-manual-opposite-'));
    const previousFile = process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE;
    process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE = path.join(directory, 'state.json');
    try {
      const long = normalizeBtcManualTradeOverrideRequest({
        direction: 'long', closeTrigger: 80000, takeProfits: []
      }, now);
      long.status = 'EXECUTING';
      const short = normalizeBtcManualTradeOverrideRequest({
        direction: 'short', closeTrigger: 78000, takeProfits: []
      }, now + 1000);
      const currentStore = store([long, short]);
      const monitor = new BtcManualTradeOverrideMonitor();
      monitor.configureEntryHandler({
        executeManualBtcEntry: async () => ({ tradePlaced: true }),
        sendManualBtcTriggerEmail: async () => ({ sent: true })
      });
      await (monitor as any).completeExecution(currentStore, long, {
        market: 'BTC-USD',
        direction: 'long',
        signature: 'manual-long-test',
        signalCandleStartedAt: '2026-09-10T11:00:00.000Z',
        signalCandleClosedAt: '2026-09-10T11:59:59.999Z',
        signalClose: 80100,
        closeTrigger: 80000,
        takeProfits: []
      });

      const saved = readBtcManualTradeOverrideStore().overrides;
      expect(saved.find((plan) => plan.id === long.id)?.status).toBe('TRIGGERED');
      expect(saved.find((plan) => plan.id === short.id)?.status).toBe('ARMED');
    } finally {
      if (previousFile === undefined) delete process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE;
      else process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE = previousFile;
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

  test('recovers one recent manual entry that was fail-safe flattened', () => {
    const state: BtcManualTradeOverrideState = {
      version: 1,
      id: 'short',
      market: 'BTC-USD',
      status: 'SKIPPED',
      direction: 'short',
      closeTrigger: 76000,
      takeProfits: [{ price: 62500, allocationPct: 100 }],
      armedAt: '2026-09-14T08:32:02.538Z',
      expiresAt: '2026-09-17T11:32:02.538Z',
      triggeredAt: '2026-09-15T15:00:20.002Z',
      signalCandleStartedAt: '2026-09-15T14:00:00.000Z',
      signalCandleClosedAt: '2026-09-15T14:59:59.999Z',
      signalClose: 75907,
      result: {
        signature: 'btc-manual-test',
        tradePlacement: { outcome: 'TARGET_FAILED_FLATTENED' }
      },
      updatedAt: '2026-09-15T15:04:19.822Z'
    };

    expect(recoverableManualOverrideRequest(state, Date.parse('2026-09-15T15:30:00.000Z')))
      .toMatchObject({ direction: 'short', signature: 'btc-manual-test', recovery: true });
    state.recoveryAttempts = 1;
    expect(recoverableManualOverrideRequest(state, Date.parse('2026-09-15T15:30:00.000Z')))
      .toBeUndefined();
  });
});
