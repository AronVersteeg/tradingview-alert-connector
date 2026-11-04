import fs from 'fs';
import os from 'os';
import path from 'path';
import axios from 'axios';
import * as gap from '../src/services/decentraderGapMonitor';
import * as v2 from '../src/services/openLiquidityV2EthTradeMonitor';
import { withManualDydxBodyQuarterPlan } from '../src/services/dydxSignalBodyStop';

const started = '2026-11-04T09:00:00.000Z', closed = '2026-11-04T10:00:00.000Z';
const now = Date.parse(closed) + 5000;
const templates: Record<string, any> = { ETH: v2.openLiquidityV2EthTradeMonitor, SOL: v2.openLiquidityV2SolTradeMonitor,
  INJ: v2.openLiquidityV2InjTradeMonitor, ZEC: v2.openLiquidityV2ZecTradeMonitor,
  GOLD: v2.openLiquidityV2GoldIntrusionMonitor, SILVER: v2.openLiquidityV2SilverIntrusionMonitor };
const marketFor = (asset: string) => templates[asset]?.config.market || 'BTC-USD';

function plan(asset = 'BTC', direction: 'long' | 'short' = 'long'): any {
  return { market: marketFor(asset), price: 120, timestamp: '2026-11-04 09:00:00',
    marketInfo: { status: 'ACTIVE', oraclePrice: 120, stepSize: .1 },
    stopReferenceCandle: { timestamp: started, fractalCandleSource: 'dydx-1h',
      openRef: direction === 'long' ? 114 : 126, closeRef: direction === 'long' ? 118 : 122,
      highRef: 150, lowRef: 90 },
    activePlan: { direction, status: 'ready',
      stop: { valid: true, price: direction === 'long' ? 100 : 140, buffer: .1, maxDistancePct: .3,
        fractal: { index: 10, timestamp: '2026-11-04T06:00:00.000Z', price: direction === 'long' ? 100 : 140, source: 'lowRef' } },
      sizing: { size: 1, minimumOrderSize: .1, notional: 120, riskBudgetUsd: 15, plannedStopRiskUsd: 15 },
      takeProfits: [{ price: direction === 'long' ? 140 : 100, label: 'Map TP1' }] } };
}
function request(asset = 'BTC', direction: 'long' | 'short' = 'long'): any {
  return { market: marketFor(asset), direction, signature: `manual-quarter-${asset}-${direction}`,
    signalCandleStartedAt: started, signalCandleClosedAt: closed, signalClose: 999,
    closeTrigger: 119, takeProfits: [{ price: direction === 'long' ? 140 : 100, allocationPct: 100 }] };
}

describe('manual same-hour dYdX quarter-body SL', () => {
  const oldEnv = { ...process.env };
  let directory: string;
  beforeEach(() => {
    jest.useFakeTimers(); jest.setSystemTime(now);
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'manual-body-quarter-'));
    Object.assign(process.env, { DECENTRADER_GAP_ALERT_STATE_FILE: path.join(directory, 'btc.json'),
      DECENTRADER_AUTO_TRADE_ENABLED: 'true', MANUAL_ENTRY_TP_OVERRIDE_ENABLED: 'true',
      DECENTRADER_TRADE_MARKET: 'BTC-USD', DECENTRADER_DYNAMIC_SL_ENABLED: 'true',
      DECENTRADER_DYNAMIC_SL_LIVE_UPDATES_ENABLED: 'true', DECENTRADER_DYNAMIC_TP_ENABLED: 'true',
      DECENTRADER_SL_FRACTAL_WINDOW: '2', DECENTRADER_SL_BUFFER_PCT: '.001', DECENTRADER_SL_LOOKBACK_BARS: '72',
      DECENTRADER_SL_MAX_DISTANCE_PCT: '.3', DECENTRADER_DYNAMIC_SL_FRACTAL_DELAY: '0' });
    for (const asset of Object.keys(templates)) Object.assign(process.env, {
      [`OPEN_LIQUIDITY_V2_${asset}_TRADE_STATE_FILE`]: path.join(directory, `${asset}.json`),
      [`OPEN_LIQUIDITY_V2_${asset}_AUTO_TRADE_ENABLED`]: 'true',
      [`OPEN_LIQUIDITY_V2_${asset}_GAP_DAILY_ENTRY_ENABLED`]: 'false'
    });
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'error').mockImplementation(() => {});
    jest.spyOn(console, 'warn').mockImplementation(() => {});
    jest.spyOn(axios, 'get').mockRejectedValue(new Error('Unexpected HTTP request in offline manual-stop test'));
  });
  afterEach(() => {
    jest.restoreAllMocks(); jest.useRealTimers();
    for (const key of Object.keys(process.env)) if (!(key in oldEnv)) delete process.env[key];
    Object.assign(process.env, oldEnv);
    fs.rmSync(directory, { recursive: true, force: true });
  });

  test.each(['long', 'short'] as const)('%s changes only initial stop; body uses dYdX rather than Binance signalClose or wicks', direction => {
    const original = plan('BTC', direction), before = JSON.stringify(original);
    const actual = withManualDydxBodyQuarterPlan(original, request('BTC', direction), 'BTC');
    expect(actual.activePlan.stop.price).toBe(direction === 'long' ? 115 : 125);
    expect(actual.activePlan.sizing).toBe(original.activePlan.sizing);
    expect(actual.activePlan.takeProfits).toBe(original.activePlan.takeProfits);
    expect(actual.activePlan.sizingStop).toEqual(original.activePlan.stop);
    expect(actual.activePlan.stop.fractal).toEqual(original.activePlan.stop.fractal);
    expect(actual.activePlan.initialStopPolicy).toMatchObject({ mode: 'signal-body-quarter', bodyFraction: .25,
      candleSource: 'dydx-1h', candleStartedAt: started, candleClosedAt: closed });
    expect(JSON.stringify(original)).toBe(before);
  });
  test.each([closed, '2026-11-04T09:59:59.999Z'])('normalizes closed-hour identity: %s', signalCandleClosedAt => {
    expect(withManualDydxBodyQuarterPlan(plan(), { ...request(), signalCandleClosedAt }, 'BTC').activePlan.stop.price).toBe(115);
  });
  test.each(['wrong-hour', 'missing', 'wrong-market', 'invalid-close', 'future', 'wrong-source', 'crossed', 'no-fractal'])('refuses unsafe %s inputs', kind => {
    const p = plan(), r = request();
    if (kind === 'wrong-hour') p.stopReferenceCandle.timestamp = '2026-11-04T08:00:00Z';
    if (kind === 'missing') delete p.stopReferenceCandle;
    if (kind === 'wrong-market') r.market = 'SOL-USD';
    if (kind === 'invalid-close') r.signalCandleClosedAt = '2026-11-04T10:30:00Z';
    if (kind === 'future') jest.setSystemTime(Date.parse(closed) - 1);
    if (kind === 'wrong-source') p.stopReferenceCandle.fractalCandleSource = 'binance-spot';
    if (kind === 'crossed') p.marketInfo.oraclePrice = 114;
    if (kind === 'no-fractal') delete p.activePlan.stop.fractal;
    expect(() => withManualDydxBodyQuarterPlan(p, r, 'BTC')).toThrow();
  });

  describe.each(['BTC', ...Object.keys(templates)])('%s manual execution', asset => {
    function setup(direction: 'long' | 'short' = 'long') {
      const market = marketFor(asset), p = plan(asset, direction);
      let position: any;
      const orders = { getAccountSnapshot: jest.fn(async () => ({ markets: { [market]: p.marketInfo },
        openPositions: position ? [position] : [] })),
        placeOrder: jest.fn(async () => { position = { market, size: direction === 'long' ? 1 : -1, entryPrice: 120 }; }),
        syncTrailingStop: jest.fn().mockResolvedValue({ outcome: 'UNCHANGED' }),
        syncTakeProfits: jest.fn().mockResolvedValue({ outcome: 'UNCHANGED' }) };
      const monitor: any = asset === 'BTC' ? new gap.DecentraderGapMonitor()
        : new v2.OpenLiquidityV2EthTradeMonitor({} as any, templates[asset].config);
      monitor.configureTradeExecutor(orders);
      monitor.getTradePlan = jest.fn().mockResolvedValue(p);
      const run = (r: any) => asset === 'BTC' ? monitor.executeManualBtcEntry(r) : monitor.executeManualEntry(r);
      const file = asset === 'BTC' ? process.env.DECENTRADER_GAP_ALERT_STATE_FILE! : process.env[`OPEN_LIQUIDITY_V2_${asset}_TRADE_STATE_FILE`]!;
      return { monitor, orders, p, run, file };
    }
    test.each(['long', 'short'] as const)('%s sends quarter stop with unchanged size and locked TP ladder', async direction => {
      const x = setup(direction), r = request(asset, direction), expected = direction === 'long' ? 115 : 125;
      const result = await x.run(r);
      expect(result.tradePlaced).toBe(true);
      expect(x.orders.placeOrder).toHaveBeenCalledWith(expect.objectContaining({ market: marketFor(asset),
        size: 1, static_sl: expected, take_profits: [expect.objectContaining({ price: direction === 'long' ? 140 : 100, size: 1, manual_locked: true })],
        decentrader: expect.objectContaining({ sizingStop: x.p.activePlan.stop,
          initialStopPolicy: expect.objectContaining({ bodyFraction: .25, candleSource: 'dydx-1h' }) }) }));
      const saved = JSON.parse(fs.readFileSync(x.file, 'utf8'));
      expect(saved.managedPosition).toMatchObject({ currentStop: expected, currentStopFractalTimestamp: '2026-11-04T06:00:00.000Z',
        takeProfitMode: 'manual-locked' });
      expect(saved.lastTradeDecision.initialStopPolicy.bodyFraction).toBe(.25);
      await x.run(r); expect(x.orders.placeOrder).toHaveBeenCalledTimes(1);
    });
    test('does not submit when the same-hour dYdX candle is unavailable', async () => {
      const x = setup(); delete x.p.stopReferenceCandle;
      const result = await x.run(request(asset));
      expect(result.tradePlaced).toBe(false); expect(result.tradeDeferred).toBe(true); expect(result.tradePendingData).toBe(true);
      expect(x.orders.placeOrder).not.toHaveBeenCalled();
      x.p.stopReferenceCandle = plan(asset).stopReferenceCandle;
      expect((await x.run(request(asset))).tradePlaced).toBe(true);
      expect(x.orders.placeOrder).toHaveBeenCalledTimes(1);
    });
    test('does not move an existing position or consume a second plan', async () => {
      const x = setup(); await x.run(request(asset));
      const before = fs.readFileSync(x.file, 'utf8');
      const result = await x.run({ ...request(asset), signature: 'another-manual-plan' });
      expect(result.tradeDeferred).toBe(true); expect(x.orders.placeOrder).toHaveBeenCalledTimes(1);
      expect(JSON.parse(fs.readFileSync(x.file, 'utf8')).managedPosition).toEqual(JSON.parse(before).managedPosition);
    });
    test.each(['long', 'short'] as const)('%s trailing does not loosen the quarter-body stop to a less favorable new fractal', async direction => {
      const x = setup(direction); await x.run(request(asset, direction));
      const rows = [114, 112, 110, 113, 116].map((low, i) => ({
        startedAt: new Date(Date.parse(closed) - (5 - i) * 3_600_000).toISOString(), resolution: '1HOUR',
        open: '120', close: '121', low: String(low), high: String([125, 130, 135, 130, 125][i])
      }));
      jest.spyOn(gap, 'fetchDydxHourlyCandlesForMarket').mockResolvedValue(rows as any);
      (axios.get as jest.Mock).mockResolvedValue({ data: { candles: rows } });
      const result = await x.monitor.checkManagedPosition();
      expect(result.error).toBeUndefined();
      expect(result).toMatchObject({ ok: true });
      const saved = JSON.parse(fs.readFileSync(x.file, 'utf8'));
      expect(saved.managedPosition.currentStop).toBe(direction === 'long' ? 115 : 125);
      expect(saved.managedPosition.takeProfitMode).toBe('manual-locked');
      expect(x.orders.placeOrder).toHaveBeenCalledTimes(1);
    });
  });
});
