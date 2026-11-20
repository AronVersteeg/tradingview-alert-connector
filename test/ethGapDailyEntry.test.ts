import fs from 'fs';
import os from 'os';
import path from 'path';
import * as entry from '../src/services/ethGapDailyEntry';
import * as green from '../src/services/greenLineEntry';
import * as gap from '../src/services/decentraderGapMonitor';
import { ethIntrusionFuturesDeltaFeed, parseFuturesDeltaCandle } from '../src/services/intrusionFuturesDelta';
import { OpenLiquidityV2EthTradeMonitor, openLiquidityV2BtcIntrusionMonitor, openLiquidityV2InjTradeMonitor, openLiquidityV2SolTradeMonitor, openLiquidityV2ZecTradeMonitor } from '../src/services/openLiquidityV2EthTradeMonitor';
const fractals = require('../public/btc-spot-chart/fractals.js');
const purple = require('../public/btc-spot-chart/intrusion-daily-breakouts.js');
import { PURPLE_LINE_SETUPS, purpleLineEntryEnabled } from '../src/services/purpleLineSetup';
import * as dailyHistory from '../src/services/binanceDailyFractalHistory';
import { binanceSpotChartFeed, ethSpotChartFeed, injSpotChartFeed, solSpotChartFeed, zecSpotChartFeed } from '../src/services/binanceSpotChart';
import { PurpleLineContinuity } from '../src/services/purpleLineContinuity';
import { purpleTriggerEmail } from '../src/services/purpleTriggerEmail';
import { BtcSpotCloseShadow } from '../src/services/btcSpotCloseShadow';
const HOUR = 3_600_000, closeAt = Date.parse('2026-10-24T02:00:00Z'), now = closeAt + 5000;
const iso = (ms: number) => new Date(ms).toISOString();
const signal = (direction: 'long' | 'short' = 'long'): entry.EthGapDailySignal => ({
  time: (closeAt - HOUR) / 1000, closedAt: closeAt / 1000, direction, close: direction === 'long' ? 125 : 75,
  dailyLevels: [{ id: 'D|test', type: direction === 'long' ? 'HIGH' : 'LOW', price: direction === 'long' ? 120 : 80,
    confirmedAt: iso(closeAt - 2 * HOUR - 1) }]
});

describe('ETH final-5m Futures delta measurement utility (not an entry gate)', () => {
  const response = (buy: number) => {
    const hour = signal().time;
    const candle = parseFuturesDeltaCandle([(hour + 3300) * 1000, 0, 0, 0, 0, '100',
      closeAt - 1, 0, 0, String(buy)], hour, now);
    return { symbol: 'ETHUSDT', venue: 'Binance USD-M Futures', interval: '5m', timeBasis: 'signal-last-five-minutes',
      records: [{ hour, status: 'ok', candle }] } as any;
  };
  afterEach(() => jest.restoreAllMocks());

  test.each([
    ['long', 60, true, 20], ['long', 40, false, -20], ['short', 40, true, 20],
    ['short', 60, false, -20], ['long', 50, false, 0], ['short', 50, false, 0],
    ['long', 50.0001, true, .0002]
  ])('uses unrounded direction-normalized delta for %s / buy %s', async (direction, buy, passed, expected) => {
    const read = jest.spyOn(ethIntrusionFuturesDeltaFeed, 'snapshot').mockResolvedValue(response(buy as number));
    const result = await entry.readEthEntryFuturesDelta(signal(direction as 'long' | 'short'));
    expect(read).toHaveBeenCalledWith([signal().time]);
    expect(result).toMatchObject({ symbol: 'ETHUSDT', interval: '5m', passed,
      signalTime: signal().time, startedAt: signal().time + 3300, closedAt: closeAt / 1000 });
    expect(result.directionalDeltaPct).toBeCloseTo(expected as number, 8);
  });

  test('blocks absent/pending/unavailable candles, wrong market/venue/hour and corrupt values', async () => {
    const read = jest.spyOn(ethIntrusionFuturesDeltaFeed, 'snapshot');
    for (const kind of ['missing', 'pending', 'unavailable', 'btc', 'spot', 'hour', 'window', 'invalid', 'mismatch']) {
      const value = response(60);
      if (kind === 'missing') value.records = [];
      if (kind === 'pending' || kind === 'unavailable') value.records = [{ hour: signal().time, status: kind, retryAt: iso(now + 15000) }];
      if (kind === 'btc') value.symbol = 'BTCUSDT';
      if (kind === 'spot') value.venue = 'Binance Spot';
      if (kind === 'hour') value.records[0].hour -= 3600;
      if (kind === 'window') value.records[0].candle.startedAt -= 300;
      if (kind === 'invalid') value.records[0].candle.rawDeltaPct = 99;
      if (kind === 'mismatch') value.records[0].candle.closedAt += 3600;
      read.mockResolvedValue(value);
      await expect(entry.readEthEntryFuturesDelta(signal())).rejects.toThrow();
    }
    read.mockRejectedValue(new Error('cooldown'));
    await expect(entry.readEthEntryFuturesDelta(signal())).rejects.toThrow('cooldown');
  });
});

describe.each(['BTC', 'ETH', 'INJ', 'SOL', 'ZEC'] as const)('%s purple line execution parity', asset => {
  const market = `${asset}-USD` as const;
  const symbol = `${asset}USDT`;
  const setup = PURPLE_LINE_SETUPS[market];
  const stateEnv = asset === 'BTC' ? 'OPEN_LIQUIDITY_V2_BTC_INTRUSION_STATE_FILE' : `OPEN_LIQUIDITY_V2_${asset}_TRADE_STATE_FILE`;
  const entryEnv = setup.enabledEnv, autoEnv = `OPEN_LIQUIDITY_V2_${asset}_AUTO_TRADE_ENABLED`, monitorEnv = `OPEN_LIQUIDITY_V2_${asset}_INTRUSION_MONITOR_ENABLED`;
  const createMonitor = () => new OpenLiquidityV2EthTradeMonitor({} as any, ({ BTC: openLiquidityV2BtcIntrusionMonitor,
    INJ: openLiquidityV2InjTradeMonitor, SOL: openLiquidityV2SolTradeMonitor, ZEC: openLiquidityV2ZecTradeMonitor, ETH: undefined }[asset] as any)?.config);
  const evaluateSignal = (...args: Parameters<typeof entry.evaluateEthGapDailySignal>) => entry.evaluatePurpleLineSignal(market, ...args);
  const HOUR = 3_600_000, closeAt = Date.parse('2026-10-24T02:00:00Z'), now = closeAt + 5000;
  const iso = (ms: number) => new Date(ms).toISOString();
  const signal = (direction: 'long' | 'short' = 'long'): entry.EthGapDailySignal => ({
    time: (closeAt - HOUR) / 1000, closedAt: closeAt / 1000, direction, close: direction === 'long' ? 125 : 75,
    dailyLevels: [{ id: 'D|test', type: direction === 'long' ? 'HIGH' : 'LOW', price: direction === 'long' ? 120 : 80,
      confirmedAt: iso(closeAt - 2 * HOUR - 1) }]
  });

  function inputs(direction = 'long') {
    const candles = [100, direction === 'long' ? 125 : 75].map((close, i) => ({
      time: (closeAt - (2 - i) * HOUR) / 1000, closeTime: closeAt - (1 - i) * HOUR - 1,
      open: 100, high: Math.max(100, close) + 1, low: Math.min(100, close) - 1, close,
      closed: true, volume: 1, quoteVolume: 100
    }));
    const payload = {
      source: { market, sourceStatuses: [{ name: `Binance Spot ${symbol} 1H`, ok: true }] },
      modelVersion: setup.modelVersion, quality: { priceStepUsd: setup.priceStepUsd, causalModel: true },
      frames: candles.map(c => ({ ...c, t: iso(c.time * 1000).slice(0, 19).replace('T', ' '), startedAtMs: c.time * 1000 })),
      gaps: [ { left: 85, right: 115, width: 30 }, { left: 85, right: 115, width: 30 } ],
      zoneSeed: [['L', 10, 85, 4], ['S', 10, 115, 3]],
      zoneDeltas: [[], [[direction === 'long' ? 'L' : 'S', 10, direction === 'long' ? 90 : 110, 1]]]
    };
    const intrusions = [{ timestamp: payload.frames[1].t, direction }];
    const page = { symbol, venue: 'Binance Spot', interval: '1h', fetchedAt: iso(now), candles, hasMore: false } as any;
    const daily = { symbol, venue: 'Binance Spot', interval: '1d', latestClosedCandleAt: '2026-10-23T23:59:59.999Z',
      records: [{ id: 'test', type: direction === 'long' ? 'HIGH' : 'LOW', priceExact: direction === 'long' ? '120' : '80',
        pivotAt: '2026-09-01T00:00:00Z', confirmedAt: '2026-10-23T23:59:59.999Z', isLatestForType: true }] };
    return { payload, intrusions, page, daily };
  }

  const greenSignal = (direction: 'long' | 'short' = 'long'): entry.GreenLineSignal => ({
    time: signal(direction).time, closedAt: signal(direction).closedAt, direction, close: signal(direction).close,
    setup: 'green-line', edge: direction === 'long' ? 115 : 85
  });

  describe('Green entry and chart parity', () => {
    afterEach(() => { jest.restoreAllMocks(); jest.useRealTimers(); });

    test.each(['long', 'short'] as const)('uses the same fresh %s gap edge as the chart, including mixed intrusions', direction => {
      const {payload,page} = inputs(direction);
      payload.gaps[1] = {left:60,right:140,width:80};
      const intrusions = [{timestamp:payload.frames[1].t,direction:'mixed'}];
      const history = purple.compactIntrusionHistory({source:payload.source,intrusionBenchmarks:{history:intrusions}},
        now,market,{includeGapHours:true});
      const timeline = require('../public/btc-spot-chart/gap-boxes.js').timelineFromPayload(payload,market);
      const [marker] = purple.buildGapIntrusionMarkers(page.candles,history,timeline,now,market);
      expect(green.evaluateGreenLineSignal(market,payload,intrusions,page,now)).toEqual({...marker,...greenSignal(direction)});
      expect(green.evaluateGreenLineSignal(market,payload,[{...intrusions[0],timestamp:payload.frames[0].t}],page,now)).toBeUndefined();
      expect(green.evaluateGreenLineSignal(market,payload,[],page,now)).toBeUndefined();
    });

    test('ignores touches, wick breaks, and levels already crossed; rejects stale or cross-market data', () => {
      const {payload,intrusions,page} = inputs();
      for (const price of [114,115]) {
        page.candles[1].close = payload.frames[1].close = price;
        expect(green.evaluateGreenLineSignal(market,payload,intrusions,page,now)).toBeUndefined();
      }
      page.candles[1].close = payload.frames[1].close = 125;
      payload.gaps[0] = {left:85,right:99,width:14};
      expect(green.evaluateGreenLineSignal(market,payload,intrusions,page,now)).toBeUndefined();
      expect(() => green.evaluateGreenLineSignal(market,payload,intrusions,{...page,symbol:'XRPUSDT'},now)).toThrow('sources');
      expect(() => green.evaluateGreenLineSignal(market,payload,intrusions,page,now+HOUR)).toThrow('consecutive');
      page.candles[0].closed = false;
      expect(() => green.evaluateGreenLineSignal(market,payload,intrusions,page,now)).toThrow('consecutive');
    });

    test('reads only the matching pair’s Spot feed, with no Daily dependency', async () => {
      jest.useFakeTimers(); jest.setSystemTime(now);
      const {payload,intrusions,page} = inputs();
      const feeds = {BTC:binanceSpotChartFeed,ETH:ethSpotChartFeed,INJ:injSpotChartFeed,SOL:solSpotChartFeed,ZEC:zecSpotChartFeed};
      const read = jest.spyOn(feeds[asset],'page').mockResolvedValue(page);
      const wrong = Object.entries(feeds).filter(([other])=>other!==asset).map(([,feed])=>jest.spyOn(feed,'page').mockRejectedValue(new Error('wrong pair')));
      const daily = jest.spyOn(dailyHistory,'binanceDailyFractalHistory').mockRejectedValue(new Error('Daily must not be used'));
      await expect(green.readGreenLineSignal(market,payload,intrusions)).resolves.toEqual(greenSignal());
      expect(read).toHaveBeenCalledTimes(1);
      expect(daily).not.toHaveBeenCalled();
      for(const spy of wrong) expect(spy).not.toHaveBeenCalled();
    });

    test.each(['long', 'short'] as const)('Yellow %s needs only the matching closed intrusion, not a box or Daily break', direction => {
      const { payload, intrusions } = inputs(direction);
      delete (payload as any).gaps;
      expect(green.evaluateYellowIntrusionSignal(market, payload, intrusions, direction, now)).toEqual({
        time: signal().time, closedAt: signal().closedAt, direction });
      for (const wrong of [[], [{ ...intrusions[0], direction: direction === 'long' ? 'short' : 'long' }],
        [{ ...intrusions[0], direction: 'mixed' }], [{ ...intrusions[0], timestamp: payload.frames[0].t }]]) {
        expect(green.evaluateYellowIntrusionSignal(market, payload, wrong, direction, now)).toBeUndefined();
      }
      expect(() => green.evaluateYellowIntrusionSignal(market, payload, intrusions, direction, now + HOUR)).toThrow('closed');
      expect(() => green.evaluateYellowIntrusionSignal(market, { ...payload, source: { market: 'XRP-USD' } }, intrusions, direction, now)).toThrow('sources');
      expect(() => green.evaluateYellowIntrusionSignal(market, { ...payload, modelVersion: 'wrong' }, intrusions, direction, now)).toThrow('sources');
    });
  });

  describe('Spot gap intrusion + Daily entry signal', () => {
    afterEach(() => { jest.restoreAllMocks(); jest.useRealTimers(); });
    test.each(['long', 'short'] as const)('Purple reads a fresh %s break of both levels from its own pair', async direction => {
      jest.useFakeTimers(); jest.setSystemTime(now);
      const x = inputs(direction);
      const ownFeed = { BTC: binanceSpotChartFeed, ETH: ethSpotChartFeed, INJ: injSpotChartFeed, SOL: solSpotChartFeed, ZEC: zecSpotChartFeed }[asset];
      const candleRead = jest.spyOn(ownFeed, 'page').mockResolvedValue(x.page);
      const read = (payload: any, intrusions: any[]) => asset === 'BTC' ? green.readBtcPurpleTrailSignal(payload, intrusions)
        : green.readPurpleGapBreakSignal(market, payload, intrusions);
      const dailyRead = jest.spyOn(dailyHistory, 'binanceDailyFractalHistory').mockResolvedValue(x.daily as any);
      const [prev] = x.payload.gaps;
      x.payload.gaps[1] = { left: 65, right: 145, width: 80 };
      await expect(read(x.payload, x.intrusions)).resolves.toMatchObject({
        setup: 'purple-green-trail', ...signal(direction), gap: { lower: prev.left, upper: prev.right }
      });
      expect(candleRead).toHaveBeenCalledTimes(1);
      expect(dailyRead).toHaveBeenCalledWith(market, false, 'Binance Spot');
      await expect(read(x.payload, [{ ...x.intrusions[0], direction: 'mixed' }])).resolves.toBeUndefined();
      x.daily.records[0].priceExact = direction === 'long' ? '126' : '74';
      await expect(read(x.payload, x.intrusions)).resolves.toBeUndefined();
      x.daily.records[0].priceExact = direction === 'long' ? '120' : '80';
      x.payload.gaps[0] = { left: 75, right: 125, width: 50 };
      dailyRead.mockClear();
      await expect(read(x.payload, x.intrusions)).resolves.toBeUndefined();
      expect(dailyRead).not.toHaveBeenCalled();
    });
    test('reads only its own Spot candles and Daily source', async () => {
      jest.useFakeTimers(); jest.setSystemTime(now);
      const x = inputs();
      const daily = jest.spyOn(dailyHistory, 'binanceDailyFractalHistory').mockResolvedValue(x.daily as any);
      const own = { BTC: binanceSpotChartFeed, ETH: ethSpotChartFeed, INJ: injSpotChartFeed, SOL: solSpotChartFeed, ZEC: zecSpotChartFeed }[asset];
      const candles = jest.spyOn(own, 'page').mockResolvedValue(x.page);
      const wrong = [binanceSpotChartFeed, ethSpotChartFeed, solSpotChartFeed, zecSpotChartFeed].filter(feed => feed !== own)
        .map(feed => jest.spyOn(feed, 'page').mockRejectedValue(new Error('wrong pair read')));
      expect(await entry.readPurpleLineSignal(market, x.payload, x.intrusions)).toMatchObject({ direction: 'long' });
      expect(daily).toHaveBeenCalledWith(market, false, 'Binance Spot');
      expect(candles).toHaveBeenCalledTimes(1); for (const read of wrong) expect(read).not.toHaveBeenCalled();
    });
    test.each(['long', 'short'])('has exactly the same %s signal as the purple overlay, without Weekly/Monthly data', direction => {
      const { payload, intrusions, page, daily } = inputs(direction);
      const result = evaluateSignal(payload, intrusions, page, daily, now);
      const projection = fractals.projectDailyFractals({ symbol, venue: 'Binance Spot', daily,
        evaluatedThrough: iso(closeAt) }, page.candles, symbol);
      const history = purple.compactIntrusionHistory({ source: payload.source, intrusionBenchmarks: { history: intrusions } }, now, market);
      expect(result).toEqual(purple.buildIntrusionDailyBreakouts(page.candles, history, projection, now, market).signals[0]);
      expect(result).toMatchObject({ direction, time: (closeAt - HOUR) / 1000, closedAt: closeAt / 1000 });
    });
    test.each(['long', 'short'])('keeps a Daily level eligible for a %s close break after an earlier wick-only excursion', direction => {
      const x = inputs(direction);
      if (direction === 'long') x.page.candles[0].high = 121;
      else x.page.candles[0].low = 79;
      Object.assign(x.daily.records[0], { firstBrokenAt: '2026-10-24T00:00:00Z' });
      expect(evaluateSignal(x.payload, x.intrusions, x.page, x.daily, now)).toMatchObject({ direction });
    });
    test('rejects equality touches, mismatching intrusion candle/direction and unconfirmed Daily levels', () => {
      for (const kind of ['touch', 'other-hour', 'other-direction', 'unconfirmed']) {
        const x = inputs();
        if (kind === 'touch') { x.page.candles[1].close = 120; x.payload.frames[1].close = 120; }
        if (kind === 'other-hour') x.intrusions[0].timestamp = x.payload.frames[0].t;
        if (kind === 'other-direction') x.intrusions[0].direction = 'short';
        if (kind === 'unconfirmed') x.daily.records[0].confirmedAt = iso(closeAt - HOUR);
        expect(evaluateSignal(x.payload, x.intrusions, x.page, x.daily, now)).toBeUndefined();
      }
    });
    test('fails closed for Futures, stale/missing coverage, wrong market/model, or mismatched latest Spot candles', () => {
      for (const kind of ['futures', 'stale', 'old-daily', 'btc', 'other-pair', 'grid', 'source-failed', 'model', 'coverage', 'price', 'open']) {
        const x = inputs();
        if (kind === 'futures') x.page.venue = 'Binance Futures';
        if (kind === 'stale') (x.daily as any).stale = true;
        if (kind === 'old-daily') x.daily.latestClosedCandleAt = '2026-10-22T23:59:59.999Z';
        if (kind === 'btc') x.payload.source.market = market === 'BTC-USD' ? 'ETH-USD' : 'BTC-USD';
        if (kind === 'other-pair') x.page.symbol = market === 'SOL-USD' ? 'ETHUSDT' : 'SOLUSDT';
        if (kind === 'grid') (x.payload.quality as any).priceStepUsd = 999;
        if (kind === 'source-failed') x.payload.source.sourceStatuses[0].ok = false;
        if (kind === 'model') x.payload.quality.causalModel = false;
        if (kind === 'coverage') x.page.candles.shift();
        if (kind === 'price') x.payload.frames[1].close = 124;
        if (kind === 'open') x.page.candles[1].closed = false;
        expect(() => evaluateSignal(x.payload, x.intrusions, x.page, x.daily, now)).toThrow();
      }
    });
    test('only accepts the current closed hour, within five minutes, after activation', () => {
      expect(entry.freshEthGapDailySignal(signal(), iso(closeAt - 1), now)).toBe(true);
      expect(entry.freshEthGapDailySignal(signal(), iso(closeAt), now)).toBe(false);
      expect(entry.freshEthGapDailySignal(signal(), iso(closeAt - HOUR), closeAt + 300001)).toBe(false);
      expect(entry.freshEthGapDailySignal(signal(), iso(closeAt - HOUR), closeAt - 1)).toBe(false);
      expect(entry.freshEthGapDailySignal(signal(), iso(closeAt - HOUR), closeAt + HOUR)).toBe(false);
    });
  });

  describe('dYdX body-open initial stop with original fractal sizing', () => {
    const candle = () => ({ timestamp: iso(closeAt - HOUR), fractalCandleSource: 'dydx-1h',
      openRef: 110, closeRef: 118, highRef: 150, lowRef: 80 });
    const plan = (direction = 'long') => ({ direction, status: 'watch',
      stop: { valid: true, price: direction === 'long' ? 100 : 140, source: 'confirmed-fractal',
        buffer: .1, maxDistancePct: .3, fractal: { index: 2, timestamp: iso(closeAt - 3 * HOUR), price: direction === 'long' ? 100 : 140, source: 'lowRef' } },
      sizing: { size: .75, riskBudgetUsd: 15, plannedStopRiskUsd: 15 } });

    test('changes only initial stop price, not sizing/anchor; uses dYdX body rather than Spot price or wick midpoint', () => {
      const original = plan(), before = JSON.stringify(original);
      const result = entry.withPurpleDydxBodyOpenStop(original, candle(), signal(), 120);
      expect(result.stop).toMatchObject({ price: 110, source: 'dydx-signal-body-open', buffer: 0, distance: 10 });
      expect(result.sizing).toBe(original.sizing); expect(result.sizingStop).toEqual(original.stop);
      expect(result.stop.fractal).toEqual(original.stop.fractal);
      expect(result.initialStopPolicy).toMatchObject({ mode: 'signal-body-open', bodyFraction: 0,
        candleSource: 'dydx-1h', open: 110, close: 118,
        bodyStopPrice: 110, sizingStopPrice: 100, fractalPlannedStopRiskUsd: 15, bodyStopPlannedStopRiskUsd: 7.5 });
      expect(JSON.stringify(original)).toBe(before);
    });
    test('short uses the candle open above the dYdX entry reference, not its high wick or close', () => {
      const result = entry.withPurpleDydxBodyOpenStop(plan('short'), { ...candle(), openRef: 118, closeRef: 110 }, signal('short'), 108);
      expect(result.stop.price).toBe(118); expect(result.sizing.size).toBe(.75);
      expect(result.sizingStop.price).toBe(140);
    });
    test.each([
      ['long', 100, 110, 100], ['short', 100, 90, 100]
    ])('uses the exact %s body start without a buffer or quarter-body offset', (direction, open, close, expected) => {
      const result = entry.withPurpleDydxBodyOpenStop(plan(String(direction)), {
        ...candle(), openRef: open, closeRef: close
      }, signal(direction as 'long' | 'short'), Number(close));
      expect(result.stop.price).toBe(expected);
    });
    test.each([
      ['long', 118, 110, 120], ['short', 110, 118, 108], ['long', 110, 110, 120]
    ])('uses the open even for %s with opposite-color or doji dYdX body %s to %s', (direction, open, close, reference) => {
      const result = entry.withPurpleDydxBodyOpenStop(plan(String(direction)), {
        ...candle(), openRef: open, closeRef: close
      }, signal(direction as 'long' | 'short'), Number(reference));
      expect(result.stop.price).toBe(open);
      expect(result.initialStopPolicy.bodyFraction).toBe(0);
    });
    test.each([
      ['signal-body-open', 'dYdX signal candle body open'],
      ['signal-body-quarter', 'dYdX signal-body 25% from open'],
      ['signal-body-midpoint', 'dYdX signal-body midpoint'],
      ['confirmed-fractal', 'confirmed dYdX fractal']
    ])('reports the recorded %s mail policy, including older queued notifications', (mode, label) => {
      const activePlan = {stop: {price: 118}, initialStopPolicy: {mode}};
      const intent = {signature: `${market}|GAP_DAILY|${signal().time}|long`, outcome: 'CONFIRMED', plan: {activePlan}};
      const mail = purpleTriggerEmail(market, signal(), {intent}, {});
      expect(mail.body).toContain(`Planned initial SL (${label}): 118`);
    });
    test.each(['source', 'old-hour', 'future-close', 'invalid-ohlc', 'missing-open', 'no-fractal', 'direction', 'not-protective', 'equal-reference', 'max-distance'])('fails closed without a silent fractal fallback: %s', kind => {
      const p: any = plan(), c: any = candle(), s: any = signal(); let reference = 120;
      if (kind === 'source') c.fractalCandleSource = 'binance-spot';
      if (kind === 'old-hour') c.timestamp = iso(closeAt - 2 * HOUR);
      if (kind === 'future-close') s.closedAt = Date.now() / 1000 + HOUR;
      if (kind === 'invalid-ohlc') c.lowRef = 119;
      if (kind === 'missing-open') delete c.openRef;
      if (kind === 'no-fractal') p.stop.valid = false;
      if (kind === 'direction') p.direction = 'short';
      if (kind === 'not-protective') reference = 109;
      if (kind === 'equal-reference') reference = 110;
      if (kind === 'max-distance') p.stop.maxDistancePct = .01;
      expect(() => entry.withPurpleDydxBodyOpenStop(p, c, s, reference)).toThrow();
    });
  });

  describe('Purple live execution and persistent trailing-only management', () => {
    const oldEnv = { ...process.env };
    let directory: string, file: string, monitor: any, orders: any, position: any;
    beforeEach(() => {
      jest.useFakeTimers(); jest.setSystemTime(now);
      directory = fs.mkdtempSync(path.join(os.tmpdir(), 'eth-gap-daily-'));
      file = path.join(directory, 'eth.json');
      for (const pair of ['ETH', 'INJ', 'SOL', 'ZEC']) delete process.env[`${pair}_ENTRY_MODE`];
      Object.assign(process.env, {
        BTC_ENTRY_MODE: 'manual', DECENTRADER_AUTO_TRADE_ENABLED: 'true',
        DECENTRADER_GAP_ALERT_STATE_FILE: path.join(directory, 'btc-manual-execution.json'),
        BTC_MANUAL_TRADE_OVERRIDE_FILE: path.join(directory, 'btc-manual-plans.json'),
        [`${asset}_MANUAL_TRADE_OVERRIDE_FILE`]: path.join(directory, `${asset}-manual-plans.json`),
        [stateEnv]: file, [entryEnv]: asset === 'BTC' || asset === 'INJ' ? 'green' : 'true',
        [autoEnv]: 'true', [monitorEnv]: 'true',
        DECENTRADER_DYNAMIC_SL_ENABLED: 'true', DECENTRADER_DYNAMIC_SL_LIVE_UPDATES_ENABLED: 'true',
        DYDX_V4_FRACTAL_TRAIL_ENABLED: 'true', DECENTRADER_DYNAMIC_TP_ENABLED: 'true',
        DECENTRADER_SL_FRACTAL_WINDOW: '2', DECENTRADER_SL_BUFFER_PCT: '0.001', DECENTRADER_SL_LOOKBACK_BARS: '72',
        DECENTRADER_SL_MAX_DISTANCE_PCT: '0.3', DECENTRADER_DYNAMIC_SL_FRACTAL_DELAY: '0',
        DECENTRADER_TRADE_RISK_USD: '15', DECENTRADER_TRADE_RISK_USD_CAP_BY_PCT: 'false'
      });
      position = undefined;
      jest.spyOn(gap, 'smtpSettingsFromEnv').mockReturnValue(undefined);
      jest.spyOn(gap, 'sendEmailBestEffort').mockResolvedValue({ sent: true });
      jest.spyOn(entry, 'readEthEntryFuturesDelta').mockImplementation(async s => ({
        symbol: 'ETHUSDT', venue: 'Binance USD-M Futures', interval: '5m', signalTime: s.time,
        startedAt: s.time + 3300, closedAt: s.closedAt, fetchedAt: iso(now),
        rawDeltaPct: s.direction === 'long' ? 20 : -20, directionalDeltaPct: 20, passed: true
      }));
      orders = {
        getAccountSnapshot: jest.fn().mockImplementation(async () => ({ equity: 1000, freeCollateral: 1000,
          openPositionsCount: position ? 1 : 0, openPositions: position ? [position] : [],
          markets: { [market]: { status: 'ACTIVE', oraclePrice: 120, initialMarginFraction: .1, stepSize: .1 } } })),
        getStatefulOrderCapacity: jest.fn().mockResolvedValue({ limit: 20, openOrders: 0, marketOpenOrders: 0, marketOrderLimit: 3 }),
        placeOrder: jest.fn().mockImplementation(async (alert: any) => {
          expect(JSON.parse(fs.readFileSync(file, 'utf8')).gapDailyEntry.intent).toMatchObject({ outcome: 'SUBMITTING', orderAlert: { profile: 'NO_TP', take_profits: [] } });
          position = { market, size: alert.desired_position === 'LONG' ? .4 : -.4, entryPrice: 120 };
        }),
        syncTrailingStop: jest.fn().mockResolvedValue({ outcome: 'UPDATED', verified: true }),
        syncTakeProfits: jest.fn().mockResolvedValue({ outcome: 'UPDATED' })
      };
      monitor = createMonitor();
      monitor.processStartedAt = closeAt - HOUR;
      monitor.configureTradeExecutor(orders);
      jest.spyOn(gap, 'fetchDydxHourlyCandlesForMarket').mockResolvedValue([115, 112, 110, 113, 116].map((low, i) => ({
        startedAt: iso(closeAt - (5 - i) * HOUR), resolution: '1HOUR', open: String(low + 2), high: String(low + 5),
        low: String(low), close: String(low + 3)
      })) as any);
      jest.spyOn(console, 'error').mockImplementation(() => {});
      jest.spyOn(console, 'log').mockImplementation(() => {});
      jest.spyOn(console, 'warn').mockImplementation(() => {});
    });
    afterEach(() => {
      monitor.stop(); jest.restoreAllMocks(); jest.useRealTimers();
      for (const key of Object.keys(process.env)) if (!(key in oldEnv)) delete process.env[key];
      Object.assign(process.env, oldEnv);
      fs.rmSync(directory, { recursive: true, force: true });
    });
    const state = () => ({ gapDailyEntry: { activatedAt: iso(closeAt - HOUR) } });

    if (asset === 'BTC') {
      test('BTC shadow uses the real Green evaluator without fetching chart data or placing any order', async () => {
        const { payload, page } = inputs();
        const candles = page.candles.map((row: any) => ({
          timestampMs: row.time * 1000, closeTimeMs: row.closeTime,
          open: row.open, high: row.high, low: row.low, close: row.close
        }));
        process.env.OPEN_LIQUIDITY_V2_BTC_SPOT_CLOSE_SHADOW_ENABLED = 'true';
        jest.spyOn(BtcSpotCloseShadow.prototype, 'start').mockImplementation(function (this: BtcSpotCloseShadow) {
          (this as any).running = true;
        });
        const chart = jest.spyOn(binanceSpotChartFeed, 'page').mockResolvedValue(page);
        monitor.collector.previewBtcSpotClose = jest.fn().mockReturnValue({ payload, candles });
        monitor.startBtcSpotCloseShadow();
        const shadow = monitor.btcSpotCloseShadow;
        const signalCandle = candles.at(-1);
        shadow.receive({ e: 'kline', s: 'BTCUSDT', k: { s: 'BTCUSDT', i: '1h', x: true,
          t: signalCandle.timestampMs, T: signalCandle.closeTimeMs, o: String(signalCandle.open),
          h: String(signalCandle.high), l: String(signalCandle.low), c: String(signalCandle.close) } }, now);
        expect(monitor.getStatus().btcSpotCloseShadow.recent[0].shadowDecision).toBe('GREEN');
        expect(chart).not.toHaveBeenCalled();
        expect(orders.getAccountSnapshot).not.toHaveBeenCalled();
        expect(orders.placeOrder).not.toHaveBeenCalled();
        jest.setSystemTime(closeAt + 60_000);
        await monitor.checkGapDailyEntry(state(), payload, {});
        expect(chart).toHaveBeenCalledTimes(1);
        expect(orders.placeOrder).toHaveBeenCalledTimes(1);
        expect(monitor.getStatus().btcSpotCloseShadow.recent[0]).toMatchObject({
          shadowDecision: 'GREEN', restDecision: 'GREEN', comparison: { result: 'MATCH', leadMs: 55_000 }
        });
      });
    }

    test.each([
      ['atr', 'pass', 2], ['atr', 'weak', 1.99], ['atr', 'missing', 2], ['atr', 'wrong-market', 2],
      ['atr-purple', 'pass', 2], ['atr-purple', 'weak', 1.99], ['atr-purple', 'missing', 2],
      ['atr-purple', 'wrong-market', 2], ['atr-purple', 'no-purple', 2], ['atr-purple', 'no-checkpoint', 2]
    ] as const)('%s %s only trades with matching confluence and a dYdX body >= 2x Spot ATR', async (mode, kind, ratio) => {
        process.env[`${asset}_ENTRY_MODE`] = mode;
        const usePurple = mode === 'atr-purple';
        const readGreen = jest.spyOn(green, 'readGreenLineSignal').mockResolvedValue(greenSignal());
        const readPurple = jest.spyOn(green, 'readPurpleGapBreakSignal').mockResolvedValue(kind === 'no-purple' ? undefined : {
          ...signal(), setup: 'purple-green-trail', gap: { lower: 85, upper: 115, candleTime: signal().time - 3600 }
        });
        if (usePurple && kind !== 'no-checkpoint') {
          orders.supportsEntrySubmissionCheckpoint = true;
          orders.placeOrder.mockImplementationOnce(async (_alert: any, checkpoint: any) => {
            await checkpoint.beforeEntrySubmit();
            position = { market, size: .4, entryPrice: 120 };
          });
        }
        jest.spyOn(monitor, 'latestSpotIntrusions').mockReturnValue([{}]);
        const { payload } = inputs();
        payload.frames = [...Array.from({ length: 14 }, (_, i) => ({ ...payload.frames[0], startedAtMs: closeAt - (16 - i) * HOUR,
          high: 121, low: 120, close: 120 })),
          ...payload.frames.map((frame, i) => i === 0 ? { ...frame, high: 121, low: 120, close: 120 } : frame)];
        (gap.fetchDydxHourlyCandlesForMarket as jest.Mock).mockResolvedValue([115, 112, 110, 113, 116].slice(0, kind === 'missing' ? 4 : 5)
          .map((low, i) => ({ ticker: kind === 'wrong-market' ? 'PAXG-USD' : market,
            startedAt: iso(closeAt - (5 - i) * HOUR), resolution: '1HOUR', open: String(low + 2), high: String(low + 5),
            low: String(low), close: String(i === 4 ? 118 + ratio : low + 3) })));
        const managed: any = state(), result: any = {};
        await monitor.checkGapDailyEntry(managed, payload, result);
        expect(readGreen).toHaveBeenCalledTimes(usePurple ? 0 : 1);
        expect(readPurple).toHaveBeenCalledTimes(usePurple ? 1 : 0);
        if (kind === 'pass') {
          expect(orders.placeOrder).toHaveBeenCalledTimes(1);
          expect(orders.placeOrder.mock.calls[0][0]).toMatchObject({ market, static_sl: 118, take_profits: [],
            gap_daily_entry: { setup: usePurple ? 'atr-purple-line' : 'atr-line', atr: { spotAtr: 1, ratio: 2 },
              ...(usePurple && { dailyLevels: signal().dailyLevels }) } });
          expect(managed.gapDailyEntry.triggerEmails[0].subject).toContain(`${asset} ${usePurple ? 'ATR PURPLE' : 'ATR'} LONG TRIGGERED`);
        } else {
          expect(orders.placeOrder).not.toHaveBeenCalled();
          if (kind !== 'no-checkpoint') expect(managed.gapDailyEntry.triggerEmails).toBeUndefined();
          expect(managed.gapDailyEntry.decision.outcome).toBe(kind === 'weak' ? 'ATR_BELOW_THRESHOLD'
            : kind === 'no-purple' ? 'NO_PURPLE_CONFLUENCE' : kind === 'no-checkpoint' ? 'SKIPPED' : 'ERROR');
          if (kind === 'missing' || kind === 'wrong-market') expect(result.gapDailyEntryPending).toBe(true);
          if (kind === 'no-purple') expect(gap.fetchDydxHourlyCandlesForMarket).not.toHaveBeenCalled();
        }
      });

    test.each([['purple', 'long'], ['purple', 'short'], ['green', 'long'], ['green', 'short']] as const)(
      'the selected %s %s scanner dispatches only its own entry route and records the correct trigger mail', async (mode, direction) => {
        process.env[`${asset}_ENTRY_MODE`] = mode;
        orders.supportsEntrySubmissionCheckpoint = true;
        if (direction === 'short') (gap.fetchDydxHourlyCandlesForMarket as jest.Mock).mockResolvedValue([125, 128, 130, 127, 124].map((high, i) => ({
          startedAt: iso(closeAt - (5 - i) * HOUR), open: String(high - 2), close: String(high - 3), high: String(high), low: String(high - 8)
        })));
        const purpleSignal: entry.BtcPurpleTrailSignal = { ...signal(direction), setup: 'purple-green-trail',
          gap: { lower: 85, upper: 115, candleTime: signal().time - 3600 } };
        const purpleRead = jest.spyOn(green, 'readPurpleGapBreakSignal').mockResolvedValue(purpleSignal);
        const greenRead = jest.spyOn(green, 'readGreenLineSignal').mockResolvedValue(greenSignal(direction));
        orders.placeOrder.mockImplementationOnce(async (_alert: any, execution: any) => {
          await execution.beforeEntrySubmit();
          position = { market, size: direction === 'long' ? .4 : -.4, entryPrice: 120 };
        });
        const managed: any = state(), result: any = {};
        await monitor.checkGapDailyEntry(managed, inputs(direction).payload, result);
        expect(result.tradePlaced).toBe(true); expect(orders.placeOrder).toHaveBeenCalledTimes(1);
        if (mode === 'purple') { expect(purpleRead).toHaveBeenCalledWith(market, expect.anything(), expect.any(Array)); expect(greenRead).not.toHaveBeenCalled(); }
        else { expect(greenRead).toHaveBeenCalledWith(market, expect.anything(), expect.any(Array)); expect(purpleRead).not.toHaveBeenCalled(); }
        expect(managed.managedPosition).toMatchObject({ direction, takeProfitMode: 'trailing-only', intrusionTrail: { entryClosedAt: signal().closedAt } });
        expect(orders.placeOrder.mock.calls[0][0]).toMatchObject({ market, static_sl: direction === 'long' ? 118 : 122,
          take_profits: [], decentrader: { initialStopPolicy: { mode: 'signal-body-open' } } });
        const queued = managed.gapDailyEntry.triggerEmails[0];
        expect(queued).toMatchObject({ outcome: 'PLACED', subject: expect.stringContaining(`${asset} ${mode.toUpperCase()} ${direction.toUpperCase()} TRIGGERED`) });
        expect(queued.body).toContain(`Source: Binance Spot ${symbol} 1H intrusion + ${mode === 'purple' ? 'gap edge and Daily fractal' : 'gap edge'} close break`);
        expect(queued.body).toContain('later same-direction Yellow intrusion closes');
        expect(gap.sendEmailBestEffort).not.toHaveBeenCalled();
        (gap.smtpSettingsFromEnv as jest.Mock).mockReturnValue({ host: 'fixture.invalid' });
        await monitor.flushPurpleTriggerEmails();
        expect(gap.sendEmailBestEffort).toHaveBeenCalledWith(expect.anything(), queued.subject, queued.body,
          { kind: 'entry', market, signature: queued.signature, outcome: 'PLACED' });
        expect(gap.sendEmailBestEffort).toHaveBeenCalledTimes(1);
      });

  test('sends one Green trigger email after entry and stop setup, never placing a second order', async () => {
    jest.spyOn(green, 'readGreenLineSignal').mockResolvedValue(greenSignal());
    (gap.smtpSettingsFromEnv as jest.Mock).mockReturnValue({ host: 'fixture.invalid', recipients: ['fixture@example.invalid'] });
    const send = gap.sendEmailBestEffort as jest.Mock;
    send.mockImplementation(async (_smtp, subject, body) => {
      expect(orders.placeOrder).toHaveBeenCalledTimes(1);
      expect(orders.syncTrailingStop).toHaveBeenCalled();
      expect(subject).toContain(`${asset} GREEN LONG TRIGGERED`);
      expect(body).toContain(`Execution market: dYdX ${market}`);
      expect(body).toContain('Previous gap upper edge: 115');
      expect(body).toContain('Order result: PLACED'); expect(body).toContain('TPs: none.');
      expect(body).toContain('Planned initial SL (dYdX signal candle body open): 118');
      return { sent: true };
    });
    const managed: any = state(); await monitor.checkGapDailyEntry(managed, inputs().payload, {});
    expect(send).not.toHaveBeenCalled();
    await monitor.flushPurpleTriggerEmails();
    await monitor.checkGapDailyEntry(managed, inputs().payload, {});
    await monitor.flushPurpleTriggerEmails();
    expect(send).toHaveBeenCalledTimes(1); expect(orders.placeOrder).toHaveBeenCalledTimes(1);
    expect(monitor.getStatus().gapDailyEntry.triggerEmail.recent[0]).toMatchObject({ outcome: 'PLACED', attempts: 1, smtpSentAt: iso(now) });
  });

  test('does not email a short Green skipped because a position is already open', async () => {
    jest.spyOn(green, 'readGreenLineSignal').mockResolvedValue(greenSignal('short'));
    position = { market, size: .4, entryPrice: 120 };
    (gap.smtpSettingsFromEnv as jest.Mock).mockReturnValue({ host: 'fixture.invalid' });
    await monitor.checkGapDailyEntry(state(), inputs('short').payload, {});
    await monitor.flushPurpleTriggerEmails();
    expect(gap.sendEmailBestEffort).not.toHaveBeenCalled();
    expect(orders.placeOrder).not.toHaveBeenCalled();
  });

  test.each(['DETECTED', 'SKIPPED', 'ERROR', 'UNCONFIRMED', 'FLATTENED', 'NOT_SUBMITTED', 'NO_POSITION_OBSERVED'])(
    'does not deliver a queued %s signal, including records saved before deployment', async outcome => {
      (gap.smtpSettingsFromEnv as jest.Mock).mockReturnValue({ host: 'fixture.invalid' });
      const managed: any = state();
      managed.gapDailyEntry.triggerEmails = [{ ...purpleTriggerEmail(market, greenSignal(), managed.gapDailyEntry, {}), outcome }];
      fs.writeFileSync(file, JSON.stringify(managed));
      await monitor.flushPurpleTriggerEmails();
      expect(gap.sendEmailBestEffort).not.toHaveBeenCalled();
      expect(monitor.getStatus().gapDailyEntry.triggerEmail.recent[0]).toMatchObject({ outcome, attempts: 0 });
    });

  test('promotes an already queued signal after confirmed recovery, without creating a new order', async () => {
    jest.spyOn(green, 'readGreenLineSignal').mockResolvedValue(greenSignal());
    (gap.smtpSettingsFromEnv as jest.Mock).mockReturnValue({ host: 'fixture.invalid' });
    const managed: any = state();
    await monitor.checkGapDailyEntry(managed, inputs().payload, {});
    managed.gapDailyEntry.triggerEmails[0].outcome = 'UNCONFIRMED';
    managed.gapDailyEntry.triggerEmails[0].body = 'Previous incomplete signal';
    fs.writeFileSync(file, JSON.stringify(managed));
    const restarted: any = createMonitor();
    await restarted.flushPurpleTriggerEmails();
    expect(gap.sendEmailBestEffort).toHaveBeenCalledWith(expect.anything(), expect.any(String),
      expect.stringContaining('Order result: PLACED'), { kind: 'entry', market,
        signature: managed.gapDailyEntry.intent.signature, outcome: 'PLACED' });
    expect(orders.placeOrder).toHaveBeenCalledTimes(1);
  });

  test('SMTP failure is persisted and retried after restart without repeating the trade', async () => {
    jest.spyOn(green, 'readGreenLineSignal').mockResolvedValue(greenSignal());
    (gap.smtpSettingsFromEnv as jest.Mock).mockReturnValue({ host: 'fixture.invalid' });
    const send = gap.sendEmailBestEffort as jest.Mock;
    send.mockResolvedValueOnce({ sent: false, error: 'fixture timeout' }).mockResolvedValue({ sent: true });
    const result: any = {};
    await monitor.checkGapDailyEntry(state(), inputs().payload, result);
    await monitor.flushPurpleTriggerEmails();
    expect(result.tradePlaced).toBe(true);
    expect(monitor.getStatus().gapDailyEntry.triggerEmail.recent[0]).toMatchObject({ attempts: 1, error: 'fixture timeout' });
    await monitor.flushPurpleTriggerEmails(); expect(send).toHaveBeenCalledTimes(1);
    jest.setSystemTime(now + 60_000);
    const restarted: any = createMonitor();
    await restarted.flushPurpleTriggerEmails();
    expect(send).toHaveBeenCalledTimes(2); expect(orders.placeOrder).toHaveBeenCalledTimes(1);
    expect(restarted.getStatus().gapDailyEntry.triggerEmail.recent[0]).toMatchObject({ attempts: 2, smtpSentAt: iso(now + 60_000) });
  });

  test('the optional mail switch suppresses new and queued notifications without blocking the trade', async () => {
    process.env.OPEN_LIQUIDITY_V2_PURPLE_TRIGGER_EMAIL_ENABLED = 'false';
    jest.spyOn(green, 'readGreenLineSignal').mockResolvedValue(greenSignal());
    const result: any = {}; await monitor.checkGapDailyEntry(state(), inputs().payload, result);
    await monitor.flushPurpleTriggerEmails();
    expect(result.tradePlaced).toBe(true); expect(gap.sendEmailBestEffort).not.toHaveBeenCalled();
  });

  test('slow SMTP does not hold the position lock or delay a protective stop check', async () => {
    jest.spyOn(green, 'readGreenLineSignal').mockResolvedValue(greenSignal());
    (gap.smtpSettingsFromEnv as jest.Mock).mockReturnValue({ host: 'fixture.invalid' });
    let finish!: (value: any) => void;
    (gap.sendEmailBestEffort as jest.Mock).mockReturnValue(new Promise(resolve => { finish = resolve; }));
    await monitor.checkGapDailyEntry(state(), inputs().payload, {});
    const mail = monitor.flushPurpleTriggerEmails();
    try {
      const before = orders.syncTrailingStop.mock.calls.length;
      const protection = await monitor.checkManagedPosition();
      expect(protection.ok).toBe(true); expect(orders.syncTrailingStop.mock.calls.length).toBeGreaterThan(before);
    } finally { finish({ sent: true }); await mail; }
  });

  test('does not backfill old confirmed intents as new trigger mail', async () => {
    const managed: any = state(); await monitor.executeGapDailyEntry(managed, signal(), {});
    await monitor.flushPurpleTriggerEmails();
    expect(gap.sendEmailBestEffort).not.toHaveBeenCalled();
  });

  test('does not email source failures or non-Green candles', async () => {
    const read = jest.spyOn(green, 'readGreenLineSignal').mockRejectedValue(new Error('source unavailable'));
    await monitor.checkGapDailyEntry(state(), inputs().payload, {});
    await monitor.flushPurpleTriggerEmails(); expect(gap.sendEmailBestEffort).not.toHaveBeenCalled();
    read.mockResolvedValue(undefined);
    await monitor.checkGapDailyEntry(state(), inputs().payload, {});
    await monitor.flushPurpleTriggerEmails(); expect(gap.sendEmailBestEffort).not.toHaveBeenCalled();
  });

  test('production startup resumes a proven short outage across close and places once, with no startup delay', async () => {
    jest.setSystemTime(closeAt - 30_000);
    const lease = new PurpleLineContinuity(market, `${file}.entry-lease.json`, () => true, 'green-line');
    lease.start(); const activatedAt = lease.activation()!; lease.stop();
    jest.setSystemTime(now);
    const restarted: any = createMonitor(); monitor = restarted;
    restarted.configureTradeExecutor(orders);
    const check = jest.spyOn(restarted, 'check').mockResolvedValue({ ok: true });
    jest.spyOn(restarted, 'checkManagedPosition').mockResolvedValue({ ok: true });
    restarted.start(255_000);
    await jest.advanceTimersByTimeAsync(0); expect(check).toHaveBeenCalledTimes(1);
    expect(restarted.getStatus().gapDailyEntry.continuity).toMatchObject({ resumedAfterRestart: true, ready: true });
    jest.spyOn(green, 'readGreenLineSignal').mockResolvedValue(greenSignal());
    const managed: any = { gapDailyEntry: { activatedAt } }, result: any = {};
    await restarted.checkGapDailyEntry(managed, inputs().payload, result);
    expect(result.tradePlaced).toBe(true); expect(managed.gapDailyEntry.activatedAt).toBe(activatedAt);
    delete managed.managedPosition; position = undefined;
    await restarted.checkGapDailyEntry(managed, inputs().payload, {});
    expect(orders.placeOrder).toHaveBeenCalledTimes(1); expect(orders.syncTakeProfits).not.toHaveBeenCalled();
  });

  test('a malformed order-state file blocks execution rather than being mistaken for no previous trade', async () => {
    fs.writeFileSync(file, '{broken');
    expect(monitor.getStatus()).toMatchObject({ operational: false, tradeBlocked: 'STATE_UNREADABLE' });
    await expect(monitor.checkManagedPosition()).rejects.toThrow('no prior order');
    expect(orders.placeOrder).not.toHaveBeenCalled(); expect(fs.readFileSync(file, 'utf8')).toBe('{broken');
  });

    test('reports the Green setup and respects its own independent kill switches', () => {
      expect(monitor.getStatus()).toMatchObject({ market, autoTradeEnabled: true,
        gapDailyEntry: { setup: 'green-line', enabled: true, env: `${asset}_ENTRY_MODE`,
          source: `Binance Spot ${symbol} 1H`, executionMarket: market,
          takeProfitMode: 'trailing-only', futuresDeltaFilter: { enabled: false },
          initialStop: { mode: 'signal-body-open', bodyFraction: 0, candleSource: 'dydx-1h', tightenOnly: true } } });
      const otherMarket = market === 'SOL-USD' ? 'ETH-USD' : 'SOL-USD';
      process.env[PURPLE_LINE_SETUPS[otherMarket].enabledEnv] = 'false';
      expect(purpleLineEntryEnabled(market)).toBe(true);
      process.env[entryEnv] = 'false';
      expect(monitor.getStatus().gapDailyEntry.enabled).toBe(false);
      delete process.env[entryEnv];
      expect(purpleLineEntryEnabled(market)).toBe(false);
      expect(purpleLineEntryEnabled('BTC-USD')).toBe(false);
    expect(purpleLineEntryEnabled('toString')).toBe(false);
    });

    test('keeps original fractal sizing but places a dYdX body-open SL and manages the actual smaller fill', async () => {
      const managed: any = state(), result: any = {};
      await monitor.executeGapDailyEntry(managed, signal(), result);
      expect(result.tradePlaced).toBe(true);
      expect(orders.placeOrder).toHaveBeenCalledTimes(1);
      expect(gap.fetchDydxHourlyCandlesForMarket).toHaveBeenCalledWith(market);
      expect(orders.placeOrder).toHaveBeenCalledWith(expect.objectContaining({ market, profile: 'NO_TP',
        signal: 'LONG_ENTRY', size: 1.4, static_sl: 118, take_profits: [] }));
      expect(managed.managedPosition).toMatchObject({ takeProfitMode: 'trailing-only', takeProfits: [], initialSize: .4, currentStop: 118,
        currentStopFractalTimestamp: iso(closeAt - 3 * HOUR), currentStopFractalCandleSource: 'dydx-1h' });
      expect(managed.gapDailyEntry.intent.plan.activePlan.sizingStop.price).toBe(109.89);
      expect(managed.gapDailyEntry.intent.plan.activePlan.sizing.plannedStopRiskUsd).toBeCloseTo(1.4 * (120 - 109.89));
      expect(orders.placeOrder.mock.calls[0][0].decentrader.initialStopPolicy).toMatchObject({ candleSource: 'dydx-1h',
        mode: 'signal-body-open', bodyFraction: 0, open: 118, close: 119, bodyStopPrice: 118, sizingStopPrice: 109.89 });
      expect(orders.syncTrailingStop).toHaveBeenCalled();
      expect(orders.syncTakeProfits).not.toHaveBeenCalled();
      expect(JSON.parse(fs.readFileSync(file, 'utf8')).managedPosition.takeProfitMode).toBe('trailing-only');
      expect(managed.gapDailyEntry.intent.futuresDelta).toBeUndefined();
      expect(orders.placeOrder.mock.calls[0][0].gap_daily_futures_delta).toBeUndefined();
      expect(entry.readEthEntryFuturesDelta).not.toHaveBeenCalled();
    });
    test('real short plan sizes from a dYdX top fractal but stops at that hour open', async () => {
      (gap.fetchDydxHourlyCandlesForMarket as jest.Mock).mockResolvedValue([125, 128, 130, 127, 124].map((high, i) => ({
        startedAt: iso(closeAt - (5 - i) * HOUR), open: String(high - 2), close: String(high - 3),
        high: String(high), low: String(high - 8)
      })));
      const managed: any = state(), result: any = {};
      await monitor.executeGapDailyEntry(managed, signal('short'), result);
      expect(result.tradePlaced).toBe(true);
      expect(orders.placeOrder).toHaveBeenCalledWith(expect.objectContaining({ size: 1.4, static_sl: 122, profile: 'NO_TP', take_profits: [] }));
      expect(managed.gapDailyEntry.intent.plan.activePlan.sizingStop.price).toBeCloseTo(130.13);
      expect(managed.managedPosition).toMatchObject({ direction: 'short', currentStop: 122,
        currentStopFractalTimestamp: iso(closeAt - 3 * HOUR) });
    });
    test.each(['long', 'short'])('Yellow-gated %s trailing never widens, skips the entry Yellow and retries the same improvement', async direction => {
      if (direction === 'short') (gap.fetchDydxHourlyCandlesForMarket as jest.Mock).mockResolvedValue([125, 128, 130, 127, 124].map((high, i) => ({
        startedAt: iso(closeAt - (5 - i) * HOUR), open: String(high - 2), close: String(high - 3), high: String(high), low: String(high - 8)
      })));
      const managed: any = state(); await monitor.executeGapDailyEntry(managed, signal(direction as 'long' | 'short'), {});
      const initial = managed.managedPosition.currentStop, anchor = managed.managedPosition.currentStopFractalTimestamp;
      const candidate = jest.spyOn(gap, 'buildFractalStop');
      const read = gap.fetchDydxHourlyCandlesForMarket as jest.Mock;
      managed.managedPosition.intrusionTrail.lastIntrusionClosedAt = signal().closedAt;
      await monitor.syncManagedStop(managed, {}, position, { marketInfo: { oraclePrice: 120 }, price: 120 });
      expect(candidate).not.toHaveBeenCalled();
      expect(read).toHaveBeenCalledTimes(1);
      let gate = closeAt;
      const nextYellow = () => {
        gate += HOUR;
        jest.setSystemTime(gate + 5000);
        managed.managedPosition.intrusionTrail.lastIntrusionClosedAt = gate / 1000;
        read.mockResolvedValue([115, 112, 110, 113, 116].map((low, i) => ({
          startedAt: iso(gate - (5 - i) * HOUR), open: String(low + 2), high: String(low + 5), low: String(low), close: String(low + 3)
        })));
      };
      for (const price of [direction === 'long' ? initial - 1 : initial + 1, initial]) {
        nextYellow();
        candidate.mockReturnValue({ valid: true, price, fractal: { index: 3, timestamp: iso(closeAt - 2 * HOUR), price, source: direction === 'long' ? 'lowRef' : 'highRef' } } as any);
        const result: any = {};
        await monitor.syncManagedStop(managed, result, position, { marketInfo: { oraclePrice: 120 }, price: 120 });
        expect(managed.managedPosition.currentStop).toBe(initial);
        expect(managed.managedPosition.currentStopFractalTimestamp).toBe(anchor);
        expect(orders.syncTrailingStop.mock.calls.at(-1)[0].static_sl).toBe(initial);
        expect(result.intrusionTrail.outcome).toBe('YELLOW_NO_BETTER_CONFIRMED_FRACTAL');
      }
      const better = direction === 'long' ? initial + .5 : initial - .5;
      nextYellow();
      candidate.mockReturnValue({ valid: true, price: better, fractal: { index: 3, timestamp: iso(closeAt - 2 * HOUR), price: better, source: direction === 'long' ? 'lowRef' : 'highRef' } } as any);
      const pending: any = {};
      orders.syncTrailingStop.mockResolvedValueOnce({ outcome: 'PENDING_VERIFICATION', verified: false });
      await monitor.syncManagedStop(managed, pending, position, { marketInfo: { oraclePrice: 120 }, price: 120 });
      expect(pending.dynamicSlSync.outcome).toBe('PENDING_VERIFICATION');
      expect(managed.managedPosition.currentStop).toBe(initial);
      expect(managed.managedPosition.currentStopFractalTimestamp).toBe(anchor);
      expect(orders.syncTrailingStop.mock.calls.at(-1)[1]).toEqual({ requireVisibleReplacement: true });
      await monitor.syncManagedStop(managed, {}, position, { marketInfo: { oraclePrice: 120 }, price: 120 });
      expect(managed.managedPosition.currentStop).toBe(better);
      expect(orders.syncTrailingStop.mock.calls.at(-1)[0].static_sl).toBe(better);
      const reads = read.mock.calls.length;
      await monitor.syncManagedStop(managed, {}, position, { marketInfo: { oraclePrice: 120 }, price: 120 });
      expect(read).toHaveBeenCalledTimes(reads);
      expect(candidate).toHaveBeenCalledTimes(3);
      nextYellow();
      candidate.mockReturnValue({ valid: true, price: (initial + better) / 2, fractal: { source: direction === 'long' ? 'lowRef' : 'highRef' } } as any);
      await monitor.syncManagedStop(managed, {}, position, { marketInfo: { oraclePrice: 120 }, price: 120 });
      expect(managed.managedPosition.currentStop).toBe(better);
      expect(orders.syncTrailingStop.mock.calls.at(-1)[0].static_sl).toBe(better);
    });
    test.each([['purple', 'long'], ['purple', 'short'], ['purple-trail', 'long'], ['purple-trail', 'short']] as const)('%s %s stops at its dYdX candle open and trails only on later Yellows after a mode switch', async (mode, direction) => {
      process.env[`${asset}_ENTRY_MODE`] = mode;
      const isLong = direction === 'long';
      const signals = { ...signal(direction), setup: 'purple-green-trail', gap: { lower: 85, upper: 115, candleTime: signal().time - 3600 } };
      const initial = isLong ? 118 : 122;
      const sizingStop = isLong ? 109.89 : 130.13;
      const updated = isLong ? 118.3815 : 121.6215;
      const dydxRows = (lows: number[]) => lows.map((low, i) => {
        const high = 240 - low, open = isLong ? low + 2 : high - 2;
        return { startedAt: iso(closeAt + (i - 5) * HOUR), resolution: '1HOUR', open: String(open),
          high: String(isLong ? low + 5 : high), low: String(isLong ? low : high - 5), close: String(isLong ? open + 1 : open - 1) };
      });
      const read = gap.fetchDydxHourlyCandlesForMarket as jest.Mock;
      read.mockResolvedValue(dydxRows([115, 112, 110, 113, 116]));
      const managed: any = state();
      orders.supportsEntrySubmissionCheckpoint = true;
      orders.placeOrder.mockImplementationOnce(async (_alert: any, execution: any) => {
        expect(execution.requireFlat).toBe(true);
        await execution.beforeEntrySubmit();
        position = { market, size: isLong ? .4 : -.4, entryPrice: 120 };
      });
      await monitor.executeGapDailyEntry(managed, greenSignal(direction), {});
      expect(orders.placeOrder).not.toHaveBeenCalled();
      await monitor.executeGapDailyEntry(managed, signals, {});
      const alert = orders.placeOrder.mock.calls[0][0];
      expect(alert).toMatchObject({ profile: 'NO_TP', take_profits: [], static_sl: initial,
        decentrader: { riskBudgetUsd: 15, stop: { price: initial }, sizingStop: { price: sizingStop },
          initialStopPolicy: { mode: 'signal-body-open', bodyFraction: 0, open: initial, bodyStopPrice: initial } } });
      expect(alert.size * Math.abs(120 - alert.static_sl)).toBeLessThanOrEqual(15);
      expect(managed.managedPosition).toMatchObject({ takeProfitMode: 'trailing-only', currentStop: initial,
        intrusionTrail: { entryClosedAt: signal().closedAt } });
      expect(orders.syncTrailingStop.mock.calls.at(-1)[0].static_sl).toBe(initial);
      expect(orders.syncTakeProfits).not.toHaveBeenCalled();
      const mail = purpleTriggerEmail(market, signals as entry.BtcPurpleTrailSignal, managed.gapDailyEntry, {});
      expect(mail.body).toContain(`Planned initial SL (dYdX signal candle body open): ${initial}`);
      expect(mail.body).not.toContain('Planned initial SL (confirmed dYdX fractal)');
      const entryScan = jest.spyOn(monitor, 'latestSpotIntrusions');
      await monitor.checkIntrusionTrail(managed, {}, {});
      expect(entryScan).not.toHaveBeenCalled();

      process.env[`${asset}_ENTRY_MODE`] = 'off';
      const newClose = closeAt + 5 * HOUR;
      jest.setSystemTime(newClose + 5000);
      const greenRead = jest.spyOn(green, 'readGreenLineSignal');
      const scanPayload = (at: number, dir: 'long' | 'short') => {
        const payload = inputs(dir).payload;
        payload.frames.forEach(f => { f.startedAtMs += at - closeAt; f.t = iso(f.startedAtMs).slice(0, 19).replace('T', ' '); });
        return payload;
      };
      entryScan.mockRestore();
      jest.setSystemTime(newClose - HOUR + 5000);
      await monitor.checkIntrusionTrail(managed, scanPayload(newClose - HOUR, isLong ? 'short' : 'long'), {});
      expect(managed.managedPosition.intrusionTrail.lastIntrusionClosedAt).toBeUndefined();
      jest.setSystemTime(newClose + 5000);
      read.mockRejectedValueOnce(new Error('temporary dYdX failure'));
      await monitor.checkIntrusionTrail(managed, scanPayload(newClose, direction), {});
      expect(monitor.getStatus().managedPosition.intrusionTrail).toMatchObject({ lastIntrusionClosedAt: newClose / 1000 });
      expect(greenRead).not.toHaveBeenCalled();
      expect(orders.syncTrailingStop.mock.calls.at(-1)[0].static_sl).toBe(initial);

      const rows = dydxRows([115, 112, 110, 113, 116, 119, 119.5, 118.5, 119, 119.5, 115, 110, 115, 118]);
      // Later dYdX candles cannot influence the Yellow decision even if retried much later.
      jest.setSystemTime(newClose + 5 * HOUR);
      read.mockResolvedValue(rows);
      orders.syncTrailingStop.mockResolvedValueOnce({ outcome: 'PENDING_VERIFICATION', verified: false });
      const pending = await monitor.checkManagedPosition();
      expect(pending.dynamicSlSync.outcome).toBe('PENDING_VERIFICATION');
      expect(orders.syncTrailingStop.mock.calls.at(-1)[0].static_sl).toBeCloseTo(updated);
      expect(monitor.getStatus().managedPosition.currentStop).toBe(initial);
      orders.syncTrailingStop.mockResolvedValueOnce({ outcome: 'UPDATED', verified: true });
      const restarted: any = createMonitor();
      restarted.configureTradeExecutor(orders);
      await restarted.checkManagedPosition();
      expect(restarted.getStatus().managedPosition.currentStop).toBeCloseTo(updated);
      expect(Date.parse(restarted.getStatus().managedPosition.currentStopFractalTimestamp)).toBeGreaterThan(signal().time * 1000);
      expect(restarted.getStatus().managedPosition.intrusionTrail.pending).toBeUndefined();
      const reads = read.mock.calls.length;
      await restarted.checkManagedPosition();
      expect(read).toHaveBeenCalledTimes(reads);
      expect(orders.placeOrder).toHaveBeenCalledTimes(1);
      expect(orders.syncTakeProfits).not.toHaveBeenCalled();
    });
    test('a Yellow may use the latest already-confirmed pivot, even if that pivot predates entry', async () => {
      jest.setSystemTime(closeAt + HOUR + 5000);
      position = { market, size: .4, entryPrice: 120 };
      (gap.fetchDydxHourlyCandlesForMarket as jest.Mock).mockResolvedValue([115, 112, 110, 113, 116, 117].map((low, i) => ({
        startedAt: iso(closeAt - (5 - i) * HOUR), open: String(low + 2), high: String(low + 5), low: String(low), close: String(low + 3)
      })));
      const managed: any = { ...state(), managedPosition: { market, direction: 'long', initialSize: .4, entryPrice: 120,
        entrySignature: `${market}|GAP_DAILY|${signal().time}|long`,
        currentStop: 100, currentStopFractalTimestamp: iso(closeAt - 6 * HOUR), takeProfitMode: 'trailing-only',
        intrusionTrail: { entryClosedAt: closeAt / 1000, lastIntrusionClosedAt: (closeAt + HOUR) / 1000 } } };
      await monitor.syncManagedStop(managed, {}, position, { marketInfo: { oraclePrice: 120 }, price: 120 });
      expect(orders.syncTrailingStop.mock.calls.at(-1)[0].static_sl).toBeCloseTo(109.89);
      expect(managed.managedPosition.currentStop).toBeCloseTo(109.89);
      expect(managed.managedPosition.intrusionTrail.pending).toBeUndefined();
    });

    test.each(['long', 'short'] as const)('a standalone same-direction Yellow trails %s without a gap/Daily break or a new entry', async direction => {
      const nextClose = closeAt + HOUR, isLong = direction === 'long';
      jest.setSystemTime(nextClose + 5000);
      position = { market, size: isLong ? .4 : -.4, entryPrice: 120 };
      const x = inputs(direction);
      x.payload.frames.forEach(f => {
        f.startedAtMs += HOUR; f.t = iso(f.startedAtMs).slice(0, 19).replace('T', ' ');
        f.close = 100; // Neither gap edge nor Daily level is crossed.
      });
      const greenRead = jest.spyOn(green, 'readGreenLineSignal').mockRejectedValue(new Error('No gap evaluation during Yellow trailing'));
      const dailyRead = jest.spyOn(dailyHistory, 'binanceDailyFractalHistory').mockRejectedValue(new Error('No Daily evaluation during Yellow trailing'));
      const prices = isLong ? [115, 112, 110, 113, 116] : [125, 128, 130, 127, 124];
      const read = gap.fetchDydxHourlyCandlesForMarket as jest.Mock;
      read.mockResolvedValue(prices.map((p, i) => ({ startedAt: iso(nextClose - (5 - i) * HOUR),
        open: String(isLong ? p + 2 : p - 2), close: String(isLong ? p + 3 : p - 3),
        high: String(isLong ? p + 5 : p), low: String(isLong ? p : p - 5) })));
      const managed: any = { ...state(), managedPosition: { market, direction, initialSize: .4, entryPrice: 120,
        entrySignature: `${market}|GAP_DAILY|${signal().time}|${direction}`,
        currentStop: isLong ? 100 : 140, takeProfitMode: 'trailing-only', intrusionTrail: { entryClosedAt: closeAt / 1000 } } };
      await monitor.checkIntrusionTrail(managed, x.payload, {});
      expect(monitor.getStatus().managedPosition.currentStop).toBeCloseTo(isLong ? 109.89 : 130.13);
      expect(read).toHaveBeenCalledWith(market);
      const after = monitor.getStatus().managedPosition;
      expect(after.intrusionTrail).toMatchObject({ lastIntrusionClosedAt: nextClose / 1000, evaluatedIntrusionClosedAt: nextClose / 1000 });
      await monitor.checkIntrusionTrail({ ...managed, managedPosition: after }, x.payload, {});
      expect(orders.syncTrailingStop).toHaveBeenCalledTimes(1);
      expect(greenRead).not.toHaveBeenCalled(); expect(dailyRead).not.toHaveBeenCalled();
      expect(orders.placeOrder).not.toHaveBeenCalled(); expect(orders.syncTakeProfits).not.toHaveBeenCalled();
    });

    test('adopts an existing automatic trade without resetting its stop, replaying an old gate or changing ownership', async () => {
      position = { market, size: .4, entryPrice: 120 };
      const managed: any = { ...state(), managedPosition: { market, direction: 'long', initialSize: .4, entryPrice: 120,
        entrySignature: `${market}|GAP_DAILY|${signal().time}|long`, currentStop: 117.5, takeProfitMode: 'trailing-only',
        ...(asset === 'BTC' ? { btcPurpleTrail: { entryClosedAt: closeAt / 1000, lastScannedClosedAt: closeAt / 1000,
          lastGreenClosedAt: closeAt / 1000, pending: { greenClosedAt: closeAt / 1000, stop: { price: 118 } } } } : {}) } };
      await monitor.syncManagedStop(managed, {}, position, { marketInfo: { oraclePrice: 120 }, price: 120 });
      expect(managed.managedPosition.currentStop).toBe(117.5);
      expect(managed.managedPosition.intrusionTrail).toMatchObject({ entryClosedAt: closeAt / 1000 });
      expect(managed.managedPosition.btcPurpleTrail).toBeUndefined();
      expect(managed.managedPosition.intrusionTrail.pending).toBeUndefined();
      expect(gap.fetchDydxHourlyCandlesForMarket).not.toHaveBeenCalled();
      expect(orders.syncTrailingStop.mock.calls[0][0].static_sl).toBe(117.5);
      expect(orders.placeOrder).not.toHaveBeenCalled(); expect(orders.syncTakeProfits).not.toHaveBeenCalled();
      const manual = { ...state(), managedPosition: { ...managed.managedPosition, entrySignature: 'manual-override-plan', intrusionTrail: undefined } };
      expect(monitor.intrusionTrail(manual)).toBeUndefined();
    });
    test.each(['wrong-hour', 'missing-open', 'nonprotective-open'])('new Purple refuses %s rather than falling back to a fractal SL', async invalid => {
      process.env[`${asset}_ENTRY_MODE`] = 'purple';
      orders.supportsEntrySubmissionCheckpoint = true;
      const rows = [115, 112, 110, 113, 116].map((low, i) => ({
        startedAt: iso(closeAt - (5 - i) * HOUR), open: String(low + 2), high: String(low + 5), low: String(low), close: String(low + 3)
      }));
      if (invalid === 'wrong-hour') rows[4].startedAt = iso(closeAt - 2 * HOUR);
      if (invalid === 'missing-open') (rows[4] as any).open = undefined;
      if (invalid === 'nonprotective-open') rows[4].open = '120';
      (gap.fetchDydxHourlyCandlesForMarket as jest.Mock).mockResolvedValue(rows);
      const purpleSignal = { ...signal(), setup: 'purple-green-trail', gap: { lower: 85, upper: 115, candleTime: signal().time - 3600 } };
      await expect(monitor.executeGapDailyEntry(state(), purpleSignal, {})).rejects.toThrow();
      expect(orders.placeOrder).not.toHaveBeenCalled();
      expect(orders.syncTrailingStop).not.toHaveBeenCalled();
    });
    test.each(['long', 'short'] as const)('a later real Purple %s qualifies as one Yellow trailing event without Daily reads or new entries', async direction => {
      process.env[`${asset}_ENTRY_MODE`] = 'purple';
      const nextClose = closeAt + 4 * HOUR;
      jest.setSystemTime(nextClose + 5000);
      const x = inputs(direction);
      x.page.candles.forEach((c: any) => { c.time += 4 * HOUR / 1000; c.closeTime += 4 * HOUR; });
      const first = x.page.candles[0].time;
      x.page.candles.unshift(...Array.from({ length: 4 }, (_, i) => ({
        time: first - (4 - i) * 3600, closeTime: (first - (3 - i) * 3600) * 1000 - 1,
        open: 100, high: 101, low: 99, close: 100, closed: true, volume: 1, quoteVolume: 100
      })));
      x.payload.frames.forEach(f => {
        f.time += 4 * HOUR / 1000;
        f.startedAtMs += 4 * HOUR;
        f.t = iso(f.startedAtMs).slice(0, 19).replace('T', ' ');
      });
      x.intrusions[0].timestamp = x.payload.frames[1].t;
      jest.spyOn({ BTC: binanceSpotChartFeed, ETH: ethSpotChartFeed, INJ: injSpotChartFeed, SOL: solSpotChartFeed, ZEC: zecSpotChartFeed }[asset], 'page').mockResolvedValue(x.page);
      const dailyRead = jest.spyOn(dailyHistory, 'binanceDailyFractalHistory').mockResolvedValue(x.daily as any);
      const actualPurple = await green.readPurpleGapBreakSignal(market, x.payload, x.intrusions);
      expect(actualPurple).toMatchObject({ setup: 'purple-green-trail', direction, closedAt: nextClose / 1000 });
      dailyRead.mockClear();
      const rawGreenRead = jest.spyOn(green, 'readGreenLineSignal');
      jest.spyOn(monitor, 'latestSpotIntrusions').mockReturnValue(x.intrusions);
      const isLong = direction === 'long';
      position = { market, size: isLong ? .4 : -.4, entryPrice: 120 };
      const prices = isLong ? [115, 112, 110, 113, 116] : [125, 128, 130, 127, 124];
      (gap.fetchDydxHourlyCandlesForMarket as jest.Mock).mockResolvedValue(prices.map((price, i) => ({
        startedAt: iso(nextClose - (5 - i) * HOUR), open: String(isLong ? price + 2 : price - 2),
        close: String(isLong ? price + 3 : price - 3), high: String(isLong ? price + 5 : price), low: String(isLong ? price : price - 5)
      })));
      const managed: any = { ...state(), managedPosition: { market, direction, openedAt: iso(closeAt),
        entrySignature: `${market}|GAP_DAILY|${signal().time}|${direction}`, initialSize: .4, entryPrice: 120,
        currentStop: isLong ? 100 : 140, currentStopFractalTimestamp: iso(closeAt - 3 * HOUR),
        currentStopFractalCandleSource: 'dydx-1h', currentStopFractalSource: isLong ? 'lowRef' : 'highRef',
        takeProfitMode: 'trailing-only', intrusionTrail: { entryClosedAt: closeAt / 1000 } } };
      fs.writeFileSync(file, JSON.stringify(managed));
      orders.syncTrailingStop.mockResolvedValue({ outcome: 'UPDATED', verified: true });
      await monitor.checkIntrusionTrail(managed, x.payload, {});
      expect(rawGreenRead).not.toHaveBeenCalled();
      expect(orders.syncTrailingStop).toHaveBeenCalledTimes(1);
      expect(orders.syncTrailingStop.mock.calls[0][0].static_sl).toBeCloseTo(isLong ? 109.89 : 130.13);
      const after = monitor.getStatus().managedPosition;
      expect(after.intrusionTrail.evaluatedIntrusionClosedAt).toBe(actualPurple!.closedAt);
      await monitor.checkIntrusionTrail({ ...managed, managedPosition: after }, x.payload, {});
      expect(rawGreenRead).not.toHaveBeenCalled();
      expect(orders.syncTrailingStop).toHaveBeenCalledTimes(1);
      expect(dailyRead).not.toHaveBeenCalled();
      expect(orders.placeOrder).not.toHaveBeenCalled();
      expect(orders.syncTakeProfits).not.toHaveBeenCalled();
      const mail = purpleTriggerEmail(market, actualPurple!, {}, {});
      expect(mail.body).toContain('Yellow intrusion closes, once per candle');
    });
    test.each([
      ['long', 'negative'], ['long', 'zero'], ['long', 'unavailable'],
      ['short', 'negative'], ['short', 'zero'], ['short', 'unavailable']
    ])('allows a valid purple %s signal independently from %s FD, with SL/trailing and no TPs', async (direction, condition) => {
      const delta = entry.readEthEntryFuturesDelta as jest.Mock;
      if (condition === 'unavailable') delta.mockRejectedValue(new Error('Futures unavailable'));
      else delta.mockResolvedValue({ directionalDeltaPct: condition === 'zero' ? 0 : -20, passed: false });
      const stop = direction === 'long' ? 110 : 130;
      monitor.getGapDailyStopPlan = jest.fn().mockResolvedValue({ market, price: 120,
        timestamp: '2026-10-24 01:00:00', marketInfo: { oraclePrice: 120, status: 'ACTIVE' },
        activePlan: { direction, stop: { valid: true, price: stop },
          sizing: { size: 1, minimumOrderSize: .1, notional: 120, riskBudgetUsd: 15 }, takeProfits: [] } });
      const managed: any = state(), result: any = {};
      await monitor.executeGapDailyEntry(managed, signal(direction as 'long' | 'short'), result);
      expect(result.tradePlaced).toBe(true); expect(orders.placeOrder).toHaveBeenCalledTimes(1);
      expect(delta).not.toHaveBeenCalled(); expect(orders.syncTrailingStop).toHaveBeenCalled();
      expect(orders.syncTakeProfits).not.toHaveBeenCalled();
      expect(managed.managedPosition).toMatchObject({ direction, takeProfitMode: 'trailing-only', currentStop: stop });
      expect(monitor.getStatus().gapDailyEntry.futuresDeltaFilter.enabled).toBe(false);
    });
    test('never resubmits the same candle even if the trade closed or intrusion counts changed', async () => {
      const managed: any = state();
      await monitor.executeGapDailyEntry(managed, signal(), {});
      delete managed.managedPosition; position = undefined;
      await monitor.executeGapDailyEntry(managed, { ...signal(), close: 126 }, {});
      expect(orders.placeOrder).toHaveBeenCalledTimes(1);
      expect(managed.gapDailyEntry.decision.reason).toMatch(/already handled/);
    });
    test('short entries use the protective stop above price and also have no TP orders', async () => {
      monitor.getGapDailyStopPlan = jest.fn().mockResolvedValue({ market, price: 120,
        timestamp: '2026-10-24 01:00:00', marketInfo: { oraclePrice: 120, status: 'ACTIVE' },
        activePlan: { direction: 'short', stop: { valid: true, price: 130 },
          sizing: { size: 1, minimumOrderSize: .1, notional: 120, riskBudgetUsd: 15 }, takeProfits: [] } });
      const managed: any = state(), result: any = {};
      await monitor.executeGapDailyEntry(managed, signal('short'), result);
      expect(result.tradePlaced).toBe(true);
      expect(orders.placeOrder).toHaveBeenCalledWith(expect.objectContaining({ desired_position: 'SHORT',
        signal: 'SHORT_ENTRY', static_sl: 130, profile: 'NO_TP', take_profits: [] }));
      expect(managed.managedPosition).toMatchObject({ direction: 'short', currentStop: 130, takeProfitMode: 'trailing-only' });
    });
    test.each(['pair-off', 'sl-off', 'live-off', 'executor-off', 'expired', 'position'])('blocks unsafe or inapplicable entry: %s', async kind => {
      if (kind === 'pair-off') process.env[asset === 'BTC' ? 'BTC_ENTRY_MODE' : autoEnv] = asset === 'BTC' ? 'off' : 'false';
      if (kind === 'sl-off') process.env.DECENTRADER_DYNAMIC_SL_ENABLED = 'false';
      if (kind === 'live-off') process.env.DECENTRADER_DYNAMIC_SL_LIVE_UPDATES_ENABLED = 'false';
      if (kind === 'executor-off') delete orders.syncTrailingStop;
      if (kind === 'expired') jest.setSystemTime(closeAt + 300001);
      if (kind === 'position') position = { market, size: -1, entryPrice: 120 };
      const managed: any = state();
      await monitor.executeGapDailyEntry(managed, signal(), {});
      expect(managed.gapDailyEntry.decision.outcome).toBe('SKIPPED');
      expect(orders.placeOrder).not.toHaveBeenCalled();
    });
    test('requires capacity for a protective stop even with zero TPs', async () => {
      orders.getStatefulOrderCapacity.mockResolvedValue({ limit: 20, openOrders: 20, marketOpenOrders: 0, marketOrderLimit: 0 });
      await expect(monitor.executeGapDailyEntry(state(), signal(), {})).rejects.toThrow('protective stop');
      expect(orders.placeOrder).not.toHaveBeenCalled();
    });
    test('waits for the latest dYdX closed hour instead of using a stale initial fractal stop', async () => {
      (gap.fetchDydxHourlyCandlesForMarket as jest.Mock).mockResolvedValue([115, 112, 110, 113, 116].map((low, i) => ({
        startedAt: iso(closeAt - (6 - i) * HOUR), open: String(low + 2), high: String(low + 5), low: String(low), close: String(low + 3)
      })));
      await expect(monitor.executeGapDailyEntry(state(), signal(), {})).rejects.toThrow('Awaiting the signal hour');
      expect(orders.placeOrder).not.toHaveBeenCalled();
    });
    test('refuses to submit when the durable intent cannot be written', async () => {
      jest.spyOn(fs, 'writeFileSync').mockImplementation(() => { throw new Error('disk full'); });
      await expect(monitor.executeGapDailyEntry(state(), signal(), {})).rejects.toThrow('disk full');
      expect(orders.placeOrder).not.toHaveBeenCalled();
    });
    test('arms at activation without opening a trade from old chart evidence', async () => {
      const read = jest.spyOn(green, 'readGreenLineSignal').mockResolvedValue(greenSignal());
      const managed: any = {};
      await monitor.checkGapDailyEntry(managed, inputs().payload, {});
      expect(managed.gapDailyEntry.activatedAt).toBe(iso(now));
      await monitor.checkGapDailyEntry(managed, inputs().payload, {});
      expect(read).not.toHaveBeenCalled(); expect(orders.placeOrder).not.toHaveBeenCalled();
    });
    test('retries missing Spot candles independently from Delay, then executes a fresh Green signal', async () => {
      process.env.DECENTRADER_DELAY_ENTRY_MODEL_ENABLED = 'false';
      const read = jest.spyOn(green, 'readGreenLineSignal').mockRejectedValueOnce(new Error('Spot candles unavailable')).mockResolvedValue(greenSignal());
      const managed: any = state(), first: any = {}, second: any = {};
      await monitor.checkGapDailyEntry(managed, inputs().payload, first);
      expect(first.gapDailyEntryPending).toBe(true); expect(orders.placeOrder).not.toHaveBeenCalled();
      await monitor.checkGapDailyEntry(managed, inputs().payload, second);
      expect(read).toHaveBeenCalledTimes(2); expect(second.tradePlaced).toBe(true);
    });
    test('a mixed-side Green intrusion can place an order without a Daily break, using the real detector', async () => {
      const {payload,page} = inputs();
      payload.zoneDeltas[1].push(['S', 5, 110, 1]);
      const feed = { BTC:binanceSpotChartFeed, ETH:ethSpotChartFeed, INJ:injSpotChartFeed, SOL:solSpotChartFeed, ZEC:zecSpotChartFeed }[asset];
      jest.spyOn(feed, 'page').mockResolvedValue(page);
      const daily = jest.spyOn(dailyHistory, 'binanceDailyFractalHistory').mockRejectedValue(new Error('No Daily data'));
      const managed: any = state();
      await monitor.checkGapDailyEntry(managed, payload, {});
      expect(managed.gapDailyEntry.intent.signal).toEqual(greenSignal());
      expect(orders.placeOrder).toHaveBeenCalledTimes(1);
      expect(orders.syncTrailingStop).toHaveBeenCalled();
      expect(orders.syncTakeProfits).not.toHaveBeenCalled();
      expect(daily).not.toHaveBeenCalled();
    });
    test('a wick-only break does not send an order', async () => {
      const {payload,page} = inputs();
      page.candles[1].close = payload.frames[1].close = 114;
      jest.spyOn({ BTC:binanceSpotChartFeed, ETH:ethSpotChartFeed, INJ:injSpotChartFeed, SOL:solSpotChartFeed, ZEC:zecSpotChartFeed }[asset], 'page').mockResolvedValue(page);
      const managed: any = state();
      await monitor.checkGapDailyEntry(managed, payload, {});
      expect(managed.gapDailyEntry.decision.outcome).toBe('NO_GAP_BREAK');
      expect(orders.placeOrder).not.toHaveBeenCalled();
    });
    test('an old saved signal cannot substitute for a new intrusion on the latest candle', async () => {
      const {payload} = inputs();
      payload.zoneDeltas[1].length = 0;
      Object.assign(payload, {intrusionBenchmarks: {history: [{timestamp:payload.frames[1].t,direction:'long'}]}});
      const read = jest.spyOn(green,'readGreenLineSignal').mockResolvedValue(greenSignal());
      const managed: any = state();
      await monitor.checkGapDailyEntry(managed,payload,{});
      expect(managed.gapDailyEntry.decision.outcome).toBe('NO_INTRUSION');
      expect(read).not.toHaveBeenCalled();
      expect(orders.placeOrder).not.toHaveBeenCalled();
    });
    test('a recent Purple lease cannot arm Green retroactively at rollout', async () => {
      jest.setSystemTime(closeAt - 30_000);
      new PurpleLineContinuity(market, `${file}.entry-lease.json`, () => true).heartbeat();
      jest.setSystemTime(now);
      const read = jest.spyOn(green,'readGreenLineSignal').mockResolvedValue(greenSignal());
      jest.spyOn(monitor,'check').mockResolvedValue({ok:true});
      jest.spyOn(monitor,'checkManagedPosition').mockResolvedValue({ok:true});
      monitor.start();
      await jest.advanceTimersByTimeAsync(0);
      const managed: any = state();
      await monitor.checkGapDailyEntry(managed, inputs().payload, {});
      expect(managed.gapDailyEntry.activatedAt).toBe(iso(now));
      expect(read).not.toHaveBeenCalled();
      expect(orders.placeOrder).not.toHaveBeenCalled();
    });
    test('does not register stale fills after an explicit fail-safe flatten', async () => {
      orders.placeOrder.mockResolvedValue({ outcome: 'TARGET_FAILED_FLATTENED', reason: 'flattened' });
      const managed: any = state();
      await monitor.executeGapDailyEntry(managed, signal(), {});
      expect(managed.managedPosition).toBeUndefined(); expect(managed.gapDailyEntry.intent.outcome).toBe('FLATTENED');
    });
    test('recovers a partial/uncertain submission with an SL and no TPs, without resubmitting', async () => {
      orders.placeOrder.mockImplementation(async () => {
        position = { market, size: .4, entryPrice: 120 };
        throw new Error('socket reset after submit');
      });
      const managed: any = state();
      await monitor.executeGapDailyEntry(managed, signal(), {});
      expect(managed.managedPosition.takeProfitMode).toBe('trailing-only');
      expect(managed.managedPosition.currentStop).toBe(118);
      expect(managed.managedPosition.currentStopFractalTimestamp).toBe(iso(closeAt - 3 * HOUR));
      expect(managed.gapDailyEntry.intent.outcome).toBe('CONFIRMED');
      expect(orders.syncTrailingStop).toHaveBeenCalled();
      expect(orders.syncTakeProfits).not.toHaveBeenCalled();
      const restarted = createMonitor();
      restarted.configureTradeExecutor(orders);
      process.env[entryEnv] = 'false';
      await restarted.checkManagedPosition();
      expect(orders.syncTakeProfits).not.toHaveBeenCalled(); expect(orders.placeOrder).toHaveBeenCalledTimes(1);
    });
    test('continues trailing a persisted position after disabling entries and never prepares map TPs', async () => {
      const managed: any = state(); await monitor.executeGapDailyEntry(managed, signal(), {});
      const delta = entry.readEthEntryFuturesDelta as jest.Mock;
      delta.mockClear(); delta.mockRejectedValue(new Error('FD must not gate protection of an existing position'));
      process.env[entryEnv] = 'false';
      const restarted: any = createMonitor(); restarted.configureTradeExecutor(orders);
      const plan = jest.spyOn(restarted, 'getTradePlan').mockRejectedValue(new Error('No map access allowed'));
      await restarted.checkManagedPosition();
      await restarted.syncManagedOrders(JSON.parse(fs.readFileSync(file, 'utf8')), {});
      expect(orders.syncTrailingStop.mock.calls.length).toBeGreaterThan(1);
      expect(plan).not.toHaveBeenCalled(); expect(orders.syncTakeProfits).not.toHaveBeenCalled();
      expect(delta).not.toHaveBeenCalled();
    });
    test('coalesces overlapping regular and close-aligned checks into one protective operation', async () => {
      await monitor.executeGapDailyEntry(state(), signal(), {});
      const account = await orders.getAccountSnapshot([market]);
      orders.getAccountSnapshot.mockClear(); orders.syncTrailingStop.mockClear();
      let resolve: (value: any) => void = () => {};
      const waiting = new Promise(done => { resolve = done; });
      orders.getAccountSnapshot.mockReturnValueOnce(waiting);
      const regular = monitor.checkManagedPosition(), closedHour = monitor.checkManagedPosition();
      resolve(account);
      const [first, second] = await Promise.all([regular, closedHour]);
      expect(first).toBe(second);
      expect(orders.getAccountSnapshot).toHaveBeenCalledTimes(1);
      expect(orders.syncTrailingStop).toHaveBeenCalledTimes(1);
      expect(orders.placeOrder).toHaveBeenCalledTimes(1);
      expect(orders.syncTakeProfits).not.toHaveBeenCalled();
    });
    test('reports stale dYdX Yellow-hour coverage for retries without widening a stop or repeating an entry', async () => {
      await monitor.executeGapDailyEntry(state(), signal(), {});
      const candles = await gap.fetchDydxHourlyCandlesForMarket(market);
      const oldStop = JSON.parse(fs.readFileSync(file, 'utf8')).managedPosition.currentStop;
      jest.setSystemTime(closeAt + HOUR + 2000);
      const saved = JSON.parse(fs.readFileSync(file, 'utf8'));
      saved.managedPosition.intrusionTrail.lastIntrusionClosedAt = (closeAt + HOUR) / 1000;
      fs.writeFileSync(file, JSON.stringify(saved));
      const stale = await monitor.checkManagedPosition();
      expect(stale).toMatchObject({ ok: true, trailingCandle: { source: 'dydx-1h', pending: true,
        closedAt: iso(closeAt), expectedClosedAt: iso(closeAt + HOUR) } });
      expect(JSON.parse(fs.readFileSync(file, 'utf8')).managedPosition.currentStop).toBe(oldStop);
      (gap.fetchDydxHourlyCandlesForMarket as jest.Mock).mockResolvedValue([...candles!, {
        startedAt: iso(closeAt), open: '119', close: '119', high: '120', low: '118'
      }]);
      const fresh = await monitor.checkManagedPosition();
      expect(fresh.trailingCandle).toMatchObject({ pending: false, closedAt: iso(closeAt + HOUR) });
      expect(orders.placeOrder).toHaveBeenCalledTimes(1); expect(orders.syncTakeProfits).not.toHaveBeenCalled();
      // Old pending evidence must not survive after the position has been cleared.
      const cleared = JSON.parse(fs.readFileSync(file, 'utf8')); delete cleared.managedPosition;
      fs.writeFileSync(file, JSON.stringify(cleared));
      expect((await monitor.checkManagedPosition()).trailingCandle).toBeUndefined();
    });
    test.each(['manual', 'off'])('preserves automatic trailing-only ownership after switching to %s', async mode => {
      await monitor.executeGapDailyEntry(state(), signal(), {});
      const previous = orders.syncTrailingStop.mock.calls.length;
      process.env[`${asset}_ENTRY_MODE`] = mode;
      const restarted: any = createMonitor(); restarted.configureTradeExecutor(orders);
      const map = jest.spyOn(restarted, 'getTradePlan').mockRejectedValue(new Error('No map TP planning for purple positions'));
      const result = await restarted.checkManagedPosition();
      expect(result.ok).toBe(true); expect(orders.syncTrailingStop.mock.calls.length).toBeGreaterThan(previous);
      expect(orders.placeOrder).toHaveBeenCalledTimes(1); expect(orders.syncTakeProfits).not.toHaveBeenCalled();
      expect(map).not.toHaveBeenCalled();
      expect(JSON.parse(fs.readFileSync(file, 'utf8')).managedPosition.takeProfitMode).toBe('trailing-only');
    });
    test.each(['fractal', 'midpoint', 'quarter'])('recovers a delayed legacy %s fill without moving its recorded stop or repeating an entry', async legacy => {
      orders.placeOrder.mockRejectedValue(new Error('uncertain submission'));
      const managed: any = state();
      await expect(monitor.executeGapDailyEntry(managed, signal(), {})).rejects.toThrow('uncertain submission');
      expect(managed.gapDailyEntry.intent.outcome).toBe('UNKNOWN');
      // Existing submissions keep the submitted SL rather than acquiring the new body-open policy.
      delete managed.gapDailyEntry.intent.futuresDelta;
      delete managed.gapDailyEntry.intent.orderAlert.gap_daily_futures_delta;
      const legacyPlan = managed.gapDailyEntry.intent.plan.activePlan;
      if (legacy === 'fractal') {
        legacyPlan.stop = legacyPlan.sizingStop;
        delete legacyPlan.sizingStop; delete legacyPlan.initialStopPolicy;
      } else if (legacy === 'midpoint') {
        legacyPlan.stop = { ...legacyPlan.stop, price: 118.5, source: 'dydx-signal-body-midpoint' };
        legacyPlan.initialStopPolicy = { mode: 'signal-body-midpoint', candleSource: 'dydx-1h', midpoint: 118.5 };
      } else {
        legacyPlan.stop = { ...legacyPlan.stop, price: 118.25, source: 'dydx-signal-body-quarter' };
        legacyPlan.initialStopPolicy = { mode: 'signal-body-quarter', bodyFraction: .25, candleSource: 'dydx-1h', bodyStopPrice: 118.25 };
      }
      managed.gapDailyEntry.intent.orderAlert.static_sl = legacyPlan.stop.price;
      delete managed.gapDailyEntry.intent.orderAlert.decentrader.initialStopPolicy;
      delete managed.gapDailyEntry.intent.orderAlert.decentrader.sizingStop;
      fs.writeFileSync(file, JSON.stringify(managed));
      const delta = entry.readEthEntryFuturesDelta as jest.Mock;
      delta.mockClear(); delta.mockRejectedValue(new Error('FD unavailable during protective recovery'));
      position = { market, size: .4, entryPrice: 120 };
      process.env[entryEnv] = 'false';
      const restarted = createMonitor(); restarted.configureTradeExecutor(orders);
      const result = await restarted.checkManagedPosition();
      expect(result.ok).toBe(true);
      expect(JSON.parse(fs.readFileSync(file, 'utf8')).managedPosition.takeProfitMode).toBe('trailing-only');
      expect(JSON.parse(fs.readFileSync(file, 'utf8')).managedPosition.currentStop).toBe(legacy === 'fractal' ? 109.89 : legacy === 'midpoint' ? 118.5 : 118.25);
      expect(orders.syncTrailingStop).toHaveBeenCalled();
      expect(orders.placeOrder).toHaveBeenCalledTimes(1); expect(orders.syncTakeProfits).not.toHaveBeenCalled();
      expect(delta).not.toHaveBeenCalled();
    });
    test('a restart does not turn a pre-restart purple candle into a fresh entry', async () => {
      const restarted: any = createMonitor(); restarted.configureTradeExecutor(orders);
      const read = jest.spyOn(green, 'readGreenLineSignal').mockResolvedValue(greenSignal());
      const managed: any = state();
      await restarted.checkGapDailyEntry(managed, inputs().payload, {});
      expect(managed.gapDailyEntry.activatedAt).toBe(iso(now));
      expect(read).not.toHaveBeenCalled(); expect(orders.placeOrder).not.toHaveBeenCalled();
    });
    test.each([false, true])('reports a risk failure with broadcast checkpoint crossed: %s', async broadcasting => {
      orders.supportsEntrySubmissionCheckpoint = true;
      jest.spyOn(green, 'readGreenLineSignal').mockResolvedValue(greenSignal());
      const error = broadcasting ? 'Uncertain submission' : 'Entry risk guard: protective stop is no longer valid against the current dYdX oracle.';
      orders.placeOrder.mockImplementationOnce(async (_alert, execution) => {
        if (broadcasting) await execution.beforeEntrySubmit();
        throw new Error(error);
      });
      await monitor.checkGapDailyEntry(state(), inputs().payload, {});
      const status = monitor.getStatus().gapDailyEntry;
      const expected = broadcasting ? 'UNKNOWN' : 'NOT_SUBMITTED';
      expect(status.latestExecutionAttempt).toMatchObject({
        phase: broadcasting ? 'BROADCASTING' : 'PREPARING', outcome: expected, error
      });
      expect(Boolean(status.latestExecutionAttempt.broadcastStartedAt)).toBe(broadcasting);
      expect(status.triggerEmail.recent[0].outcome).toBe(broadcasting ? 'UNCONFIRMED' : 'NOT_SUBMITTED');
      expect(orders.placeOrder).toHaveBeenCalledTimes(1);
      expect(orders.syncTrailingStop).not.toHaveBeenCalled();
    });
    test('saves signal mail and a PREPARING checkpoint before execution, then revalidates an unsent fresh signal after restart', async () => {
      orders.supportsEntrySubmissionCheckpoint = true;
      jest.spyOn(green, 'readGreenLineSignal').mockResolvedValue(greenSignal());
      let crashed: any;
      orders.placeOrder.mockImplementationOnce(async () => {
        crashed = JSON.parse(fs.readFileSync(file, 'utf8'));
        expect(crashed.gapDailyEntry.intent).toMatchObject({ phase: 'PREPARING', outcome: 'SUBMITTING' });
        expect(crashed.gapDailyEntry.triggerEmails[0]).toMatchObject({ outcome: 'DETECTED', attempts: 0 });
        throw new Error('simulated interruption before SDK broadcast');
      });
      await monitor.checkGapDailyEntry(state(), inputs().payload, {});
      fs.writeFileSync(file, JSON.stringify(crashed));
      const restarted: any = createMonitor(); restarted.processStartedAt = closeAt - HOUR;
      restarted.configureTradeExecutor(orders);
      await restarted.checkManagedPosition();
      const recovered = JSON.parse(fs.readFileSync(file, 'utf8'));
      expect(recovered.gapDailyEntry.intent.outcome).toBe('NOT_SUBMITTED');
      expect(recovered.gapDailyEntry.handledCandle).toBeUndefined();
      expect(orders.placeOrder).toHaveBeenCalledTimes(1);
      const actualBroadcast = jest.fn();
      orders.placeOrder.mockImplementationOnce(async (alert, execution) => {
        expect(execution.requireFlat).toBe(true);
        await execution.beforeEntrySubmit();
        expect(JSON.parse(fs.readFileSync(file, 'utf8')).gapDailyEntry.intent.phase).toBe('BROADCASTING');
        actualBroadcast(); position = { market, size: .4, entryPrice: 120 };
      });
      const result: any = {};
      await restarted.checkGapDailyEntry(recovered, inputs().payload, result);
      expect(result.tradePlaced).toBe(true); expect(actualBroadcast).toHaveBeenCalledTimes(1);
      expect(orders.syncTrailingStop).toHaveBeenCalled(); expect(orders.syncTakeProfits).not.toHaveBeenCalled();
      const status = restarted.getStatus().gapDailyEntry;
      expect(status.latestExecutionAttempt).toMatchObject({ phase: 'BROADCASTING', outcome: 'CONFIRMED', signal: { time: signal().time } });
      expect(status.recentDecisions.some(row => row.decision.outcome === 'RETRY_SAFE_BEFORE_BROADCAST')).toBe(true);
    });
    test.each(['expired', 'disabled', 'checkpoint-write-failed'])('does not start a broadcast when %s during preparation', async why => {
      orders.supportsEntrySubmissionCheckpoint = true;
      const broadcast = jest.fn();
      orders.placeOrder.mockImplementationOnce(async (_alert, execution) => {
        if (why === 'expired') jest.setSystemTime(closeAt + 300001);
        if (why === 'disabled') process.env[entryEnv] = 'false';
        if (why === 'checkpoint-write-failed') jest.spyOn(fs, 'fsyncSync').mockImplementation(() => { throw new Error('checkpoint disk failure'); });
        await execution.beforeEntrySubmit();
        broadcast();
      });
      await expect(monitor.executeGapDailyEntry(state(), signal(), {})).rejects.toThrow();
      expect(broadcast).not.toHaveBeenCalled();
      expect(position).toBeUndefined();
    });
    test('rechecks manual ownership at the actual automatic broadcast boundary', async () => {
      process.env[`${asset}_ENTRY_MODE`] = 'green';
      orders.supportsEntrySubmissionCheckpoint = true;
      const broadcast = jest.fn();
      orders.placeOrder.mockImplementationOnce(async (_alert, execution) => {
        if (asset === 'BTC') fs.writeFileSync(process.env.DECENTRADER_GAP_ALERT_STATE_FILE!, JSON.stringify({ managedPosition: { market } }));
        else fs.writeFileSync(process.env[`${asset}_MANUAL_TRADE_OVERRIDE_FILE`]!, JSON.stringify({ version: 3, market, overrides: [{ status: 'EXECUTING' }] }));
        await execution.beforeEntrySubmit(); broadcast();
      });
      await expect(monitor.executeGapDailyEntry(state(), signal(), {})).rejects.toThrow(/still manages|unfinished/);
      expect(broadcast).not.toHaveBeenCalled(); expect(position).toBeUndefined();
    });
    test.each(['BROADCASTING', 'legacy'])('never resends an ambiguous %s attempt after restart', async phase => {
      orders.supportsEntrySubmissionCheckpoint = true;
      let crashed: any;
      orders.placeOrder.mockImplementationOnce(async (_alert, execution) => {
        if (phase === 'BROADCASTING') await execution.beforeEntrySubmit();
        crashed = JSON.parse(fs.readFileSync(file, 'utf8'));
        if (phase === 'legacy') delete crashed.gapDailyEntry.intent.phase;
        throw new Error('simulated interruption around submission');
      });
      await expect(monitor.executeGapDailyEntry(state(), signal(), {})).rejects.toThrow();
      fs.writeFileSync(file, JSON.stringify(crashed));
      const restarted: any = createMonitor(); restarted.processStartedAt = closeAt - HOUR;
      restarted.configureTradeExecutor(orders);
      await restarted.checkManagedPosition();
      const recovered = JSON.parse(fs.readFileSync(file, 'utf8'));
      jest.spyOn(green, 'readGreenLineSignal').mockResolvedValue(greenSignal());
      await restarted.checkGapDailyEntry(recovered, inputs().payload, {});
      expect(orders.placeOrder).toHaveBeenCalledTimes(1);
      expect(recovered.gapDailyEntry.handledCandle).toBe(signal().closedAt * 1000);
    });
    test.each(['expired', 'disabled', 'position'])('does not rearm an interrupted unsent signal when %s', async why => {
      orders.supportsEntrySubmissionCheckpoint = true;
      let crashed: any;
      orders.placeOrder.mockImplementationOnce(async () => {
        crashed = JSON.parse(fs.readFileSync(file, 'utf8'));
        throw new Error('interrupted preparation');
      });
      await expect(monitor.executeGapDailyEntry(state(), signal(), {})).rejects.toThrow();
      if (why === 'expired') jest.setSystemTime(closeAt + 300001);
      if (why === 'disabled') process.env[entryEnv] = 'false';
      if (why === 'position') position = { market, size: -.4, entryPrice: 120 };
      fs.writeFileSync(file, JSON.stringify(crashed));
      const restarted: any = createMonitor(); restarted.configureTradeExecutor(orders);
      await restarted.checkManagedPosition();
      const recovered = JSON.parse(fs.readFileSync(file, 'utf8'));
      expect(recovered.gapDailyEntry.intent.outcome).toBe('NOT_SUBMITTED');
      expect(recovered.gapDailyEntry.handledCandle).toBe(signal().closedAt * 1000);
      expect(recovered.managedPosition).toBeUndefined();
      expect(orders.placeOrder).toHaveBeenCalledTimes(1);
    });
    test('preserves an interrupted signal for diagnostics but does not mail an unconfirmed entry', async () => {
      jest.spyOn(green, 'readGreenLineSignal').mockResolvedValue(greenSignal());
      let crashed: any;
      orders.placeOrder.mockImplementationOnce(async () => {
        crashed = JSON.parse(fs.readFileSync(file, 'utf8'));
        throw new Error('crash before execution finished');
      });
      await monitor.checkGapDailyEntry(state(), inputs().payload, {});
      fs.writeFileSync(file, JSON.stringify(crashed));
      (gap.smtpSettingsFromEnv as jest.Mock).mockReturnValue({ host: 'fixture.invalid' });
      const restarted: any = createMonitor(); restarted.configureTradeExecutor(orders);
      await restarted.flushPurpleTriggerEmails();
      expect(gap.sendEmailBestEffort).not.toHaveBeenCalled();
      expect(restarted.getStatus().gapDailyEntry.triggerEmail.recent[0].smtpSentAt).toBeUndefined();
      expect(orders.placeOrder).toHaveBeenCalledTimes(1);
    });
    test('reconciles an uncertain fill after five seconds without waiting for the regular management interval', async () => {
      orders.placeOrder.mockRejectedValue(new Error('uncertain submission'));
      const managed: any = state();
      await expect(monitor.executeGapDailyEntry(managed, signal(), {})).rejects.toThrow();
      position = { market, size: .4, entryPrice: 120 };
      await jest.advanceTimersByTimeAsync(5000);
      expect(JSON.parse(fs.readFileSync(file, 'utf8')).managedPosition.takeProfitMode).toBe('trailing-only');
      expect(orders.placeOrder).toHaveBeenCalledTimes(1); expect(orders.syncTakeProfits).not.toHaveBeenCalled();
    });

    test('retries old-position stop coverage after restart and backs off until dYdX confirms full coverage', async () => {
      const managed: any = state();
      await monitor.executeGapDailyEntry(managed, greenSignal(), {});
      monitor.stop();
      process.env[`${asset}_ENTRY_MODE`] = 'off';
      jest.setSystemTime(now + 18 * 60_000);
      position = { market, size: .2, entryPrice: 120 };
      orders.syncTrailingStop.mockClear();
      orders.syncTrailingStop.mockResolvedValue({ outcome: 'PENDING_VERIFICATION', verified: false });
      monitor = createMonitor();
      monitor.configureTradeExecutor(orders);
      const firstCheck = Date.now();
      await monitor.checkManagedPosition();
      expect(monitor.getStatus().stopCoverage).toMatchObject({
        state: 'UNVERIFIED', unverifiedSince: iso(firstCheck), nextCheckAt: iso(firstCheck + 5000)
      });
      await jest.advanceTimersByTimeAsync(5 * 60_000);
      const stalled = monitor.getStatus().stopCoverage;
      expect(stalled).toMatchObject({ state: 'UNVERIFIED', nextCheckAt: iso(Date.now() + 30_000) });
      expect(orders.syncTrailingStop.mock.calls.length).toBeGreaterThan(10);
      expect(orders.syncTrailingStop.mock.calls.length).toBeLessThan(20);
      for (const [alert, options] of orders.syncTrailingStop.mock.calls) {
        expect(alert).toMatchObject({ market, trail_stop: 118, size: .2 });
        expect(options).toEqual({ requireVisibleReplacement: true });
      }
      orders.syncTrailingStop.mockResolvedValue({ outcome: 'UNCHANGED', visibility: 'DYDX_OPEN_ORDER' });
      await jest.advanceTimersByTimeAsync(30_000);
      expect(monitor.getStatus().stopCoverage).toEqual({ state: 'VERIFIED', checkedAt: iso(Date.now()) });
      const completedChecks = orders.syncTrailingStop.mock.calls.length;
      await jest.advanceTimersByTimeAsync(120_000);
      expect(orders.syncTrailingStop).toHaveBeenCalledTimes(completedChecks);
      expect(orders.placeOrder).toHaveBeenCalledTimes(1);
      expect(orders.syncTakeProfits).not.toHaveBeenCalled();
      expect(gap.fetchDydxHourlyCandlesForMarket).toHaveBeenCalledTimes(1);
      expect(JSON.parse(fs.readFileSync(file, 'utf8')).managedPosition.currentStop).toBe(118);
    });

    test('resumes coverage verification without a persisted entry intent and does not trust local stop memory', async () => {
      const managed: any = state();
      await monitor.executeGapDailyEntry(managed, greenSignal(), {});
      delete managed.gapDailyEntry;
      fs.writeFileSync(file, JSON.stringify(managed));
      jest.setSystemTime(now + 20 * 60_000);
      orders.getAccountSnapshot.mockRejectedValueOnce(new Error('fixture indexer unavailable'));
      orders.syncTrailingStop
        .mockResolvedValueOnce({ outcome: 'UNCHANGED', visibility: 'RENDER_MEMORY_UNCONFIRMED' })
        .mockResolvedValueOnce({ outcome: 'UNCHANGED_WITH_DUPLICATES', visibility: 'DYDX_OPEN_ORDER_AGGREGATE' });
      await monitor.checkManagedPosition();
      expect(monitor.getStatus().stopCoverage).toMatchObject({ state: 'UNVERIFIED', reason: 'fixture indexer unavailable' });
      await jest.advanceTimersByTimeAsync(5000);
      expect(monitor.getStatus().stopCoverage).toMatchObject({ state: 'UNVERIFIED' });
      await jest.advanceTimersByTimeAsync(5000);
      expect(monitor.getStatus().stopCoverage).toMatchObject({ state: 'VERIFIED' });
      expect(orders.placeOrder).toHaveBeenCalledTimes(1);
    });

    test('does not fast-retry ownership mismatches, disabled stops, or a stopped monitor', async () => {
      const managed: any = state();
      await monitor.executeGapDailyEntry(managed, greenSignal(), {});
      const initialPosition = position;
      position = { ...position, size: 2 };
      await monitor.checkManagedPosition();
      expect(monitor.getStatus().stopCoverage).toMatchObject({ state: 'BLOCKED' });
      expect(monitor.getStatus().stopCoverage.nextCheckAt).toBeUndefined();
      position = initialPosition;
      process.env.DECENTRADER_DYNAMIC_SL_ENABLED = 'false';
      await monitor.checkManagedPosition();
      expect(monitor.getStatus().stopCoverage).toMatchObject({ state: 'DISABLED' });
      expect(monitor.getStatus().stopCoverage.nextCheckAt).toBeUndefined();
      process.env.DECENTRADER_DYNAMIC_SL_ENABLED = 'true';
      orders.syncTrailingStop.mockResolvedValue({ outcome: 'PENDING_CAPACITY', verified: false });
      await monitor.checkManagedPosition();
      expect(monitor.getStatus().stopCoverage).toMatchObject({ state: 'UNVERIFIED' });
      const checks = orders.syncTrailingStop.mock.calls.length;
      monitor.stop();
      await jest.advanceTimersByTimeAsync(30_000);
      expect(orders.syncTrailingStop).toHaveBeenCalledTimes(checks);
      expect(orders.placeOrder).toHaveBeenCalledTimes(1);
    });
    test('blocks competing old entry routes, preserving plans, and leaves other BTC settings unchanged', async () => {
      expect((await monitor.executeFractalEntry({ market })).tradeSkipped).toMatch(/entry selector/);
      expect(await monitor.executeManualEntry({ market })).toMatchObject({ tradeDeferred: true });
      const result: any = {}; await monitor.executeAlertExclusive({}, {}, 'old-delay', result);
      expect(result.tradeSkipped).toMatch(/entry selector/);
      expect(orders.placeOrder).not.toHaveBeenCalled();
      expect(openLiquidityV2BtcIntrusionMonitor.getStatus()).toMatchObject({ market: 'BTC-USD', autoTradeEnabled: asset === 'BTC' });
    });
  });
});
