import { btcSpotChartRouter } from '../src/controllers/btcSpotChart';
import { buildGreenRetestWithHistory, readRetestHistory, type RetestMode } from '../src/research/greenRetestHistory';
import { injSpotChartFeed } from '../src/services/binanceSpotChart';
import { injIntrusionFuturesDeltaFeed } from '../src/services/intrusionFuturesDelta';
import { injIntrusionSpotPocFeed } from '../src/services/intrusionSpotPoc';
import { openLiquidityV2InjCollector } from '../src/services/openLiquidityV2Replica';
import { PURPLE_LINE_SETUPS } from '../src/services/purpleLineSetup';

const { streamCandle } = require('../public/btc-spot-chart/data.js');
const { readReplay } = require('../public/btc-spot-chart/historical-replay.js');
const market = 'INJ-USD', symbol = 'INJUSDT';

test('INJ chart routes and feeds use the native Spot replica, not another pair', () => {
  expect(PURPLE_LINE_SETUPS[market]).toMatchObject({ symbol, priceStepUsd: .01,
    modelVersion: (openLiquidityV2InjCollector as any).config.modelVersion });
  for (const feed of [injSpotChartFeed, injIntrusionFuturesDeltaFeed, injIntrusionSpotPocFeed]) {
    expect(feed).toMatchObject({ symbol });
  }
  const routes = new Set(btcSpotChartRouter.stack.map(layer => layer.route?.path));
  for (const name of ['spot-candles', 'spot-fractal-projections', 'intrusion-spot-poc', 'intrusion-futures-delta']) {
    expect(routes.has(`/open-liquidity/v2/inj-${name}`)).toBe(true);
  }
  const t = Date.UTC(2026, 10, 20, 6), T = t + 3_600_000 - 1;
  const msg = { e: 'kline', E: T, s: symbol, k: { s: symbol, i: '1h', x: true,
    t, T, o: '6.644', h: '6.669', l: '6.561', c: '6.642', v: '1', q: '6.642' } };
  expect(streamCandle(msg, symbol)).toMatchObject({ open: 6.644, close: 6.642, closed: true });
  expect(streamCandle({ ...msg, s: 'ETHUSDT' }, symbol)).toBeNull();
  expect(readReplay(require('../public/btc-spot-chart/inj-green-replay.json'), 'green', symbol).market).toBe(market);
});

test.each(['green', 'purple', 'atr', 'atr-purple'] as RetestMode[])('%s INJ retest reaches the archive end', mode => {
  const history = readRetestHistory(market, mode)!;
  expect(history.context.from).toBe(Date.UTC(2025, 10, 21, 12));
  const result = buildGreenRetestWithHistory(history, undefined, mode);
  expect(result).toMatchObject({ market, strategy: mode, missingDydxHours: 0, through: Date.UTC(2026, 10, 20, 7) });
  expect(result.incompleteAt).toBeUndefined();
  expect(result.trades.some(trade => trade.status === 'CLOSED')).toBe(true);
});
