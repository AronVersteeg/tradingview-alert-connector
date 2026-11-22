import { btcSpotChartRouter } from '../src/controllers/btcSpotChart';
import { buildGreenRetestWithHistory, readRetestHistory, type RetestMode } from '../src/research/greenRetestHistory';
import { wldSpotChartFeed } from '../src/services/binanceSpotChart';
import { wldIntrusionFuturesDeltaFeed } from '../src/services/intrusionFuturesDelta';
import { wldIntrusionSpotPocFeed } from '../src/services/intrusionSpotPoc';
import { openLiquidityV2WldCollector } from '../src/services/openLiquidityV2Replica';
import { PURPLE_LINE_SETUPS } from '../src/services/purpleLineSetup';

const { streamCandle } = require('../public/btc-spot-chart/data.js');
const { readReplay } = require('../public/btc-spot-chart/historical-replay.js');
const market = 'WLD-USD', symbol = 'WLDUSDT';

test('WLD routes and feeds use their own native Spot market', () => {
  expect(openLiquidityV2WldCollector).toMatchObject({ config: { market, symbol, priceStepUsd: .005 } });
  expect(PURPLE_LINE_SETUPS[market]).toMatchObject({ symbol, priceStepUsd: .005,
    modelVersion: (openLiquidityV2WldCollector as any).config.modelVersion });
  for (const feed of [wldSpotChartFeed, wldIntrusionFuturesDeltaFeed, wldIntrusionSpotPocFeed]) {
    expect(feed).toMatchObject({ symbol });
  }
  const routes = new Set(btcSpotChartRouter.stack.map(layer => layer.route?.path));
  for (const name of ['spot-candles', 'spot-fractal-projections', 'intrusion-spot-poc', 'intrusion-futures-delta']) {
    expect(routes.has(`/open-liquidity/v2/wld-${name}`)).toBe(true);
  }
  const t = Date.UTC(2026, 10, 20, 6), T = t + 3_600_000 - 1;
  const msg = { e: 'kline', E: T, s: symbol, k: { s: symbol, i: '1h', x: true,
    t, T, o: '0.2461', h: '0.2500', l: '0.2401', c: '0.2422', v: '1', q: '0.2422' } };
  expect(streamCandle(msg, symbol)).toMatchObject({ open: .2461, close: .2422, closed: true });
  expect(streamCandle({ ...msg, s: 'BTCUSDT' }, symbol)).toBeNull();
  expect(readReplay(require('../public/btc-spot-chart/wld-green-replay.json'), 'green', symbol).market).toBe(market);
});

test.each(['green', 'purple', 'atr', 'atr-purple'] as RetestMode[])('%s WLD retest reaches its archive end', mode => {
  const history = readRetestHistory(market, mode)!;
  expect(history.context.from).toBe(Date.UTC(2025, 10, 21, 12));
  const result = buildGreenRetestWithHistory(history, undefined, mode);
  expect(result).toMatchObject({ market, strategy: mode, missingDydxHours: 0, through: Date.UTC(2026, 10, 20, 7) });
  expect(result.incompleteAt).toBeUndefined();
  expect(result.trades.some(trade => trade.status === 'CLOSED')).toBe(true);
});
