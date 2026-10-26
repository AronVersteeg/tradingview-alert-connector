const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require('playwright');
const { PNG } = require('pngjs');
const [url, output] = process.argv.slice(2);
if (!url || !output) throw new Error('Supply a read-only preview URL and screenshot directory');
fs.mkdirSync(output, { recursive: true });
const HOUR = 3600000, initialNow = Math.floor(Date.now() / HOUR) * HOUR + 5000;
const start = Math.floor(initialNow / HOUR) * HOUR - 150 * HOUR;
const iso = ms => new Date(ms).toISOString();
function candles(now) {
  return Array.from({ length: Math.floor((now - start) / HOUR) + 1 }, (_, i) => {
    const values = { 25: [80200, 81000, 79000, 80500, 1000], 60: [82500, 83500, 81500, 82000, 1100],
      80: [79900, 81200, 78800, 80000, 1], 100: [80600, 81000, 80000, 80500, 1200],
      115: [81000, 84000, 80800, 81050, 1], 150: [81000, 81200, 79900, 81050, 1] }[i] || [81000, 81200, 80800, 81050, 1];
    const [open, high, low, close, volume] = values;
    return { time: (start + i * HOUR) / 1000, open, high, low, close, volume, quoteVolume: volume * close,
      closed: start + (i + 1) * HOUR <= now };
  });
}
const frames = page => page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
const pixel = (buffer, point) => {
  const png = PNG.sync.read(buffer), at = (Math.round(point.y) * png.width + Math.round(point.x)) * 4;
  return [...png.data.subarray(at, at + 3)];
};
(async () => {
  const browser = await chromium.launch({ headless: true, channel: process.env.SPOT_CHART_BROWSER_CHANNEL || 'msedge' });
  const results = [];
  try {
    for (const mobile of [false, true]) {
      let now = initialNow, olderReads = 0;
      const errors = [], writes = [];
      const context = await browser.newContext({ viewport: mobile ? { width: 390, height: 844 } : { width: 1440, height: 1000 },
        isMobile: mobile, hasTouch: mobile, deviceScaleFactor: 1 });
      await context.addInitScript(value => { window.__obNow = value; Date.now = () => window.__obNow; window.WebSocket = class { close() {} }; }, now);
      await context.route('**/*', route => {
        const request = route.request(), target = new URL(request.url());
        if (!['GET', 'HEAD', 'OPTIONS'].includes(request.method())) { writes.push(request.method()); return route.abort(); }
        if (target.origin === new URL(url).origin && (target.pathname === '/decentrader/map' || target.pathname.startsWith('/assets/'))) return route.continue();
        if (target.pathname.endsWith('/btc-spot-candles')) {
          const before = Number(target.searchParams.get('before'));
          if (before) olderReads++;
          const rows = candles(now).filter(c => !before || c.time * 1000 < before);
          return route.fulfill({ json: { symbol: 'BTCUSDT', venue: 'Binance Spot', interval: '1h', fetchedAt: iso(now),
            candles: before ? rows : rows.slice(50), hasMore: !before } });
        }
        if (target.pathname.endsWith('/btc-spot-fractal-projections')) {
          const snapshot = interval => ({ venue: 'Binance Spot', symbol: 'BTCUSDT', interval,
            latestClosedCandleAt: iso(Math.floor(now / 86400000) * 86400000 - 1), records: [] });
          return route.fulfill({ json: { symbol: 'BTCUSDT', venue: 'Binance Spot', evaluatedThrough: iso(Math.floor(now / HOUR) * HOUR),
            daily: snapshot('1d'), weekly: snapshot('1w'), monthly: snapshot('1M') } });
        }
        if (target.pathname === '/open-liquidity/v2/liquidity-timelapse') {
          const rows = candles(now).filter(c => c.closed);
          return route.fulfill({ json: { modelVersion: 'binance-spot-liquidation-cohorts-v2.4',
            source: { market: 'BTC-USD', sourceStatuses: [{ name: 'Binance Spot BTCUSDT 1H' }] },
            range: { minPrice: 78000, maxPrice: 85000 }, events: [],
            frames: rows.map((c, i) => ({ i, startedAtMs: c.time * 1000, t: iso(c.time * 1000).slice(0, 19).replace('T', ' '), price: c.close })),
            gaps: rows.map(() => ({ left: 78500, right: 84500, width: 6000,
              leftEdge: { leverage: 10, relativeCount: 1 }, rightEdge: { leverage: 10, relativeCount: 1 } })) } });
        }
        if (target.pathname === '/decentrader/manual-override') return route.fulfill({ json: { enabled: true, market: 'BTC-USD', overrides: [] } });
        return route.fulfill({ status: 503, json: { error: 'Unrelated source isolated in read-only fixture' } });
      });
      const page = await context.newPage(); page.on('pageerror', error => errors.push(error.message));
      await page.goto(url); await page.locator('#btcSpotChartPanel').scrollIntoViewIfNeeded();
      await page.waitForFunction(() => window.btcSpotChart?.getOrderBlocks()?.candleCount === 150);
      assert.equal(olderReads, 1, 'Older candles should be fetched once and merged without duplicates');
      for (const id of ['HourlyBoxes', 'Gap', 'Daily', 'Weekly', 'Monthly', 'GapWeeklyBreaks', 'GapDailyBreaks', 'IntrusionDailyBreaks', 'Manual']) {
        await page.locator('#spotChart' + id).uncheck();
      }
      await page.locator('[data-spot-range="all"]').click();
      await page.locator('#spotChartCanvas').scrollIntoViewIfNeeded(); await frames(page);
      const model = await page.evaluate(() => window.btcSpotChart.getOrderBlocks());
      assert.equal(model.blocks.length, 3); assert.equal(model.blocks.filter(b => b.state === 'broken').length, 2);
      const points = await page.evaluate(() => {
        const api = window.btcSpotChart, rows = api.getCandles(), blocks = api.getOrderBlocks().blocks;
        return Object.fromEntries(blocks.filter(b => b.direction === 'bullish').map(b => [b.state, {
          x: api.chart.timeScale().logicalToCoordinate(rows.findIndex(c => c.time === b.originTime) + 10),
          y: api.candleSeries.priceToCoordinate(b.bottom + (b.top - b.bottom) * .25)
        }]));
      });
      const canvas = page.locator('#spotChartCanvas'), on = await canvas.screenshot();
      await page.screenshot({ path: path.join(output, mobile ? 'order-blocks-mobile.png' : 'order-blocks-desktop.png') });
      await page.locator('#spotChartOrderBlocks').uncheck(); await frames(page);
      const off = await canvas.screenshot();
      const activeDelta = pixel(on, points.intact)[1] - pixel(off, points.intact)[1];
      const brokenDelta = pixel(on, points.broken)[1] - pixel(off, points.broken)[1];
      assert(activeDelta > 15, `Nonblank active green fill required: ${activeDelta}`);
      assert(brokenDelta > 5 && brokenDelta < activeDelta * .7 && brokenDelta > activeDelta * .3,
        `Broken fill should be about half strength: ${brokenDelta}/${activeDelta}`);
      await page.locator('#spotChartOrderBlocks').check();
      await page.locator('#spotChartGap').check(); await frames(page);
      await page.waitForFunction(() => window.btcSpotChart.getOrderBlockLabels().length === 1);
      const label = await page.evaluate(() => {
        const api = window.btcSpotChart, label = api.getOrderBlockLabels()[0];
        const edge = api.chart.timeScale().logicalToCoordinate(api.getCandles().length - 1 + Number(document.getElementById('spotChartGapLead').value));
        return { ...label, edge, paneWidth: api.chart.timeScale().width() };
      });
      assert.equal(label.bottom, 80000); assert.equal(label.top, 80500); assert.equal(label.x, label.edge + 10);
      assert(label.x + label.width <= label.paneWidth - 3, 'Full price label must fit in the right column');
      const labeled = await canvas.screenshot();
      let labelPixels = 0; const png = PNG.sync.read(labeled);
      for (let y = Math.floor(label.y); y < Math.ceil(label.y + label.height); y++) {
        for (let x = Math.floor(label.x); x < Math.ceil(label.x + label.width); x++) {
          const i = (y * png.width + x) * 4;
          if (png.data[i] > 65 && png.data[i + 1] > 90 && png.data[i + 2] > 60) labelPixels++;
        }
      }
      assert(labelPixels > 10, 'Price text must actually render on the canvas');
      await page.screenshot({ path: path.join(output, mobile ? 'order-block-labels-mobile.png' : 'order-block-labels-desktop.png') });
      await page.locator('#spotChartGapLead').fill('20'); await page.locator('#spotChartGapLead').press('Tab'); await frames(page);
      const shifted = await page.evaluate(() => {
        const api = window.btcSpotChart;
        return { label: api.getOrderBlockLabels()[0], edge: api.chart.timeScale().logicalToCoordinate(api.getCandles().length - 1 + 20) };
      });
      assert.equal(shifted.label.x, shifted.edge + 10);
      await page.locator('#spotChartGap').uncheck(); await frames(page);
      assert.deepEqual(await page.evaluate(() => window.btcSpotChart.getOrderBlockLabels()), []);
      await page.locator('#spotChartGapLead').fill('10'); await page.locator('#spotChartGapLead').press('Tab');
      await page.locator('#spotChartGap').check();
      await page.locator('#spotChartObMitigation').selectOption('Close');
      assert.equal((await page.evaluate(() => window.btcSpotChart.getOrderBlocks())).mitigation, 'Close');
      await page.locator('#spotChartObLength').fill('3'); await page.locator('#spotChartObLength').press('Tab');
      assert.equal((await page.evaluate(() => window.btcSpotChart.getOrderBlocks())).length, 3);
      await page.locator('#spotChartObLength').fill('5'); await page.locator('#spotChartObLength').press('Tab');
      await page.locator('#spotChartObMitigation').selectOption('Wick');
      now += HOUR; await page.evaluate(value => { window.__obNow = value; }, now);
      await page.locator('#spotChartRefresh').click();
      await page.waitForFunction(() => window.btcSpotChart.getOrderBlocks().blocks.every(b => b.state === 'broken'));
      const latest = await page.evaluate(() => window.btcSpotChart.getOrderBlocks());
      assert.equal(latest.blocks.at(-1).brokenAt, (start + 151 * HOUR) / 1000);
      await frames(page); assert.deepEqual(await page.evaluate(() => window.btcSpotChart.getOrderBlockLabels()), []);
      const overflow = await page.locator('#btcSpotChartPanel').evaluate(node => node.scrollWidth > node.clientWidth + 1);
      assert.equal(overflow, false, 'Chart panel must fit on mobile and desktop');
      assert.deepEqual(errors, []); assert.deepEqual(writes, []);
      results.push({ mobile, blocks: model.blocks.length, olderReads, activeDelta, brokenDelta, toggle: true,
        controls: true, clusterLabel: true, labelPixels, gapLeadAlignment: true, closedHourRefresh: true, overflow, writes });
      await context.close();
    }
  } finally { await browser.close(); }
  fs.writeFileSync(path.join(output, 'order-blocks-qa.json'), JSON.stringify(results, null, 2));
  console.log(JSON.stringify(results, null, 2));
})().catch(error => { console.error(error); process.exitCode = 1; });
