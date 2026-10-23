const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require('playwright');
const { PNG } = require('pngjs');

const url = process.argv[2] || 'http://127.0.0.1:4322/decentrader/map?source=open-v2';
const output = process.argv[3];
if (!output) throw new Error('Supply a screenshot directory');
fs.mkdirSync(output, { recursive: true });
const html = fs.readFileSync(path.join(__dirname, '../public/decentrader_liquidity_timelapse.html'), 'utf8')
  .replace('    wireViewer();', '    window.__gapFixtureRefresh = refreshPayload;\n    wireViewer();');
const hour = 3_600_000, initial = Math.floor(Date.now() / hour) * hour + 5000;
const start = initial - 5000 - 150 * hour;
const record = (i, lower, upper) => ({ candleTime: (start + i * hour) / 1000, availableAt: (start + (i + 1) * hour) / 1000, lower, upper });
const archive = { symbol: 'BTCUSDT', venue: 'Decentrader map', historicalOnly: true, historicalAccuracy: 'observed-snapshots',
  records: Array.from({ length: 140 }, (_, i) => record(i, i < 40 ? 74000 : i < 80 ? 76000 : 79400, i < 40 ? 90000 : i < 80 ? 88000 : 87400)) };

function mapPayload(total = 150) {
  const frames = Array.from({ length: total - 100 }, (_, i) => ({ i, startedAtMs: start + (100 + i) * hour,
    t: new Date(start + (100 + i) * hour).toISOString().slice(0, 19).replace('T', ' '), price: 81500, high: 82000, low: 80500 }));
  return { source: { market: 'BTC-USD', sourceStatuses: [{ name: 'Binance Spot BTCUSDT 1H' }] }, modelVersion: 'fixture',
    range: { minPrice: 73000, maxPrice: 91000 }, frames, events: [], contextEvents: [], topCurrentZones: [],
    gaps: frames.map((_, i) => ({ left: i < 40 ? 79400 : 80000, right: 87400, width: i < 40 ? 8000 : 7400,
      leftEdge: { leverage: 10, weightedUsd: 1e6 }, rightEdge: { leverage: 10, weightedUsd: 1e6 } })) };
}

function greenCount(buffer) {
  const png = PNG.sync.read(buffer);
  let green = 0;
  for (let i = 0; i < png.data.length; i += 4) if (png.data[i] < 80 && png.data[i + 1] > 210 && png.data[i + 2] < 80) green++;
  return green;
}

(async () => {
  const browser = await chromium.launch({ headless: true, channel: process.env.SPOT_CHART_BROWSER_CHANNEL || 'msedge' });
  const results = [];
  try {
    for (const mobile of [false, true]) {
      const context = await browser.newContext({ viewport: mobile ? { width: 390, height: 844 } : { width: 1440, height: 1000 },
        isMobile: mobile, hasTouch: mobile });
      const page = await context.newPage(), errors = [], writes = [];
      let now = initial, historyReads = 0, failHistory = false, untrustedHistory = true;
      page.on('pageerror', (error) => errors.push(error.message));
      await page.addInitScript((value) => {
        window.__gapFixtureNow = value;
        Date.now = () => window.__gapFixtureNow;
        window.WebSocket = class { close() {} };
      }, now);
      await context.route('**/*', async (route) => {
        const request = route.request(), target = new URL(request.url());
        if (!['GET', 'HEAD', 'OPTIONS'].includes(request.method())) { writes.push(request.method()); return route.abort(); }
        if (target.pathname === '/decentrader/map') return route.fulfill({ contentType: 'text/html', body: html });
        if (target.origin === new URL(url).origin && target.pathname.startsWith('/assets/')) return route.continue();
        if (target.pathname === '/decentrader/gap-projection-history') {
          historyReads++;
          if (untrustedHistory) return route.fulfill({ status: mobile ? 200 : 409, json: mobile
            ? { ...archive, historicalAccuracy: 'current-active-reconstruction' }
            : { code: 'HISTORICAL_STATE_UNVERIFIED' } });
          return route.fulfill({ status: failHistory ? 503 : 200, json: failHistory ? { error: 'Stored history unavailable' } : archive });
        }
        if (target.pathname === '/open-liquidity/v2/liquidity-timelapse') return route.fulfill({ json: mapPayload(now > initial + hour ? 151 : 150) });
        if (target.pathname === '/decentrader/liquidity-timelapse') return route.fulfill({ json: {
          ...mapPayload(), source: { method: 'getOHLCHourlyCalculations', params: ['btcusdt'] },
          events: [{ i: 0, s: 'L', l: 10, p: 79400, a: 1, n: 1 }, { i: 0, s: 'S', l: 10, p: 87400, a: 1, n: 1 }]
        } });
        if (target.pathname === '/decentrader/manual-override') return route.fulfill({ json: { enabled: true, market: 'BTC-USD', overrides: [] } });
        if (target.pathname.endsWith('/btc-spot-candles')) return route.fulfill({ json: { symbol: 'BTCUSDT', venue: 'Binance Spot', interval: '1h',
          fetchedAt: new Date(now).toISOString(), hasMore: false, candles: Array.from({ length: 151 }, (_, i) => ({
            time: (start + i * hour) / 1000, open: 80800, high: i % 3 ? 82000 : 83000, low: 79000,
            close: i % 2 ? 81500 : 80100, volume: 1, quoteVolume: 81000, closed: i < 150
          })) } });
        return route.fulfill({ status: 503, json: { error: 'Unrelated fixture source disabled' } });
      });
      await page.goto(url, { waitUntil: 'domcontentloaded' });
      await page.locator('#btcSpotChartPanel').scrollIntoViewIfNeeded();
      await page.waitForFunction(() => window.btcSpotChart?.getGapProjections() &&
        document.getElementById('spotChartGapStatus').textContent.includes('past state unverified'));
      assert.equal(await page.evaluate(() => window.btcSpotChart.getGapProjections().history), undefined,
        'Untrusted current-active data must never extend the causal Public V2 timeline');
      untrustedHistory = false; now += hour + 1;
      await page.evaluate((time) => { window.__gapFixtureNow = time; }, now);
      await page.evaluate(() => window.__gapFixtureRefresh());
      await page.waitForFunction(() => window.btcSpotChart?.getGapProjections()?.history?.records === 100);
      const model = await page.evaluate(() => window.btcSpotChart.getGapProjections());
      assert.equal(model.boxes.length, 4);
      assert.equal(model.history.mismatches, 0);
      assert.equal(model.history.overlapCompared, 40);
      assert.equal(model.boxes[2].fromTime, archive.records[80].availableAt);
      assert.equal(model.boxes[2].startSource, 'decentrader-map');
      assert.equal(model.boxes.at(-1).lower, 80000);
      await page.locator('[data-spot-range="all"]').click();
      await page.evaluate(() => window.btcSpotChart.candleSeries.priceScale().setVisibleRange({ from: 73000, to: 91000 }));
      await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));
      const canvas = await page.locator('#spotChartCanvas').screenshot();
      const green = greenCount(canvas);
      assert(green > 500, `Historical outlines must contain real green pixels: ${green}`);
      // Sample the exact first historical step's vertical edge, not just active labels.
      const position = await page.evaluate((time) => ({
        x: window.btcSpotChart.chart.timeScale().timeToCoordinate(time),
        a: window.btcSpotChart.candleSeries.priceToCoordinate(88000),
        b: window.btcSpotChart.candleSeries.priceToCoordinate(76000)
      }), archive.records[40].availableAt);
      const png = PNG.sync.read(canvas);
      let verticalGreen = 0;
      for (let y = Math.ceil(position.a) + 2; y < Math.floor(position.b) - 2; y++) {
        for (let x = Math.max(0, Math.floor(position.x) - 2); x <= Math.ceil(position.x) + 2; x++) {
          const at = (y * png.width + x) * 4;
          if (png.data[at] < 80 && png.data[at + 1] > 210 && png.data[at + 2] < 80) verticalGreen++;
        }
      }
      assert(verticalGreen > 100, `The historical transition cap must render at its source close: ${verticalGreen}`);
      await page.locator('#btcSpotChartPanel').screenshot({ path: path.join(output, `gap-history-${mobile ? 'mobile' : 'desktop'}.png`) });
      await page.locator('#spotChartGap').uncheck();
      assert(greenCount(await page.locator('#spotChartCanvas').screenshot()) < green / 2);
      await page.locator('#spotChartGap').check();
      await page.evaluate(() => window.__gapFixtureRefresh());
      assert.equal(historyReads, 2, 'Map refresh must not refetch the archive within an hour');
      failHistory = true; now += hour + 1000;
      await page.evaluate((time) => { window.__gapFixtureNow = time; }, now);
      await page.evaluate(() => window.__gapFixtureRefresh());
      await page.waitForFunction(() => document.getElementById('spotChartGapStatus').textContent.includes('History read failed'));
      assert.equal(historyReads, 3);
      assert.equal(await page.evaluate(() => window.btcSpotChart.getGapProjections().boxes.length), 4, 'Failed archive read must retain known rectangles');
      failHistory = false; now += 5 * 60_000 + 1;
      await page.evaluate((time) => { window.__gapFixtureNow = time; }, now);
      await page.evaluate(() => window.__gapFixtureRefresh());
      await page.waitForFunction(() => !document.getElementById('spotChartGapStatus').textContent.includes('History read failed'));
      assert.equal(historyReads, 4);
      const legacy = await context.newPage();
      legacy.on('pageerror', (error) => errors.push(error.message));
      const legacyUrl = new URL(url); legacyUrl.searchParams.set('source', 'decentrader');
      await legacy.goto(legacyUrl.href, { waitUntil: 'domcontentloaded' });
      await legacy.waitForFunction(() => document.getElementById('frame').max === '49');
      await legacy.locator('#frame').evaluate((slider) => { slider.value = '0'; slider.dispatchEvent(new Event('input', { bubbles: true })); });
      assert.equal(await legacy.locator('#gapRight').textContent(), '-');
      assert((await legacy.locator('#gapStatus').textContent()).includes('Historical gap unverified'));
      assert((await legacy.locator('#gapAlert').textContent()).includes('Unknown'));
      await legacy.locator('#frame').evaluate((slider) => { slider.value = slider.max; slider.dispatchEvent(new Event('input', { bubbles: true })); });
      assert((await legacy.locator('#gapRight').textContent()).includes('87'));
      await legacy.close();
      assert.deepEqual(errors, []); assert.deepEqual(writes, []);
      results.push({ mobile, historyBoxes: model.boxes.length, olderHours: model.history.records, green, verticalGreen,
        untrustedHistoryQuarantined: true, hourlyCache: true, sourceFailurePreservesBoxes: true, recovery: true, writeRequests: writes });
      await context.close();
    }
    console.log(JSON.stringify(results, null, 2));
  } finally { await browser.close(); }
})().catch((error) => { console.error(error); process.exitCode = 1; });
