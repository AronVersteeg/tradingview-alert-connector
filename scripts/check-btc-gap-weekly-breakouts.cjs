const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require('playwright');
const { PNG } = require('pngjs');
const [url, output] = process.argv.slice(2);
if (!url || !output) throw new Error('Supply preview URL and screenshot directory');
fs.mkdirSync(output, { recursive: true });
const html = fs.readFileSync(path.join(__dirname, '../public/decentrader_liquidity_timelapse.html'), 'utf8')
  .replace('    wireViewer();', '    window.__weeklyFixtureRefresh = refreshPayload;\n    wireViewer();');
const HOUR = 3_600_000, start = Date.parse('2026-09-28T00:00:00Z'), day = Date.parse('2026-10-19T00:00:00Z');
const iso = time => new Date(time).toISOString();
const special = { 3: [100, 128, 98, 125], 4: [125, 133, 123, 130], 5: [130, 132, 72, 75],
  6: [75, 77, 68, 70], 7: [70, 102, 68, 100], 8: [100, 155, 99, 150] };
function candles(now) {
  return Array.from({ length: Math.floor((now - start) / HOUR) + 1 }, (_, i) => {
    const time = start + i * HOUR, values = special[(time - day) / HOUR] || [100, 110, 90, 100];
    const [open, high, low, close] = values;
    return { time: time / 1000, open, high: time === Date.parse('2026-09-29T07:00:00Z') ? 120 : high,
      low, close, volume: 1, quoteVolume: 100, closed: time + HOUR <= now };
  });
}
function payload(now) {
  const rows = candles(now).filter(c => c.closed);
  return { modelVersion: 'binance-spot-liquidation-cohorts-v2.4', source: { market: 'BTC-USD', sourceStatuses: [{ name: 'Binance Spot BTCUSDT 1H' }] },
    range: { minPrice: 60, maxPrice: 160 }, events: [],
    frames: rows.map((c, i) => ({ i, startedAtMs: c.time * 1000, t: iso(c.time * 1000).slice(0, 19).replace('T', ' '), price: (c.open + c.high + c.low + c.close) / 4 })),
    gaps: rows.map(c => { const left = c.time * 1000 >= day + 5 * HOUR ? 60 : 85, right = c.time * 1000 >= day + 3 * HOUR ? 140 : 115;
      return { left, right, width: right - left, leftEdge: { leverage: 10, relativeCount: 1 }, rightEdge: { leverage: 10, relativeCount: 1 } }; }) };
}
function bundle(now) {
  const through = Math.floor(now / HOUR) * HOUR;
  const snapshot = (interval, close, records = []) => ({ venue: 'Binance Spot', symbol: 'BTCUSDT', interval, latestClosedCandleAt: close, records });
  const record = (id, type, price, pivot, confirmed, broken) => ({ id, type, priceExact: String(price), pivotAt: pivot,
    confirmedAt: confirmed, isLatestForType: true, ...(broken ? { firstBrokenAt: iso(day) } : {}) });
  return { symbol: 'BTCUSDT', venue: 'Binance Spot', evaluatedThrough: iso(through),
    daily: snapshot('1d', '2026-10-18T23:59:59.999Z'), monthly: snapshot('1M', '2026-09-30T23:59:59.999Z'),
    weekly: snapshot('1w', '2026-10-18T23:59:59.999Z', [
      record('high', 'HIGH', 120, '2026-09-28T00:00:00Z', '2026-10-18T23:59:59.999Z', through >= day + 4 * HOUR),
      record('low', 'LOW', 80, '2026-09-21T00:00:00Z', '2026-10-11T23:59:59.999Z', through >= day + 6 * HOUR)
    ]) };
}
function orangeColumn(buffer, x) {
  const png = PNG.sync.read(buffer); let count = 0;
  for (let y = 5; y < png.height - 35; y++) for (let atX = Math.max(0, Math.floor(x) - 2); atX <= Math.min(png.width - 1, Math.ceil(x) + 2); atX++) {
    const at = (y * png.width + atX) * 4;
    if (png.data[at] > 190 && png.data[at + 1] > 100 && png.data[at + 1] < 180 && png.data[at + 2] < 65) count++;
  }
  return count;
}
(async () => {
  const browser = await chromium.launch({ headless: true, channel: process.env.SPOT_CHART_BROWSER_CHANNEL || 'msedge' });
  const results = [];
  try {
    for (const mobile of [false, true]) {
      const context = await browser.newContext({ viewport: mobile ? { width: 390, height: 844 } : { width: 1440, height: 1000 },
        isMobile: mobile, hasTouch: mobile, acceptDownloads: true });
      let now = day + 4 * HOUR + 5000;
      const errors = [], writes = [];
      await context.addInitScript(value => { window.__weeklyNow = value; Date.now = () => window.__weeklyNow; window.WebSocket = class { close() {} }; }, now);
      await context.route('**/*', route => {
        const req = route.request(), target = new URL(req.url());
        if (!['GET', 'HEAD', 'OPTIONS'].includes(req.method())) { writes.push(req.method()); return route.abort(); }
        if (target.pathname === '/decentrader/map') return route.fulfill({ contentType: 'text/html', body: html });
        if (target.origin === new URL(url).origin && target.pathname.startsWith('/assets/')) return route.continue();
        if (target.pathname.endsWith('/btc-spot-candles')) return route.fulfill({ json: { symbol: 'BTCUSDT', venue: 'Binance Spot', interval: '1h',
          fetchedAt: iso(now), candles: candles(now), hasMore: false } });
        if (target.pathname.endsWith('/btc-spot-fractal-projections')) return route.fulfill({ json: bundle(now) });
        if (target.pathname === '/open-liquidity/v2/liquidity-timelapse') return route.fulfill({ json: payload(now) });
        if (target.pathname === '/decentrader/manual-override') return route.fulfill({ json: { enabled: true, market: 'BTC-USD', overrides: [] } });
        return route.fulfill({ status: 503, json: { error: 'Unrelated source isolated in read-only fixture' } });
      });
      const page = await context.newPage(); page.on('pageerror', e => errors.push(e.message));
      await page.goto(url, { waitUntil: 'domcontentloaded' });
      await page.locator('#btcSpotChartPanel').scrollIntoViewIfNeeded();
      await page.waitForFunction(() => window.btcSpotChart?.getGapWeeklyBreakouts()?.signals.length === 1);
      assert.equal(await page.locator('#spotChartGapWeeklyBreaks').isChecked(), true);
      let state = await page.evaluate(() => window.btcSpotChart.getGapWeeklyBreakouts());
      assert.equal(state.signals[0].time, (day + 3 * HOUR) / 1000);
      assert.equal(state.signals[0].gap.upper, 115, 'The expanded 140 edge must not be used retroactively');
      now = day + 8 * HOUR + 5000;
      await page.evaluate(time => { window.__weeklyNow = time; }, now);
      await page.evaluate(() => window.__weeklyFixtureRefresh());
      await page.waitForFunction(() => window.btcSpotChart?.getGapWeeklyBreakouts()?.signals.length === 2, null, { timeout: 20_000 });
      state = await page.evaluate(() => window.btcSpotChart.getGapWeeklyBreakouts());
      assert.deepEqual(state.signals.map(s => s.direction), ['long', 'short']);
      assert.equal(state.signals[1].time, (day + 5 * HOUR) / 1000);
      assert.equal(state.signals[1].gap.lower, 85, 'The short must use the old 85 edge, not the new 60 edge');
      for (const id of ['spotChartDaily', 'spotChartWeekly', 'spotChartMonthly', 'spotChartHourlyBoxes', 'spotChartGap']) await page.locator('#' + id).uncheck();
      await page.evaluate(time => {
        window.btcSpotChart.chart.timeScale().setVisibleRange({ from: time / 1000 - 6 * 3600, to: time / 1000 + 10 * 3600 });
        window.btcSpotChart.candleSeries.priceScale().setVisibleRange({ from: 60, to: 160 });
      }, day);
      await page.mouse.move(0, 0);
      await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
      const xs = await page.evaluate(times => times.map(time => window.btcSpotChart.chart.timeScale().timeToCoordinate(time)), state.signals.map(s => s.time));
      const on = await page.locator('#spotChartCanvas').screenshot();
      const pixels = xs.map(x => orangeColumn(on, x));
      assert(pixels.every(n => n > 200), `Both vertical orange lines must render across the pane: ${pixels}`);
      await page.locator('#spotChartGapWeeklyBreaks').uncheck();
      await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
      const off = await page.locator('#spotChartCanvas').screenshot();
      assert(xs.every(x => orangeColumn(off, x) < 20), 'Unchecking must remove the actual orange pixels');
      await page.locator('#spotChartGapWeeklyBreaks').check();
      await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
      const downloadEvent = page.waitForEvent('download'); await page.locator('#spotChartDownload').click();
      const download = await downloadEvent; const file = path.join(output, `gap-weekly-${mobile ? 'mobile' : 'desktop'}-export.png`);
      await download.saveAs(file);
      assert(xs.every(x => orangeColumn(fs.readFileSync(file), x) > 200), 'PNG export includes the marker layer');
      await page.locator('#btcSpotChartPanel').screenshot({ path: path.join(output, `gap-weekly-${mobile ? 'mobile' : 'desktop'}.png`) });
      const geometry = await page.locator('#btcSpotChartPanel').evaluate(node => ({ client: node.clientWidth, scroll: node.scrollWidth }));
      assert(geometry.scroll <= geometry.client + 1, 'The toolbar must not overflow on mobile');
      now += HOUR;
      await page.evaluate(time => { window.__weeklyNow = time; }, now);
      await page.evaluate(() => window.__weeklyFixtureRefresh());
      await page.locator('#spotChartRefresh').click();
      await page.waitForFunction(time => window.btcSpotChart.getGapWeeklyBreakouts()?.evaluatedThrough === new Date(time - 5000).toISOString(), now);
      assert.equal((await page.evaluate(() => window.btcSpotChart.getGapWeeklyBreakouts())).signals.length, 2,
        'A later close beyond already-broken levels must not add another line');
      assert.deepEqual(errors, []); assert.deepEqual(writes, []);
      results.push({ mobile, automaticClosedHourUpdate: true, longAndShort: true, previousBoxUsed: true,
        independentCheckbox: true, orangePixels: pixels, pngIncludesOverlay: true, noHorizontalOverflow: true, writes });
      await context.close();
    }
    console.log(JSON.stringify(results, null, 2));
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
