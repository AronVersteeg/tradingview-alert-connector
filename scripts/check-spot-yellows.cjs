const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require('playwright');
const { PNG } = require('pngjs');
const [base, output] = process.argv.slice(2);
if (!base || !output) throw new Error('Supply safe local preview origin and screenshot directory');
fs.mkdirSync(output, { recursive: true });
const html = fs.readFileSync(path.join(__dirname, '../public/decentrader_liquidity_timelapse.html'), 'utf8')
  .replace('    wireViewer();', '    window.__yellowRefresh = () => refreshPayload();\n    wireViewer();');
const HOUR = 3_600_000, day = Date.parse('2026-10-19T00:00:00Z');
const iso = ms => new Date(ms).toISOString();
function candleRows(now) {
  return Array.from({ length: 24 + Math.floor((now - day) / HOUR) + 1 }, (_, i) => {
    const time = (day - 24 * HOUR + i * HOUR) / 1000;
    return { time, open: 100, high: 101, low: 99, close: 100, volume: 1, quoteVolume: 100, closed: (time + 3600) * 1000 <= now };
  });
}
function payload(asset, now) {
  const market = `${asset}-USD`, symbol = `${asset}USDT`, rows = candleRows(now).filter(c => c.closed);
  return { source: { market, sourceStatuses: [{ name: `Binance Spot ${symbol} 1H`, ok: true }] }, range: { minPrice: 85, maxPrice: 115 },
    frames: rows.map((c, i) => ({ ...c, i, startedAtMs: c.time * 1000, t: iso(c.time * 1000).slice(0, 19).replace('T', ' '), price: 100 })),
    gaps: rows.map(() => ({ left: 90, right: 110, width: 20,
      leftEdge: { leverage: 10, relativeCount: 1 }, rightEdge: { leverage: 10, relativeCount: 1 } })), events: [],
    intrusionBenchmarks: { history: [3, 4, 5, 7, 8].map((h, i) => ({ timestamp: iso(day + h * HOUR),
      direction: i === 3 ? 'mixed' : i % 2 ? 'short' : 'long', smtpSentAt: iso(day + 15 * HOUR) })) },
    delayHistory: { records: [{ intrusionTimestamp: iso(day + 4 * HOUR), direction: 'short' }] } };
}
function yellowPixels(buffer, x) {
  const png = PNG.sync.read(buffer); let count = 0;
  for (let y = 5; y < png.height - 35; y++) for (let px = Math.max(0, Math.floor(x) - 1); px <= Math.min(png.width - 1, Math.ceil(x) + 1); px++) {
    const at = (y * png.width + px) * 4;
    if (png.data[at] > 190 && png.data[at + 1] > 180 && png.data[at + 2] < 100) count++;
  }
  return count;
}
(async () => {
  const browser = await chromium.launch({ headless: true, channel: 'msedge' });
  const results = [];
  try {
    for (const asset of ['BTC', 'ETH', 'SOL', 'ZEC']) for (const mobile of [false, true]) {
      let now = day + 8 * HOUR + 5000;
      const market = `${asset}-USD`, symbol = `${asset}USDT`, slug = asset.toLowerCase(), errors = [], writes = [], foreign = [];
      const context = await browser.newContext({ viewport: mobile ? { width: 390, height: 844 } : { width: 1440, height: 1000 },
        isMobile: mobile, hasTouch: mobile, acceptDownloads: true });
      await context.addInitScript(value => { window.__yellowNow = value; Date.now = () => window.__yellowNow; window.WebSocket = class { close() {} }; }, now);
      await context.route('**/*', route => {
        const req = route.request(), url = new URL(req.url());
        if (!['GET', 'HEAD', 'OPTIONS'].includes(req.method())) { writes.push(req.method()); return route.abort(); }
        if (url.pathname === '/decentrader/map') return route.fulfill({ contentType: 'text/html', body: html });
        if (url.origin === new URL(base).origin && url.pathname.startsWith('/assets/')) return route.continue();
        if (/\/(btc|eth|sol|zec)-(spot-|intrusion-)/.test(url.pathname) && !url.pathname.startsWith(`/open-liquidity/v2/${slug}-`)) foreign.push(req.url());
        if (url.pathname.endsWith(`/${slug}-spot-candles`)) return route.fulfill({ json: { symbol, venue: 'Binance Spot', interval: '1h', fetchedAt: iso(now), candles: candleRows(now), hasMore: false } });
        if (url.pathname === '/open-liquidity/v2/liquidity-timelapse') {
          if (url.searchParams.get('market') !== market) foreign.push(req.url());
          return route.fulfill({ json: payload(asset, now) });
        }
        if (url.pathname === '/decentrader/manual-override') return route.fulfill({ json: { market, enabled: false, overrides: [] } });
        return route.fulfill({ status: 503, json: { error: 'Other sources deliberately unavailable in Yellows-only fixture' } });
      });
      const page = await context.newPage();
      page.on('pageerror', e => errors.push(e.message));
      await page.goto(`${base}/decentrader/map?source=${asset === 'BTC' ? 'open-v2' : `open-v2-${slug}`}`, { waitUntil: 'domcontentloaded' });
      await page.locator('#btcSpotChartPanel').scrollIntoViewIfNeeded();
      await page.waitForFunction(() => window.btcSpotChart?.getCandles().length > 0);
      assert.equal(await page.locator('#spotChartYellows').isChecked(), false);
      assert.deepEqual(await page.evaluate(() => window.btcSpotChart.getYellowIntrusionMarkers()), []);
      for (const id of ['spotChartHourlyBoxes', 'spotChartOrderBlocks', 'spotChartGap', 'spotChartDaily', 'spotChartWeekly', 'spotChartMonthly',
        'spotChartGapIntrusions', 'spotChartGapWeeklyBreaks', 'spotChartGapDailyBreaks', 'spotChartIntrusionDailyBreaks', 'spotChartManual']) await page.locator('#' + id).uncheck();
      await page.locator('#spotChartYellows').check();
      await page.waitForFunction(() => window.btcSpotChart.getYellowIntrusionMarkers().length === 4);
      const expected = [3, 4, 5, 7].map(h => (day + h * HOUR) / 1000);
      assert.deepEqual(await page.evaluate(() => window.btcSpotChart.getYellowIntrusionMarkers().map(m => m.time)), expected);
      await page.evaluate(d => {
        window.btcSpotChart.chart.timeScale().setVisibleRange({ from: d / 1000, to: d / 1000 + 10 * 3600 });
        window.btcSpotChart.candleSeries.priceScale().setVisibleRange({ from: 95, to: 105 });
      }, day);
      await page.mouse.move(0, 0);
      await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
      const xs = await page.evaluate(times => times.map(t => window.btcSpotChart.chart.timeScale().timeToCoordinate(t)), expected);
      const shot = await page.locator('#spotChartCanvas').screenshot();
      assert(xs.every(x => x !== null && yellowPixels(shot, x) > 150), 'Four actual yellow lines, including mixed and no-break candles');
      await page.locator('#btcSpotChartPanel').screenshot({ path: path.join(output, `yellows-${slug}-${mobile ? 'mobile' : 'desktop'}.png`) });
      const downloadEvent = page.waitForEvent('download'); await page.locator('#spotChartDownload').click();
      const file = path.join(output, `yellows-${slug}-${mobile ? 'mobile' : 'desktop'}-export.png`);
      await (await downloadEvent).saveAs(file);
      assert(xs.every(x => yellowPixels(fs.readFileSync(file), x) > 150), 'Yellow lines survive PNG export');
      now += HOUR;
      await page.evaluate(time => { window.__yellowNow = time; }, now);
      await page.evaluate(() => window.__yellowRefresh());
      await page.waitForFunction(() => window.btcSpotChart.getYellowIntrusionMarkers().length === 5);
      await page.evaluate(() => window.dispatchEvent(new CustomEvent('btc-spot-chart:intrusion-error')));
      assert.equal((await page.evaluate(() => window.btcSpotChart.getYellowIntrusionMarkers())).length, 5);
      assert((await page.locator('#spotChartYellowsStatus').textContent()).includes('stale'));
      await page.locator('#spotChartYellows').uncheck();
      await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
      assert.deepEqual(await page.evaluate(() => window.btcSpotChart.getYellowIntrusionMarkers()), []);
      const off = await page.locator('#spotChartCanvas').screenshot();
      assert(xs.every(x => yellowPixels(off, x) < 15));
      const geometry = await page.locator('#btcSpotChartPanel').evaluate(n => ({ client: n.clientWidth, scroll: n.scrollWidth }));
      assert(geometry.scroll <= geometry.client + 1, 'No toolbar overflow');
      assert.deepEqual(errors, []); assert.deepEqual(writes, []); assert.deepEqual(foreign, []);
      results.push({ asset, mobile, defaultOff: true, noBreakRequired: true, mixedIncluded: true, closeUpdate: true, png: true, writes });
      await context.close();
    }
    console.log(JSON.stringify(results, null, 2));
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
