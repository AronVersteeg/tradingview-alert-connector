const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require('playwright');
const { PNG } = require('pngjs');
const repo = path.join(__dirname, '..');
const [url, candleFile, output] = process.argv.slice(2);
if (!url || !candleFile || !output) throw new Error('Supply preview URL, recovery candle cache and output directory');
const html = fs.readFileSync(path.join(repo, 'public/decentrader_liquidity_timelapse.html'), 'utf8');
const raw = JSON.parse(fs.readFileSync(path.join(repo, 'public/data/btc-gap-recovery-v1.json'), 'utf8'));
const history = { ...raw, venue: 'Binance Spot', historicalOnly: true,
  records: raw.records.map(([candleTime, lower, upper]) => ({ candleTime, availableAt: candleTime + 3600, lower, upper })) };
const candles = JSON.parse(fs.readFileSync(candleFile, 'utf8')).filter(c => c.timestampMs / 1000 >= history.records[0].candleTime)
  .map(c => ({ time: c.timestampMs / 1000, ...c, closed: true, volume: 0, quoteVolume: 0 }));
const frames = candles.map((c, i) => ({ i, startedAtMs: c.timestampMs,
  t: new Date(c.timestampMs).toISOString().slice(0, 19).replace('T', ' '), price: (c.open + c.high + c.low + c.close) / 4 }));
const target = Date.parse('2026-08-19T14:00:00Z') / 1000;
const green = buffer => {
  const png = PNG.sync.read(buffer); let count = 0;
  for (let i = 0; i < png.data.length; i += 4) if (png.data[i] < 80 && png.data[i + 1] > 210 && png.data[i + 2] < 80) count++;
  return count;
};
fs.mkdirSync(output, { recursive: true });
(async () => {
  const browser = await chromium.launch({ headless: true, channel: 'msedge' });
  const results = [];
  try {
    for (const mobile of [false, true]) {
      const context = await browser.newContext({ viewport: mobile ? { width: 390, height: 844 } : { width: 1440, height: 1000 }, isMobile: mobile, hasTouch: mobile });
      const errors = [], writes = [];
      await context.addInitScript(() => { window.WebSocket = class { close() {} }; });
      await context.route('**/*', route => {
        const req = route.request(), u = new URL(req.url());
        if (!['GET', 'HEAD', 'OPTIONS'].includes(req.method())) { writes.push(req.method()); return route.abort(); }
        if (u.pathname === '/decentrader/map') return route.fulfill({ contentType: 'text/html', body: html });
        if (u.origin === new URL(url).origin && u.pathname.startsWith('/assets/')) return route.continue();
        if (u.pathname.endsWith('/gap-projection-history')) return route.fulfill({ json: history });
        if (u.pathname.endsWith('/btc-spot-candles')) return route.fulfill({ json: { symbol: 'BTCUSDT', venue: 'Binance Spot', interval: '1h', fetchedAt: new Date().toISOString(), candles, hasMore: false } });
        if (u.pathname === '/decentrader/manual-override') return route.fulfill({ json: { enabled: true, market: 'BTC-USD', overrides: [] } });
        if (u.pathname === '/open-liquidity/v2/liquidity-timelapse') return route.fulfill({ json: {
          modelVersion: raw.modelVersion, source: { market: 'BTC-USD', sourceStatuses: [{ name: 'Binance Spot BTCUSDT 1H' }] },
          frames: frames.slice(-500).map((f, i) => ({ ...f, i })), events: [], range: { minPrice: 50000, maxPrice: 100000 },
          gaps: history.records.slice(-500).map(r => ({ left: r.lower, right: r.upper, width: r.upper - r.lower,
            leftEdge: { leverage: 10 }, rightEdge: { leverage: 10 } })) } });
        if (u.pathname === '/decentrader/liquidity-timelapse') return route.fulfill({ json: {
          source: { method: 'getOHLCHourlyCalculations', params: ['btcusdt'] }, frames, range: { minPrice: 50000, maxPrice: 100000 },
          events: [{ i: 0, p: 61000, s: 'L', l: 10, a: 1, n: 1 }, { i: 0, p: 87400, s: 'S', l: 10, a: 1, n: 1 }] } });
        return route.fulfill({ status: 503, json: { error: 'Unrelated source isolated in recovery test' } });
      });
      const page = await context.newPage(); page.on('pageerror', e => errors.push(e.message));
      await page.goto(url, { waitUntil: 'domcontentloaded' });
      await page.locator('#btcSpotChartPanel').scrollIntoViewIfNeeded();
      await page.waitForFunction(() => window.btcSpotChart?.getGapProjections()?.history?.records > 8000);
      await page.waitForFunction(() => window.btcSpotChart?.getCandles().length > 8000);
      const state = await page.evaluate(t => ({ row: window.BTC_SPOT_GAP_TIMELINE.records.find(r => r.candleTime === t), model: window.btcSpotChart.getGapProjections() }), target);
      assert.equal(state.row.upper, 67000); assert.equal(state.row.lower, 61000);
      assert.equal(state.model.history.mismatches, 0);
      assert.equal(state.model.boxes.length, 432);
      await page.evaluate(t => {
        window.btcSpotChart.chart.timeScale().setVisibleRange({ from: t - 36 * 3600, to: t + 24 * 3600 });
        window.btcSpotChart.candleSeries.priceScale().setVisibleRange({ from: 59000, to: 72000 });
      }, target);
      const pixels = green(await page.locator('#spotChartCanvas').screenshot()); assert(pixels > 500);
      await page.locator('#btcSpotChartPanel').screenshot({ path: path.join(output, `august19-${mobile ? 'mobile' : 'desktop'}.png`) });
      const legacy = await context.newPage(); legacy.on('pageerror', e => errors.push(e.message));
      const legacyUrl = new URL(url); legacyUrl.searchParams.set('source', 'decentrader');
      await legacy.goto(legacyUrl.href, { waitUntil: 'domcontentloaded' });
      await legacy.waitForFunction(length => Number(document.getElementById('frame').max) === length - 1, frames.length);
      await legacy.locator('#frame').evaluate((slider, index) => { slider.value = String(index); slider.dispatchEvent(new Event('input', { bubbles: true })); }, frames.findIndex(f => f.startedAtMs / 1000 === target));
      await legacy.waitForFunction(() => document.getElementById('gapStatus').textContent.includes('causal gap reconstruction'));
      assert((await legacy.locator('#gapRight').textContent()).includes('67,000'));
      assert((await legacy.locator('#gapLeft').textContent()).includes('61,000'));
      assert.deepEqual(writes, []); assert.deepEqual(errors, []);
      results.push({ mobile, recoveredHours: raw.records.length, boxes: state.model.boxes.length, august19: [61000, 67000], greenPixels: pixels, writes });
      await context.close();
    }
    console.log(JSON.stringify(results, null, 2));
  } finally { await browser.close(); }
})().catch(e => { console.error(e); process.exitCode = 1; });
