const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require('playwright');
const { PNG } = require('pngjs');

const url = process.argv[2] || 'http://127.0.0.1:4320/decentrader/map?source=open-v2';
const output = process.argv[3];
if (!output) throw new Error('Supply a screenshot directory');
fs.mkdirSync(output, { recursive: true });
// Test-only hook into the real read/render callback; never served by production.
const html = fs.readFileSync(path.join(__dirname, '../public/decentrader_liquidity_timelapse.html'), 'utf8')
  .replace('    wireViewer();', '    window.__manualFixtureRefresh = refreshManualOverride;\n    wireViewer();');
const hour = 3_600_000, now = Math.floor(Date.now() / hour) * hour + 5000;
const start = now - 120 * hour - 5000;
const makePlan = (changes = {}) => ({ market: 'BTC-USD', id: 'fixture-long', direction: 'long', status: 'ARMED',
  closeTrigger: 80000, armedAt: new Date(start + 55 * hour).toISOString(),
  takeProfits: [{ price: 82000, allocationPct: 50 }, { price: 84000, allocationPct: 30 }, { price: 86000, allocationPct: 20 }], ...changes });

function bluePixels(buffer) {
  const png = PNG.sync.read(buffer);
  let lines = 0, fill = 0;
  for (let i = 0; i < png.data.length; i += 4) {
    const [r, g, b] = png.data.subarray(i, i + 3);
    if (b > 220 && r < 100 && g > 110 && g < 185) lines++;
    if (b > 65 && b < 180 && b > g + 10 && b > r * 1.7 && g > r * 1.2) fill++;
  }
  return { lines, fill };
}

async function checkRightBuffer(page, context, mobile) {
  const measure = () => page.evaluate(() => {
    const api = window.btcSpotChart, scale = api.chart.timeScale(), range = scale.getVisibleLogicalRange();
    const edge = api.getCandles().length - 1 + Number(document.getElementById('spotChartGapLead').value);
    return { margin: range.to - edge, edgeX: scale.logicalToCoordinate(edge), width: scale.width(), span: range.to - range.from };
  });
  const verify = async () => {
    await page.waitForFunction(() => {
      const api = window.btcSpotChart, range = api.chart.timeScale().getVisibleLogicalRange();
      return range.to - (api.getCandles().length - 1 + Number(document.getElementById('spotChartGapLead').value)) >= 49.99;
    });
    const value = await measure();
    assert(value.edgeX > 0 && value.edgeX < value.width, JSON.stringify(value));
    return value;
  };
  await verify();
  for (let i = 0; i < 10; i++) { await page.locator('#spotChartZoomIn').click(); await verify(); }
  await page.locator('#spotChartReset').click(); await verify();
  await page.locator('[data-spot-range="24"]').click(); await verify();
  await page.locator('#spotChartGapLead').fill('20'); await page.locator('#spotChartGapLead').press('Tab'); await verify();
  await page.locator('#spotChartGapLead').fill('10'); await page.locator('#spotChartGapLead').press('Tab'); await verify();
  await page.locator('[data-spot-range="120"]').click();
  const box = await page.locator('#spotChartCanvas').boundingBox();
  await page.mouse.move(box.x + box.width * .3, box.y + 100);
  const before = await measure();
  await page.mouse.wheel(0, -500);
  await page.waitForTimeout(200);
  const wheel = await verify();
  assert(wheel.span < before.span, 'Real mouse-wheel zoom must zoom in while preserving the buffer');
  if (mobile) {
    const beforePinch = await measure();
    const session = await context.newCDPSession(page);
    await session.send('Input.synthesizePinchGesture', { x: Math.round(box.x + box.width * .3), y: Math.round(box.y + 100),
      scaleFactor: 1.4, relativeSpeed: 800, gestureSourceType: 'touch' });
    const pinched = await verify(); await session.detach();
    assert(pinched.span < beforePinch.span, 'Touch pinch must really zoom in');
  } else {
    const beforeAxis = await measure();
    await page.mouse.move(box.x + box.width * .45, box.y + box.height - 12);
    await page.mouse.down();
    await page.mouse.move(box.x + box.width * .25, box.y + box.height - 12, { steps: 12 });
    await page.mouse.up(); const axis = await verify();
    assert(Math.abs(axis.span - beforeAxis.span) > .1, 'Time-axis drag must really change zoom');
  }
  // Historical navigation must remain available, not pinned to today's edge.
  await page.waitForTimeout(550);
  await page.evaluate(() => window.btcSpotChart.chart.timeScale().setVisibleLogicalRange({ from: 5, to: 45 }));
  assert((await page.evaluate(() => window.btcSpotChart.chart.timeScale().getVisibleLogicalRange())).to < 100);
  await page.locator('[data-spot-range="120"]').click();
  const size = page.viewportSize();
  await page.setViewportSize({ ...size, width: size.width + 80 }); await verify();
  await page.setViewportSize(size); await verify();
  return { minimum: 50, buttons: true, wheel: true, pinch: mobile, timeAxisDrag: !mobile, historyPreserved: true, resize: true, final: await verify() };
}

(async () => {
  const browser = await chromium.launch({ headless: true, channel: process.env.SPOT_CHART_BROWSER_CHANNEL || 'msedge' });
  const results = [];
  try {
    for (const mobile of [false, true]) {
      const context = await browser.newContext({ viewport: mobile ? { width: 390, height: 844 } : { width: 1440, height: 1000 },
        isMobile: mobile, hasTouch: mobile, acceptDownloads: true });
      const page = await context.newPage(), errors = [], writes = [];
      let plans = [makePlan()], readFails = false;
      page.on('pageerror', (error) => errors.push(error.message));
      await page.addInitScript(() => { window.WebSocket = class { close() {} }; });
      await page.route('**/*', async (route) => {
        const req = route.request(), target = new URL(req.url());
        if (!['GET', 'HEAD', 'OPTIONS'].includes(req.method())) { writes.push(req.method()); return route.abort(); }
        if (target.pathname === '/decentrader/map') return route.fulfill({ contentType: 'text/html', body: html });
        if (target.origin === new URL(url).origin && target.pathname.startsWith('/assets/')) return route.continue();
        if (target.pathname === '/decentrader/manual-override') return route.fulfill({ status: readFails ? 503 : 200,
          json: readFails ? { error: 'Fixture unavailable' } : { market: 'BTC-USD', enabled: true, overrides: plans } });
        if (target.pathname.endsWith('/btc-spot-candles')) return route.fulfill({ json: { symbol: 'BTCUSDT', venue: 'Binance Spot', interval: '1h',
          fetchedAt: new Date(now).toISOString(), hasMore: false, candles: Array.from({ length: 121 }, (_, index) => ({
            time: (start + index * hour) / 1000, open: 80800, high: 81500, low: 79000, close: index % 2 ? 81100 : 80100,
            volume: 10, quoteVolume: 800000, closed: index < 120
          })) } });
        return route.fulfill({ status: 503, json: { error: 'Isolated fixture: source disabled' } });
      });
      await page.goto(url, { waitUntil: 'domcontentloaded' });
      // Publish first before scrolling into the lazy chart: tests the event/cache race.
      await page.evaluate(() => window.__manualFixtureRefresh());
      await page.locator('#btcSpotChartPanel').scrollIntoViewIfNeeded();
      await page.waitForFunction(() => window.btcSpotChart?.getCandles().length === 121 && window.btcSpotChart.getManualLevels()?.levels.length === 4);
      await page.locator('[data-spot-range="120"]').click();
      await page.locator('#spotChartFitFractals').click();
      const rightBuffer = await checkRightBuffer(page, context, mobile);
      const settle = () => page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));
      await settle();
      await page.mouse.move(0, 0);
      const on = await page.locator('#spotChartCanvas').screenshot();
      fs.writeFileSync(path.join(output, `manual-canvas-${mobile ? 'mobile' : 'desktop'}-fixture.png`), on);
      const pixels = bluePixels(on);
      assert(pixels.lines > 500 && pixels.fill > 1000, JSON.stringify(pixels));
      const axis = await page.evaluate(() => ({ width: window.btcSpotChart.chart.priceScale('right').width(),
        ys: window.btcSpotChart.getManualLevels().levels.map((level) => window.btcSpotChart.candleSeries.priceToCoordinate(level.price)) }));
      const png = PNG.sync.read(on);
      for (const y of axis.ys) {
        let blue = 0;
        for (let row = Math.max(0, Math.floor(y) - 12); row < Math.min(png.height, Math.ceil(y) + 13); row++) {
          for (let x = png.width - axis.width + 3; x < png.width - 2; x++) {
            const i = (row * png.width + Math.floor(x)) * 4;
            if (png.data[i + 2] > 220 && png.data[i] < 100) blue++;
          }
        }
        assert(blue > 100, 'Every entry/TP needs a native blue price-axis label');
      }
      // Sample one column: each subsequent TP area must be lighter (less blue fill).
      const shades = await page.evaluate(() => ({ x: window.btcSpotChart.chart.timeScale().logicalToCoordinate(100) + 3,
        ys: [81700, 83300, 85300].map((price) => window.btcSpotChart.candleSeries.priceToCoordinate(price)) }));
      const blues = shades.ys.map((y) => Math.max(...Array.from({ length: 7 }, (_, offset) =>
        png.data[(Math.floor(y) * png.width + Math.floor(shades.x) + offset) * 4 + 2])));
      assert(blues[0] + 5 < blues[1] && blues[1] + 5 < blues[2], `Consecutive TP zones must become lighter: ${JSON.stringify({ blues, shades, width: png.width, height: png.height })}`);
      assert.equal(await page.locator('#btcSpotChartPanel').evaluate((node) => node.scrollWidth > node.clientWidth + 1), false);
      await page.locator('#btcSpotChartPanel').screenshot({ path: path.join(output, `manual-bands-${mobile ? 'mobile' : 'desktop'}-fixture.png`) });
      const downloadPromise = page.waitForEvent('download');
      await page.locator('#spotChartDownload').click();
      const download = await downloadPromise;
      const pngPath = path.join(output, `manual-export-${mobile ? 'mobile' : 'desktop'}-fixture.png`);
      await download.saveAs(pngPath);
      assert(bluePixels(fs.readFileSync(pngPath)).fill > 1000, 'PNG export must include filled TP bands');
      await page.locator('#spotChartManual').uncheck(); await settle();
      const off = bluePixels(await page.locator('#spotChartCanvas').screenshot());
      assert(off.lines < pixels.lines / 3 && off.fill < pixels.fill / 3, `Toggle hides lines, labels and bands: ${JSON.stringify({ on: pixels, off })}`);
      await page.locator('#spotChartManual').check();
      plans = [makePlan({ closeTrigger: 80500, takeProfits: [{ price: 82500, allocationPct: 100 }] })];
      await page.evaluate(() => window.__manualFixtureRefresh());
      assert.deepEqual(await page.evaluate(() => window.btcSpotChart.getManualLevels().levels.map((level) => level.price)), [80500, 82500]);
      plans.push(makePlan({ id: 'fixture-short', direction: 'short', closeTrigger: 79500, takeProfits: [{ price: 77000 }, { price: 75000 }] }));
      await page.evaluate(() => window.__manualFixtureRefresh());
      assert.equal(await page.evaluate(() => window.btcSpotChart.getManualLevels().planCount), 2);
      plans[0].status = 'TRIGGERED';
      await page.evaluate(() => window.__manualFixtureRefresh());
      assert.deepEqual(await page.evaluate(() => window.btcSpotChart.getManualLevels().levels.map((level) => level.price)), [79500, 77000, 75000]);
      await settle();
      assert(bluePixels(await page.locator('#spotChartCanvas').screenshot()).fill > 1000, 'Short TP bands must render below entry');
      await page.locator('#btcSpotChartPanel').screenshot({ path: path.join(output, `manual-short-${mobile ? 'mobile' : 'desktop'}-fixture.png`) });
      readFails = true;
      await page.evaluate(() => window.__manualFixtureRefresh().catch(() => {}));
      assert.equal(await page.locator('#spotChartManualStatus').getAttribute('data-stale'), 'true');
      assert.equal(await page.evaluate(() => window.btcSpotChart.getManualLevels().planCount), 1, 'Failure retains last verified levels');
      readFails = false; plans = [];
      await page.evaluate(() => window.__manualFixtureRefresh()); await settle();
      assert.equal(await page.evaluate(() => window.btcSpotChart.getManualLevels().levels.length), 0);
      assert.equal(await page.locator('#spotChartManualStatus').getAttribute('data-stale'), 'false');
      assert(bluePixels(await page.locator('#spotChartCanvas').screenshot()).lines < pixels.lines / 3, 'Cancelled/removed plans leave no blue lines');
      assert.deepEqual(errors, []); assert.deepEqual(writes, []);
      results.push({ mobile, rightBuffer, pixels, hidden: off, bandBlueValues: blues, nativePriceLabels: true, editsAndRemoval: true,
        independentLongShort: true, staleReadsPreserved: true, export: true, browserErrors: errors, writes });
      await context.close();
    }
    console.log(JSON.stringify(results, null, 2));
  } finally { await browser.close(); }
})().catch((error) => { console.error(error.stack); process.exitCode = 1; });
