const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require('playwright');
const { PNG } = require('pngjs');

const url = process.argv[2] || 'http://127.0.0.1:4318/decentrader/map?source=open-v2';
const output = process.argv[3];
if (!output) throw new Error('Supply an output directory for chart screenshots.');
fs.mkdirSync(output, { recursive: true });

function candlePixels(buffer) {
  const png = PNG.sync.read(buffer);
  let up = 0, down = 0;
  for (let i = 0; i < png.data.length; i += 4) {
    const [r, g, b] = png.data.subarray(i, i + 3);
    if (r < 90 && g > 110 && b > 90 && g > r * 1.5) up += 1;
    if (r > 170 && g < 120 && b < 130) down += 1;
  }
  assert(up > 100 && down > 100, `Canvas must contain visible up/down candles (${up}/${down}).`);
  return { up, down, width: png.width, height: png.height };
}

function fractalPixels(buffer) {
  const png = PNG.sync.read(buffer);
  let daily = 0, weekly = 0, monthly = 0;
  for (let i = 0; i < png.data.length; i += 4) {
    const [r, g, b] = png.data.subarray(i, i + 3);
    if (r > 225 && g > 200 && b < 100) daily++;
    if (r > 225 && g > 110 && g < 180 && b < 70) weekly++;
    if (r > 250 && g < 100 && b > 65 && b < 100) monthly++;
  }
  assert(daily > 100 && weekly > 100, `D/W lines must both render (${daily}/${weekly}).`);
  assert(monthly > 100, `Monthly lines must render red (${monthly}).`);
  return { daily, weekly, monthly };
}

async function checkHourlyBoxes(page) {
  assert.equal(await page.locator('#spotChartHourlyBoxes').isChecked(), true, '1H boxes must be enabled by default.');
  const model = await page.evaluate(() => window.btcSpotChart.getHourlyFractalBoxes());
  assert(model.boxes.length > 0 && model.fractals.length > 0, 'Confirmed 1H ranges must exist.');
  await page.mouse.move(0, 0);
  const on = PNG.sync.read(await page.locator('#spotChartCanvas').screenshot());
  await page.locator('#spotChartHourlyBoxes').uncheck();
  await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  const off = PNG.sync.read(await page.locator('#spotChartCanvas').screenshot());
  assert.equal(on.width, off.width); assert.equal(on.height, off.height);
  let grayFillPixels = 0;
  for (let i = 0; i < on.data.length; i += 4) {
    const delta = [0, 1, 2].map((channel) => on.data[i + channel] - off.data[i + channel]);
    if (Math.min(...delta) > 20 && Math.max(...delta) - Math.min(...delta) < 12) grayFillPixels++;
  }
  // The reserved future area leaves fewer historical box pixels on a phone.
  const minimumGrayPixels = Math.min(500, Math.round(on.width * .5));
  assert(grayFillPixels > minimumGrayPixels, `Toggle must reveal a real translucent gray box layer (${grayFillPixels}).`);
  await page.locator('#spotChartHourlyBoxes').check();
  return { boxes: model.boxes.length, confirmedPivots: model.fractals.length, grayFillPixels };
}

function greenPixels(buffer) {
  const png = PNG.sync.read(buffer);
  let count = 0;
  for (let i = 0; i < png.data.length; i += 4) {
    const [r, g, b] = png.data.subarray(i, i + 3);
    if (r > 100 && r < 220 && g > 175 && g - r > 30 && g - b > 30) count++;
  }
  return count;
}

async function checkGapBoxes(page) {
  await page.waitForFunction(() => window.btcSpotChart?.getGapProjections()?.boxes.length > 0, null, { timeout: 40_000 });
  // Isolate green labels from manual plans that may share the exact same price.
  const manualVisible = await page.locator('#spotChartManual').isChecked();
  if (manualVisible) await page.locator('#spotChartManual').uncheck();
  await page.evaluate(() => window.btcSpotChart.candleSeries.priceScale().setAutoScale(true));
  await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  assert.equal(await page.locator('#spotChartGap').isChecked(), true);
  assert.equal(await page.locator('#spotChartGapLead').inputValue(), '10');
  const state = await page.evaluate(() => ({ model: window.btcSpotChart.getGapProjections(), latest: window.BTC_SPOT_GAP_TIMELINE.records.at(-1) }));
  const current = state.model.boxes.find((box) => box.openEnded);
  assert.equal(current.lower, state.latest.lower); assert.equal(current.upper, state.latest.upper);
  const screenshot = await page.locator('#spotChartCanvas').screenshot();
  const on = greenPixels(screenshot);
  assert(on > 100, `Gap rectangle must render light-green edges (${on}).`);
  const axis = await page.evaluate((gap) => ({ width: window.btcSpotChart.chart.priceScale('right').width(),
    ys: [gap.upper, gap.lower].map((price) => window.btcSpotChart.candleSeries.priceToCoordinate(price)) }), current);
  const pixels = PNG.sync.read(screenshot);
  for (const y of axis.ys) {
    let labelPixels = 0;
    for (let row = Math.max(0, Math.floor(y) - 13); row < Math.min(pixels.height, Math.ceil(y) + 14); row++) {
      for (let col = pixels.width - axis.width + 4; col < pixels.width - 2; col++) {
        const index = (row * pixels.width + Math.floor(col)) * 4;
        const [r, g, b] = pixels.data.subarray(index, index + 3);
        if (g > 175 && g - r > 30 && g - b > 30) labelPixels++;
      }
    }
    assert(labelPixels > 200, `Each active edge must have a green right-axis price label (${labelPixels}).`);
  }
  await page.locator('#spotChartGap').uncheck();
  const off = greenPixels(await page.locator('#spotChartCanvas').screenshot());
  assert(off < on / 2, 'Gap toggle must hide the green rectangle.');
  await page.locator('#spotChartGap').check();
  await page.locator('#spotChartGapLead').fill('6');
  await page.locator('#spotChartGapLead').press('Tab');
  assert((await page.evaluate(() => window.btcSpotChart.chart.timeScale().scrollPosition())) >= 56);
  await page.locator('#spotChartGapLead').fill('10');
  await page.locator('#spotChartGapLead').press('Tab');
  await page.waitForFunction(() => {
    const api = window.btcSpotChart, scale = api.chart.timeScale();
    const x = scale.logicalToCoordinate(api.getCandles().length - 1 + Number(document.getElementById('spotChartGapLead').value));
    return x !== null && x > 0 && x < scale.width() - 2;
  });
  if (manualVisible) await page.locator('#spotChartManual').check();
  return { lower: current.lower, upper: current.upper, fromTime: current.fromTime, startSource: current.startSource, greenPixels: on, hiddenPixels: off, adjustableLead: true, activePriceAxisLabels: true };
}

async function checkHourRefresh(browser, url, output) {
  const hour = 3_600_000;
  const initial = Date.parse('2026-10-21T08:00:00Z');
  const origin = Date.parse('2026-10-15T07:00:00Z') / 1000;
  let now = initial + 5000, projectionReads = 0;
  const context = await browser.newContext({ viewport: { width: 1200, height: 950 } });
  try {
    const page = await context.newPage();
    await page.addInitScript((time) => {
      window.__fractalTestNow = time;
      Date.now = () => window.__fractalTestNow;
      window.WebSocket = class { close() { this.onclose?.(); } };
    }, now);
    await page.route('**/*', async (route) => {
      const path = new URL(route.request().url()).pathname;
      if (path.startsWith('/assets/') || path === '/decentrader/map') return route.continue();
      const through = Math.floor(now / hour) * hour;
      if (path.endsWith('/btc-spot-candles')) {
        const first = initial - 999 * hour;
        const rows = Array.from({ length: (through - first) / hour + 1 }, (_, i) => {
          const time = first + i * hour;
          return { time: time / 1000, closeTime: time + hour - 1, open: 90, high: time >= initial ? 101 : time / 1000 === origin ? 100 : 91,
            low: time === Date.parse('2026-09-30T07:00:00Z') ? 80 : time === initial - 2 * hour ? 85 : 89,
            close: 90, volume: 1, quoteVolume: 90, closed: time < through };
        });
        return route.fulfill({ json: { symbol: 'BTCUSDT', venue: 'Binance Spot', interval: '1h', fetchedAt: new Date(now).toISOString(), candles: rows, hasMore: false } });
      }
      if (path.endsWith('/btc-spot-fractal-projections')) {
        projectionReads++;
        const make = (interval, record, latestClosedCandleAt) => ({ symbol: 'BTCUSDT', venue: 'Binance Spot', interval, records: [record], latestClosedCandleAt });
        return route.fulfill({ json: {
          symbol: 'BTCUSDT', venue: 'Binance Spot', evaluatedThrough: new Date(through).toISOString(),
          daily: make('1d', { id: 'd', type: 'HIGH', priceExact: '100.00', pivotAt: '2026-10-15T00:00:00Z', confirmedAt: '2026-10-17T23:59:59.999Z', isLatestForType: true }, '2026-10-20T23:59:59.999Z'),
          weekly: make('1w', { id: 'w', type: 'LOW', priceExact: '80.00', pivotAt: '2026-09-28T00:00:00Z', confirmedAt: '2026-10-18T23:59:59.999Z', isLatestForType: true }, '2026-10-18T23:59:59.999Z'),
          monthly: make('1M', { id: 'm', type: 'HIGH', priceExact: '200.00', pivotAt: '2026-07-01T00:00:00Z', confirmedAt: '2026-09-30T23:59:59.999Z', isLatestForType: true }, '2026-09-30T23:59:59.999Z')
        } });
      }
      return route.fulfill({ status: 503, json: { ok: false } });
    });
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    await page.locator('#btcSpotChartPanel').scrollIntoViewIfNeeded();
    await page.waitForFunction(() => window.btcSpotChart?.getFractalProjections()?.levels.length === 3);
    const sendGap = () => page.evaluate((time) => {
      const records = [{ candleTime: time / 1000 - 3600, availableAt: time / 1000, lower: 80, upper: 105 }];
      if (Date.now() >= time + 3_600_000) records.push({ candleTime: time / 1000, availableAt: time / 1000 + 3600, lower: 82, upper: 108 });
      window.dispatchEvent(new CustomEvent('btc-spot-chart:gap-timeline', { detail: { symbol: 'BTCUSDT', venue: 'Binance Spot', records } }));
    }, initial);
    await sendGap();
    assert.equal(await page.evaluate(() => window.btcSpotChart.getFractalProjections().levels[0].broken), false, 'Open 1H excursion must not break the level.');
    assert.equal(await page.evaluate(() => window.btcSpotChart.getHourlyFractalBoxes().fractals.some((pivot) => pivot.price === 85)), false, 'Open second right candle must not confirm the 1H pivot.');
    now = initial + hour + 5000;
    await page.evaluate((value) => { window.__fractalTestNow = value; }, now);
    await page.waitForFunction(() => window.btcSpotChart.getFractalProjections().levels[0].broken, null, { timeout: 15_000 });
    const broken = await page.evaluate(() => window.btcSpotChart.getFractalProjections().levels[0]);
    assert.equal(broken.originTime, origin);
    assert.equal(broken.brokenTime, initial / 1000, 'The first closed breaking hour must be the endpoint.');
    const hourlyBox = await page.evaluate(() => window.btcSpotChart.getHourlyFractalBoxes().boxes.at(-1));
    assert.equal(hourlyBox.low, 85, 'Newly closed second right candle must confirm the 1H box.');
    assert.equal(hourlyBox.fromTime, (initial - 2 * hour) / 1000, 'Box must anchor at the original wick.');
    assert.equal(hourlyBox.confirmedAt, (initial + hour) / 1000, 'Confirmation must retain its real later availability time.');
    assert.equal(projectionReads, 2, 'Only one projection read per hour.');
    await page.waitForFunction(() => document.getElementById('spotChartGapStatus').dataset.stale === 'true');
    await sendGap();
    const gapModel = await page.evaluate(() => window.btcSpotChart.getGapProjections());
    assert.equal(gapModel.boxes.length, 2);
    assert.equal(gapModel.boxes[1].fromTime, (initial + hour) / 1000, 'Gap transition must align with the source candle close, not its fetch time.');
    assert.equal(gapModel.boxes[1].lower, 82);
    assert.equal(await page.locator('#spotChartGapStatus').getAttribute('data-stale'), 'false');
    await page.locator('[data-spot-range="all"]').click();
    await page.locator('#btcSpotChartPanel').screenshot({ path: path.join(output, 'btc-spot-hour-refresh-fixture.png') });
    return { hourlyRefreshWithoutClick: true, openCandleIgnored: true, exactBreakHour: true, hourlyBoxConfirmedAfterClose: true, gapTransitionAfterClose: true, staleGapIdentified: true, projectionReads };
  } finally { await context.close(); }
}

(async () => {
  const browser = await chromium.launch({ headless: true, ...(process.env.SPOT_CHART_BROWSER_CHANNEL ? { channel: process.env.SPOT_CHART_BROWSER_CHANNEL } : {}) });
  const errors = [];
  const writes = [];
  try {
    const desktop = await browser.newContext({ viewport: { width: 1440, height: 1000 }, acceptDownloads: true });
    const page = await desktop.newPage();
    page.on('pageerror', (error) => errors.push(error.message));
    page.on('request', (request) => { if (!['GET', 'HEAD', 'OPTIONS'].includes(request.method())) writes.push(request.url()); });
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    await page.locator('#btcSpotChartPanel').scrollIntoViewIfNeeded();
    await page.waitForFunction(() => window.btcSpotChart?.getCandles().length >= 500, null, { timeout: 40_000 });
    const feed = await page.evaluate(() => ({ symbol: window.btcSpotChart.symbol, venue: window.btcSpotChart.venue, count: window.btcSpotChart.getCandles().length }));
    assert.equal(feed.symbol, 'BTCUSDT'); assert.equal(feed.venue, 'Binance Spot');
    await page.waitForFunction(() => window.btcSpotChart?.getFractalProjections()?.levels.length > 0, null, { timeout: 40_000 });
    const fractalLevels = await page.evaluate(() => window.btcSpotChart.getFractalProjections());
    const currents = fractalLevels.levels.filter((level) => level.isCurrent && !level.broken);
    assert.equal(currents.filter((level) => level.timeframe !== 'M').length, 4, 'Current Daily and Weekly highs/lows must all be present.');
    assert.equal(fractalLevels.levels.filter((level) => level.timeframe === 'M' && level.isCurrent).length, 2, 'Latest Monthly high and low must both be present, including broken levels.');
    await page.waitForFunction(() => document.querySelectorAll('#monthlyFractalHistoryRows td').length > 6);
    const monthlySnapshot = await page.evaluate(async () => (await fetch('/research/binance-monthly-fractals?market=BTC-USD')).json());
    assert.equal(monthlySnapshot.interval, '1M'); assert.equal(monthlySnapshot.venue, 'Binance Spot');
    assert.equal(await page.locator('#monthlyFractalHistoryRows tr').count(), monthlySnapshot.records.length);
    assert.equal(await page.locator('#monthlyIntactFractalRows tr').count(), monthlySnapshot.records.filter((row) => !row.firstBrokenAt).length);
    await page.locator('#monthlyFractalHistoryCard').scrollIntoViewIfNeeded();
    await page.locator('#monthlyFractalHistoryCard').screenshot({ path: path.join(output, 'monthly-fractal-tables-desktop.png') });
    await page.locator('#btcSpotChartPanel').scrollIntoViewIfNeeded();
    for (const level of fractalLevels.levels.filter((item) => item.originTime !== null)) {
      const wick = await page.evaluate((time) => window.btcSpotChart.getCandles().find((row) => row.time === time), level.originTime);
      assert.equal(wick[level.type === 'HIGH' ? 'high' : 'low'], level.price, 'Origin must be the actual Spot 1H wick.');
    }
    const span = () => page.evaluate(() => { const r = window.btcSpotChart.chart.timeScale().getVisibleLogicalRange(); return r.to - r.from; });
    const originalSpan = await span();
    await page.locator('#spotChartZoomIn').click();
    await page.waitForFunction((previous) => { const r = window.btcSpotChart.chart.timeScale().getVisibleLogicalRange(); return r.to - r.from < previous; }, originalSpan);
    assert((await span()) < originalSpan);
    await page.locator('#spotChartZoomOut').click();
    await page.locator('#spotChartVolume').uncheck();
    assert.equal(await page.evaluate(() => window.btcSpotChart.volumeSeries.options().visible), false);
    await page.locator('#spotChartVolume').check();
    await page.locator('#spotChartGrid').uncheck();
    assert.equal(await page.evaluate(() => window.btcSpotChart.chart.options().grid.horzLines.visible), false);
    await page.locator('#spotChartGrid').check();
    const candleTime = await page.evaluate(() => window.btcSpotChart.getCandles().at(-1).time);
    await page.locator('#spotChartTimezone').selectOption('UTC');
    assert.equal(await page.evaluate(() => window.btcSpotChart.getCandles().at(-1).time), candleTime);
    await page.locator('#spotChartTimezone').selectOption('Europe/Amsterdam');
    await page.locator('[data-spot-range="24"]').click();
    await page.waitForFunction(() => { const r = window.btcSpotChart.chart.timeScale().getVisibleLogicalRange(); return r.to - r.from <= 84; });
    assert((await span()) <= 84);
    await page.locator('[data-spot-range="120"]').click();
    await page.locator('#spotChartFullscreen').click();
    await page.waitForFunction(() => document.fullscreenElement?.id === 'btcSpotChartPanel');
    await page.locator('#spotChartFullscreen').click();
    const count = await page.evaluate(() => window.btcSpotChart.getCandles().length);
    await page.evaluate(() => window.btcSpotChart.chart.timeScale().setVisibleLogicalRange({ from: -5, to: 80 }));
    await page.waitForFunction((previous) => window.btcSpotChart.getCandles().length > previous, count, { timeout: 25_000 });
    await page.locator('#spotChartReset').click();
    await page.locator('#spotChartFitFractals').click();
    assert.equal(await page.locator('#spotChartFitFractals').getAttribute('aria-pressed'), 'true');
    await page.locator('#spotChartDaily').uncheck();
    await page.locator('#spotChartDaily').check();
    await page.locator('#spotChartWeekly').uncheck();
    await page.locator('#spotChartWeekly').check();
    await page.locator('#spotChartMonthly').uncheck();
    await page.locator('#spotChartMonthly').check();
    await page.locator('#spotChartBroken').uncheck();
    await page.locator('#spotChartBroken').check();
    const box = await page.locator('#spotChartCanvas').boundingBox();
    await page.mouse.move(box.x + box.width * .6, box.y + box.height * .4);
    const desktopHourlyBoxes = await checkHourlyBoxes(page);
    const desktopGap = await checkGapBoxes(page);
    const desktopPixels = candlePixels(await page.locator('#spotChartCanvas').screenshot());
    await page.locator('#btcSpotChartPanel').screenshot({ path: path.join(output, 'btc-spot-desktop.png') });
    const desktopFractals = fractalPixels(await page.locator('#spotChartCanvas').screenshot());
    const downloadPromise = page.waitForEvent('download');
    await page.locator('#spotChartDownload').click();
    const download = await downloadPromise;
    assert(download.suggestedFilename().startsWith('BTCUSDT-Spot-1H-'));
    await download.saveAs(path.join(output, 'btc-spot-export.png'));
    fractalPixels(fs.readFileSync(path.join(output, 'btc-spot-export.png')));
    assert(greenPixels(fs.readFileSync(path.join(output, 'btc-spot-export.png'))) > 100, 'PNG export must include gap rectangle edges.');
    let live = true;
    try { await page.waitForFunction(() => document.getElementById('spotChartFeed').dataset.state === 'live', null, { timeout: 12_000 }); }
    catch { live = false; }

    const other = await desktop.newPage();
    let btcRequests = 0;
    other.on('request', (request) => { if (/btc-spot-(candles|fractal-projections)/.test(request.url())) btcRequests += 1; });
    const otherUrl = new URL(url); otherUrl.searchParams.set('source', 'open-v2-eth');
    await other.goto(otherUrl.href, { waitUntil: 'domcontentloaded' });
    assert.equal(await other.locator('#btcSpotChartPanel').isVisible(), false);
    assert.equal(btcRequests, 0);
    await other.waitForFunction(() => document.getElementById('monthlyFractalHistoryTitle').textContent.includes('ETH-USD'));
    assert((await other.locator('#monthlyFractalHistoryRows tr').count()) > 1);

    const mobile = await browser.newContext({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true });
    const phone = await mobile.newPage();
    phone.on('pageerror', (error) => errors.push(error.message));
    await phone.goto(url, { waitUntil: 'domcontentloaded' });
    await phone.locator('#btcSpotChartPanel').scrollIntoViewIfNeeded();
    await phone.waitForFunction(() => window.btcSpotChart?.getCandles().length >= 500, null, { timeout: 40_000 });
    await phone.waitForFunction(() => window.btcSpotChart?.getFractalProjections()?.levels.length > 0, null, { timeout: 40_000 });
    await phone.waitForFunction(() => document.querySelectorAll('#monthlyFractalHistoryRows td').length > 6);
    await phone.locator('#monthlyFractalHistoryCard').scrollIntoViewIfNeeded();
    assert.equal(await phone.locator('#monthlyFractalHistoryCard').evaluate((node) => node.scrollWidth > node.clientWidth + 1), false);
    await phone.locator('#monthlyFractalHistoryCard').screenshot({ path: path.join(output, 'monthly-fractal-tables-mobile.png') });
    await phone.locator('#btcSpotChartPanel').scrollIntoViewIfNeeded();
    await phone.locator('#spotChartFitFractals').tap();
    const overflow = await phone.locator('#btcSpotChartPanel').evaluate((node) => node.scrollWidth > node.clientWidth + 1);
    assert.equal(overflow, false, 'Chart must not overflow its mobile container.');
    await phone.locator('#spotChartZoomIn').tap();
    const mobileHourlyBoxes = await checkHourlyBoxes(phone);
    const mobileGap = await checkGapBoxes(phone);
    const mobilePixels = candlePixels(await phone.locator('#spotChartCanvas').screenshot());
    const mobileFractals = fractalPixels(await phone.locator('#spotChartCanvas').screenshot());
    await phone.locator('#btcSpotChartPanel').screenshot({ path: path.join(output, 'btc-spot-mobile.png') });
    assert.deepEqual(errors, [], 'No uncaught browser exceptions');
    assert.deepEqual(writes, [], 'Chart interactions must not submit orders, plans, settings or SMTP requests');
    const hourlyFixture = await checkHourRefresh(browser, url, output);
    console.log(JSON.stringify({ feed, currentFractals: currents, monthlyTables: { records: monthlySnapshot.records.length, intact: monthlySnapshot.records.filter((row) => !row.firstBrokenAt).length, ethVerified: true, mobileOverflow: false }, projectionCutoff: fractalLevels.evaluatedThrough, controls: 'zoom, range, volume, grid, timezone, fullscreen, PNG export, Daily/Weekly/Monthly/Broken, Fit levels, 1H boxes, gap rectangles/lead', olderHistoryLoaded: true, otherPairsUnaffected: true, actualWebSocketLive: live, desktopPixels, mobilePixels, desktopFractals, mobileFractals, desktopHourlyBoxes, mobileHourlyBoxes, desktopGap, mobileGap, hourlyFixture, browserErrors: errors, writeRequests: writes }, null, 2));
  } finally { await browser.close(); }
})().catch((error) => { console.error(error.stack); process.exitCode = 1; });
