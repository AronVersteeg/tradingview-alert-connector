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
    await page.waitForFunction(() => { const r = window.btcSpotChart.chart.timeScale().getVisibleLogicalRange(); return r.to - r.from < 32; });
    assert((await span()) < 32);
    await page.locator('[data-spot-range="120"]').click();
    await page.locator('#spotChartFullscreen').click();
    await page.waitForFunction(() => document.fullscreenElement?.id === 'btcSpotChartPanel');
    await page.locator('#spotChartFullscreen').click();
    const count = await page.evaluate(() => window.btcSpotChart.getCandles().length);
    await page.evaluate(() => window.btcSpotChart.chart.timeScale().setVisibleLogicalRange({ from: -5, to: 80 }));
    await page.waitForFunction((previous) => window.btcSpotChart.getCandles().length > previous, count, { timeout: 25_000 });
    await page.locator('#spotChartReset').click();
    const box = await page.locator('#spotChartCanvas').boundingBox();
    await page.mouse.move(box.x + box.width * .6, box.y + box.height * .4);
    const desktopPixels = candlePixels(await page.locator('#spotChartCanvas').screenshot());
    await page.locator('#btcSpotChartPanel').screenshot({ path: path.join(output, 'btc-spot-desktop.png') });
    const downloadPromise = page.waitForEvent('download');
    await page.locator('#spotChartDownload').click();
    const download = await downloadPromise;
    assert(download.suggestedFilename().startsWith('BTCUSDT-Spot-1H-'));
    await download.saveAs(path.join(output, 'btc-spot-export.png'));
    let live = true;
    try { await page.waitForFunction(() => document.getElementById('spotChartFeed').dataset.state === 'live', null, { timeout: 12_000 }); }
    catch { live = false; }

    const other = await desktop.newPage();
    let btcRequests = 0;
    other.on('request', (request) => { if (request.url().includes('btc-spot-candles')) btcRequests += 1; });
    const otherUrl = new URL(url); otherUrl.searchParams.set('source', 'open-v2-eth');
    await other.goto(otherUrl.href, { waitUntil: 'domcontentloaded' });
    assert.equal(await other.locator('#btcSpotChartPanel').isVisible(), false);
    assert.equal(btcRequests, 0);

    const mobile = await browser.newContext({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true });
    const phone = await mobile.newPage();
    phone.on('pageerror', (error) => errors.push(error.message));
    await phone.goto(url, { waitUntil: 'domcontentloaded' });
    await phone.locator('#btcSpotChartPanel').scrollIntoViewIfNeeded();
    await phone.waitForFunction(() => window.btcSpotChart?.getCandles().length >= 500, null, { timeout: 40_000 });
    const overflow = await phone.locator('#btcSpotChartPanel').evaluate((node) => node.scrollWidth > node.clientWidth + 1);
    assert.equal(overflow, false, 'Chart must not overflow its mobile container.');
    await phone.locator('#spotChartZoomIn').tap();
    const mobilePixels = candlePixels(await phone.locator('#spotChartCanvas').screenshot());
    await phone.locator('#btcSpotChartPanel').screenshot({ path: path.join(output, 'btc-spot-mobile.png') });
    assert.deepEqual(errors, [], 'No uncaught browser exceptions');
    assert.deepEqual(writes, [], 'Chart interactions must not submit orders, plans, settings or SMTP requests');
    console.log(JSON.stringify({ feed, controls: 'zoom, range, volume, grid, timezone, fullscreen, PNG export', olderHistoryLoaded: true, otherPairsUnaffected: true, actualWebSocketLive: live, desktopPixels, mobilePixels, browserErrors: errors, writeRequests: writes }, null, 2));
  } finally { await browser.close(); }
})().catch((error) => { console.error(error.stack); process.exitCode = 1; });
