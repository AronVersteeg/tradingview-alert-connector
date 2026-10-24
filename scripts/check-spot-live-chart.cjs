const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require('playwright');
const { PNG } = require('pngjs');
const [url, output] = process.argv.slice(2);
if (!url || !output) throw new Error('Supply a safe preview URL and screenshot directory');
const eth = new URL(url).searchParams.get('source') === 'open-v2-eth';
const symbol = eth ? 'ETHUSDT' : 'BTCUSDT';
fs.mkdirSync(output, { recursive: true });
(async () => {
  const browser = await chromium.launch({ headless: true, channel: process.env.SPOT_CHART_BROWSER_CHANNEL || 'msedge' });
  const results = [];
  try {
    for (const mobile of [false, true]) {
      const context = await browser.newContext({ viewport: mobile ? { width: 390, height: 844 } : { width: 1440, height: 1000 }, isMobile: mobile, hasTouch: mobile });
      const page = await context.newPage(), errors = [], writes = [], wrongMarketReads = [];
      page.on('pageerror', error => errors.push(error.message));
      await context.route('**/*', route => {
        const req = route.request(), target = new URL(req.url());
        if (!['GET', 'HEAD', 'OPTIONS'].includes(req.method())) { writes.push(req.method()); return route.abort(); }
        if (eth && (target.pathname.startsWith('/open-liquidity/v2/btc-spot-') || target.pathname === '/decentrader/liquidity-timelapse')) wrongMarketReads.push(req.url());
        return route.continue();
      });
      await page.goto(url, { waitUntil: 'domcontentloaded' });
      try {
        await page.waitForFunction(() => window.BTC_SPOT_GAP_TIMELINE?.records.length > 0, null, { timeout: 25_000 });
      } catch (error) {
        console.error({ errors, state: await page.evaluate(() => ({
          gap: document.getElementById('spotChartGapStatus').textContent,
          live: document.getElementById('liveStatus').textContent,
          alert: document.getElementById('gapAlert').textContent
        })) });
        throw error;
      }
      await page.locator('#btcSpotChartPanel').scrollIntoViewIfNeeded();
      try {
        await page.waitForFunction(() => window.btcSpotChart?.getCandles().length >= 1000 && window.btcSpotChart.getFractalProjections()?.levels.length > 0 &&
          window.btcSpotChart.getGapProjections()?.boxes.length > 0, null, { timeout: 60_000 });
      } catch (error) {
        console.error(await page.evaluate(() => ({ candles: window.btcSpotChart?.getCandles().length,
          fractals: document.getElementById('spotChartFractalStatus').textContent,
          gaps: document.getElementById('spotChartGapStatus').textContent,
          feed: document.getElementById('spotChartFeed').textContent })));
        throw error;
      }
      const state = await page.evaluate(() => {
        const api = window.btcSpotChart, levels = api.getFractalProjections().levels;
        return { symbol: api.symbol, market: api.market, venue: api.venue, candles: api.getCandles().length,
          timeframes: [...new Set(levels.map(l => l.timeframe))].sort(), hourlyBoxes: api.getHourlyFractalBoxes().boxes.length,
          gapBoxes: api.getGapProjections().boxes.length, manualMarket: api.getManualLevels()?.market,
          intrusionSource: api.getIntrusionDailyBreakouts()?.intrusionSource };
      });
      assert.equal(state.symbol, symbol); assert.equal(state.venue, 'Binance Spot'); assert.deepEqual(state.timeframes, ['D', 'M', 'W']);
      if (state.manualMarket) assert.equal(state.manualMarket, state.market);
      const png = PNG.sync.read(await page.locator('#spotChartCanvas').screenshot());
      let greenCandles = 0, redCandles = 0;
      for (let i = 0; i < png.data.length; i += 4) {
        const [r, g, b] = png.data.subarray(i, i + 3);
        if (r < 90 && g > 110 && b > 90) greenCandles++;
        if (r > 170 && g < 120 && b < 130) redCandles++;
      }
      assert(greenCandles > 100 && redCandles > 100, 'Real candles must render in the canvas');
      assert.equal(await page.locator('#btcSpotChartPanel').evaluate(node => node.scrollWidth > node.clientWidth + 1), false);
      await page.locator('#btcSpotChartPanel').screenshot({ path: path.join(output, `${symbol}-${mobile ? 'mobile' : 'desktop'}-live.png`) });
      assert.deepEqual(errors, []); assert.deepEqual(writes, []); assert.deepEqual(wrongMarketReads, []);
      results.push({ mobile, ...state, greenCandles, redCandles, errors, writes, wrongMarketReads });
      await context.close();
    }
    console.log(JSON.stringify(results, null, 2));
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
