const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require('playwright');
const { PNG } = require('pngjs');
const [url, output] = process.argv.slice(2);
if (!url || !output) throw new Error('Supply preview URL and screenshot directory');
const eth = new URL(url).searchParams.get('source') === 'open-v2-eth';
const market = eth ? 'ETH-USD' : 'BTC-USD', symbol = eth ? 'ETHUSDT' : 'BTCUSDT', asset = eth ? 'eth' : 'btc';
fs.mkdirSync(output, { recursive: true });
const html = fs.readFileSync(path.join(__dirname, '../public/decentrader_liquidity_timelapse.html'), 'utf8')
  .replace('    wireViewer();', '    window.__weeklyFixtureRefresh = () => refreshPayload().catch(error => { showRefreshError(error); throw error; });\n    wireViewer();');
const HOUR = 3_600_000, start = Date.parse('2026-09-28T00:00:00Z'), day = Date.parse('2026-10-19T00:00:00Z');
const iso = time => new Date(time).toISOString();
function intrusionPayload() {
  if (eth) return { source: { market, sourceStatuses: [{ name: 'Binance Spot ETHUSDT 1H' }] },
    intrusionBenchmarks: { history: intrusionPayloadBtc().intrusionDomStudy.history }, delayHistory: intrusionPayloadBtc().delayHistory };
  return intrusionPayloadBtc();
}
function intrusionPayloadBtc() {
  return { source: { url: 'https://www.decentrader.com/liquidity-maps/?coin=btc', method: 'getOHLCHourlyCalculations' },
    intrusionDomStudy: { history: [
      { timestamp: iso(day + 3 * HOUR).slice(0, 19).replace('T', ' '), direction: 'long', normalEmailSentAt: iso(day + 12 * HOUR) },
      { timestamp: iso(day + 4 * HOUR).slice(0, 19).replace('T', ' '), direction: 'long' },
      { timestamp: iso(day + 5 * HOUR).slice(0, 19).replace('T', ' '), direction: 'short', filteredEmailSentAt: iso(day + 12 * HOUR) }
    ] }, delayHistory: { records: [
      { intrusionTimestamp: iso(day + 3 * HOUR).slice(0, 19).replace('T', ' '), sideCounts: '1 left edge', smtpSentAt: iso(day + 12 * HOUR) }
    ] } };
}
const special = { 3: [100, 128, 98, 125], 4: [125, 133, 123, 130], 5: [130, 132, 72, 75],
  6: [75, 77, 68, 70], 7: [70, 102, 68, 100], 8: [100, 155, 99, 150] };
function candles(now) {
  return Array.from({ length: Math.floor((now - start) / HOUR) + 1 }, (_, i) => {
    const time = start + i * HOUR, values = special[(time - day) / HOUR] || [100, 110, 90, 100];
    const [open, high, low, close] = values;
    return { time: time / 1000, open, high: time === Date.parse('2026-09-29T07:00:00Z') ? 120
      : time === Date.parse('2026-10-15T07:00:00Z') ? 118 : high,
      low: time === Date.parse('2026-10-16T07:00:00Z') ? 82 : low, close, volume: 1, quoteVolume: 100, closed: time + HOUR <= now };
  });
}
function payload(now) {
  const rows = candles(now).filter(c => c.closed);
  return { ...(eth ? intrusionPayload() : {}), modelVersion: 'binance-spot-liquidation-cohorts-v2.4', source: { market, sourceStatuses: [{ name: `Binance Spot ${symbol} 1H` }] },
    range: { minPrice: 60, maxPrice: 160 }, events: [],
    frames: rows.map((c, i) => ({ i, startedAtMs: c.time * 1000, t: iso(c.time * 1000).slice(0, 19).replace('T', ' '), price: (c.open + c.high + c.low + c.close) / 4 })),
    gaps: rows.map(c => { const left = c.time * 1000 >= day + 5 * HOUR ? 60 : 85, right = c.time * 1000 >= day + 3 * HOUR ? 140 : 115;
      return { left, right, width: right - left, leftEdge: { leverage: 10, relativeCount: 1 }, rightEdge: { leverage: 10, relativeCount: 1 } }; }) };
}
function bundle(now) {
  const through = Math.floor(now / HOUR) * HOUR;
  const snapshot = (interval, close, records = []) => ({ venue: 'Binance Spot', symbol, interval, latestClosedCandleAt: close, records });
  const record = (id, type, price, pivot, confirmed, broken) => ({ id, type, priceExact: String(price), pivotAt: pivot,
    confirmedAt: confirmed, isLatestForType: true, ...(broken ? { firstBrokenAt: iso(day) } : {}) });
  return { symbol, venue: 'Binance Spot', evaluatedThrough: iso(through),
    daily: snapshot('1d', '2026-10-18T23:59:59.999Z', [
      record('high', 'HIGH', 118, '2026-10-15T00:00:00Z', '2026-10-17T23:59:59.999Z', through >= day + 4 * HOUR),
      record('low', 'LOW', 82, '2026-10-16T00:00:00Z', '2026-10-18T23:59:59.999Z', through >= day + 6 * HOUR)
    ]), monthly: snapshot('1M', '2026-09-30T23:59:59.999Z', [
      record('high', 'HIGH', 160, '2026-06-01T00:00:00Z', '2026-08-31T23:59:59.999Z', false),
      record('low', 'LOW', 60, '2026-06-01T00:00:00Z', '2026-08-31T23:59:59.999Z', false)
    ]),
    weekly: snapshot('1w', '2026-10-18T23:59:59.999Z', [
      record('high', 'HIGH', 120, '2026-09-28T00:00:00Z', '2026-10-18T23:59:59.999Z', through >= day + 4 * HOUR),
      record('low', 'LOW', 80, '2026-09-21T00:00:00Z', '2026-10-11T23:59:59.999Z', through >= day + 6 * HOUR)
    ]) };
}
function markerColumn(buffer, x, daily = false) {
  const png = PNG.sync.read(buffer); let count = 0;
  for (let y = 5; y < png.height - 35; y++) for (let atX = Math.max(0, Math.floor(x) - 2); atX <= Math.min(png.width - 1, Math.ceil(x) + 2); atX++) {
    const at = (y * png.width + atX) * 4;
    if (daily ? png.data[at] > 190 && png.data[at + 1] > 190 && png.data[at + 2] > 100 && png.data[at + 2] < 190
      : png.data[at] > 190 && png.data[at + 1] > 100 && png.data[at + 1] < 180 && png.data[at + 2] < 65) count++;
  }
  return count;
}
const orangeColumn = (buffer, x) => markerColumn(buffer, x);
const yellowColumn = (buffer, x) => markerColumn(buffer, x, true);
function purpleColumn(buffer, x) {
  const png = PNG.sync.read(buffer); let count = 0;
  for (let y = 5; y < png.height - 35; y++) for (let atX = Math.max(0, Math.floor(x) - 2); atX <= Math.min(png.width - 1, Math.ceil(x) + 2); atX++) {
    const at = (y * png.width + atX) * 4;
    if (png.data[at] > 140 && png.data[at + 1] > 60 && png.data[at + 1] < 140 && png.data[at + 2] > 150) count++;
  }
  return count;
}
function pocPixels(buffer, cross) {
  const png = PNG.sync.read(buffer); let count = 0;
  for (let y = Math.floor(cross.y) - 1; y <= Math.ceil(cross.y) + 1; y++) {
    for (let x = Math.ceil(cross.x - 8); x < Math.floor(cross.x + 8); x++) {
      if (Math.abs(x - cross.x) < 3 || x < 0 || y < 0 || x >= png.width || y >= png.height) continue;
      const at = (y * png.width + x) * 4;
      if (png.data[at] > 140 && png.data[at + 1] > 60 && png.data[at + 1] < 140 && png.data[at + 2] > 150) count++;
    }
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
      const errors = [], writes = [], wrongMarketReads = []; let intrusionFailure = false, intrusionReads = 0, deltaReads = 0, pocReads = 0, pocReady = false;
      await context.addInitScript(value => { window.__weeklyNow = value; Date.now = () => window.__weeklyNow; window.WebSocket = class { close() {} }; }, now);
      await context.route('**/*', route => {
        const req = route.request(), target = new URL(req.url());
        if (!['GET', 'HEAD', 'OPTIONS'].includes(req.method())) { writes.push(req.method()); return route.abort(); }
        if (eth && (target.pathname.startsWith('/open-liquidity/v2/btc-spot-') || target.pathname.includes('btc-intrusion-') || target.pathname === '/decentrader/liquidity-timelapse' ||
            target.pathname.includes('gap-projection-history') || target.pathname === '/decentrader/manual-override' && target.searchParams.get('market') !== market)) wrongMarketReads.push(req.url());
        if (target.pathname === '/decentrader/map') return route.fulfill({ contentType: 'text/html', body: html });
        if (target.origin === new URL(url).origin && target.pathname.startsWith('/assets/')) return route.continue();
        if (target.pathname.endsWith(`/${asset}-spot-candles`)) return route.fulfill({ json: { symbol, venue: 'Binance Spot', interval: '1h',
          fetchedAt: iso(now), candles: candles(now), hasMore: false } });
        if (target.pathname.endsWith(`/${asset}-spot-fractal-projections`)) return route.fulfill({ json: bundle(now) });
        if (target.pathname.endsWith(`/${asset}-intrusion-spot-poc`)) {
          pocReads++;
          const hour = Number(target.searchParams.get('hour'));
          const long = hour === (day + 3 * HOUR) / 1000;
          assert([3, 5].some(offset => hour === (day + offset * HOUR) / 1000), 'POC belongs to an actual purple candle');
          return route.fulfill({ json: { symbol, venue: 'Binance Spot', interval: '1h', method: 'exact-price-base-volume',
            record: pocReady ? { hour, status: 'ok', poc: { hour, startedAt: hour, closedAt: hour + 3600,
              price: long ? 119.25 : 88.75, volumeBase: '100', priceVolumeBase: '10', tradeCount: 1000,
              aggregateTrades: 500, priceLevels: 50, pages: 1, fetchedAt: iso(now) } }
              : { hour, status: 'pending', pages: 1, retryAt: iso(now + 5000) } } });
        }
        if (target.pathname.endsWith(`/${asset}-intrusion-futures-delta`)) {
          deltaReads++;
          const hours = target.searchParams.get('hours').split(',').map(Number);
          assert(hours.length <= 8, 'Annotation requests must be bounded batches');
          return route.fulfill({ json: { symbol, venue: 'Binance USD-M Futures', interval: '5m', timeBasis: 'signal-last-five-minutes',
            records: hours.map(hour => { const buy = hour === (day + 3 * HOUR) / 1000 ? (eth ? 65 : 62.93) : (eth ? 40 : 34.78);
              return { hour, status: 'ok', candle: { hour, startedAt: hour + 3300, closedAt: hour + 3600,
                volumeBase: 100, takerBuyVolumeBase: buy, rawDeltaPct: (2 * buy / 100 - 1) * 100, fetchedAt: iso(now) } }; }) } });
        }
        if (target.pathname === '/open-liquidity/v2/liquidity-timelapse') return eth && intrusionFailure
          ? route.fulfill({ status: 503, json: { error: 'Fixture read failure' } }) : route.fulfill({ json: payload(now) });
        if (target.pathname === '/decentrader/liquidity-timelapse') {
          intrusionReads++;
          return intrusionFailure ? route.fulfill({ status: 503, json: { error: 'Fixture read failure' } })
            : route.fulfill({ json: intrusionPayload() });
        }
        if (target.pathname === '/decentrader/manual-override') return route.fulfill({ json: { enabled: true, market, overrides: [] } });
        return route.fulfill({ status: 503, json: { error: 'Unrelated source isolated in read-only fixture' } });
      });
      const page = await context.newPage(); page.on('pageerror', e => errors.push(e.message));
      await page.goto(url, { waitUntil: 'domcontentloaded' });
      await page.locator('#btcSpotChartPanel').scrollIntoViewIfNeeded();
      assert.equal(await page.locator('.spot-chart-symbol').textContent(), symbol);
      await page.waitForFunction(() => window.btcSpotChart?.getGapWeeklyBreakouts()?.signals.length === 1);
      await page.waitForFunction(() => window.btcSpotChart?.getGapDailyBreakouts()?.signals.length === 1);
      assert.equal(await page.locator('#spotChartGapWeeklyBreaks').isChecked(), true);
      assert.equal(await page.locator('#spotChartGapDailyBreaks').isChecked(), true);
      await page.waitForFunction(() => window.btcSpotChart?.getIntrusionDailyBreakouts()?.signals.length === 1);
      assert.equal(await page.locator('#spotChartIntrusionDailyBreaks').isChecked(), true);
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
      await page.waitForFunction(() => window.btcSpotChart?.getGapDailyBreakouts()?.signals.length === 2);
      const dailyState = await page.evaluate(() => window.btcSpotChart.getGapDailyBreakouts());
      assert.deepEqual(dailyState.signals.map(s => [s.time, s.direction]), state.signals.map(s => [s.time, s.direction]));
      assert.deepEqual(dailyState.signals.map(s => s.dailyLevels[0].price), [118, 82]);
      await page.waitForFunction(() => window.btcSpotChart?.getIntrusionDailyBreakouts()?.signals.length === 2);
      const intrusionState = await page.evaluate(() => window.btcSpotChart.getIntrusionDailyBreakouts());
      assert.deepEqual(intrusionState.signals.map(s => [s.time, s.direction]), state.signals.map(s => [s.time, s.direction]));
      assert.equal(intrusionState.signals[0].intrusion.alertTime, iso(day + 3 * HOUR), 'Use alert open time, not SMTP time');
      await page.locator('#spotChartIntrusionDailyBreaks').uncheck();
      for (const id of ['spotChartGapWeeklyBreaks', 'spotChartGapDailyBreaks']) await page.locator('#' + id).uncheck();
      await page.evaluate(time => {
        window.btcSpotChart.chart.timeScale().setVisibleRange({ from: time / 1000 - 6 * 3600, to: time / 1000 + 10 * 3600 });
        window.btcSpotChart.candleSeries.priceScale().setVisibleRange({ from: 55, to: 170 });
      }, day);
      await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
      const layers = PNG.sync.read(await page.locator('#spotChartCanvas').screenshot());
      const layerPixels = { daily: 0, weekly: 0, monthly: 0, gap: 0 };
      for (let i = 0; i < layers.data.length; i += 4) {
        const [r, g, b] = layers.data.subarray(i, i + 3);
        if (r > 130 && g > 130 && Math.abs(r - g) < 35 && b < 85) layerPixels.daily++;
        if (r > 130 && g > 75 && g < 180 && r > g * 1.3 && b < 70) layerPixels.weekly++;
        if (r > 250 && g < 100 && b > 65 && b < 100) layerPixels.monthly++;
        if (r < 40 && g > 200 && b < 40) layerPixels.gap++;
      }
      assert(Object.values(layerPixels).every(n => n > 50), `D/W/M and green gap render: ${JSON.stringify(layerPixels)}`);
      assert((await page.evaluate(() => window.btcSpotChart.getHourlyFractalBoxes())).boxes.length > 0, 'Confirmed 1H boxes are projected');
      await page.locator('#btcSpotChartPanel').screenshot({ path: path.join(output, `${asset}-all-overlays-${mobile ? 'mobile' : 'desktop'}.png`) });
      for (const id of ['spotChartGapWeeklyBreaks', 'spotChartGapDailyBreaks']) await page.locator('#' + id).check();
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
      const yellowPixels = xs.map(x => yellowColumn(off, x));
      assert(yellowPixels.every(n => n > 200), `Daily markers remain independently visible: ${yellowPixels}`);
      await page.locator('#btcSpotChartPanel').screenshot({ path: path.join(output, `gap-daily-${mobile ? 'mobile' : 'desktop'}.png`) });
      const dailyDownloadEvent = page.waitForEvent('download'); await page.locator('#spotChartDownload').click();
      const dailyFile = path.join(output, `gap-daily-${mobile ? 'mobile' : 'desktop'}-export.png`);
      await (await dailyDownloadEvent).saveAs(dailyFile);
      assert(xs.every(x => yellowColumn(fs.readFileSync(dailyFile), x) > 200), 'Daily markers are included in PNG export');
      await page.locator('#spotChartGapDailyBreaks').uncheck();
      await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
      const neither = await page.locator('#spotChartCanvas').screenshot();
      assert(xs.every(x => yellowColumn(neither, x) < 20), 'Daily checkbox removes only Daily markers');
      await page.locator('#spotChartGapWeeklyBreaks').check();
      await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
      const downloadEvent = page.waitForEvent('download'); await page.locator('#spotChartDownload').click();
      const download = await downloadEvent; const file = path.join(output, `gap-weekly-${mobile ? 'mobile' : 'desktop'}-export.png`);
      await download.saveAs(file);
      assert(xs.every(x => orangeColumn(fs.readFileSync(file), x) > 200), 'PNG export includes the marker layer');
      await page.locator('#spotChartGapDailyBreaks').check();
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
      assert.equal((await page.evaluate(() => window.btcSpotChart.getGapDailyBreakouts())).signals.length, 2);
      for (const id of ['spotChartGapWeeklyBreaks', 'spotChartGapDailyBreaks']) await page.locator('#' + id).uncheck();
      await page.locator('#spotChartIntrusionDailyBreaks').check();
      await page.evaluate(time => {
        window.btcSpotChart.chart.timeScale().setVisibleRange({ from: time / 1000 - 6 * 3600, to: time / 1000 + 10 * 3600 });
      }, day);
      await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
      const purpleXs = await page.evaluate(times => times.map(time => window.btcSpotChart.chart.timeScale().timeToCoordinate(time)), intrusionState.signals.map(s => s.time));
      const purple = await page.locator('#spotChartCanvas').screenshot();
      const purplePixels = purpleXs.map(x => purpleColumn(purple, x));
      assert(purplePixels.every(n => n > 200), `Actual purple lines on the alert candles: ${purplePixels}`);
      assert.deepEqual(await page.evaluate(() => window.btcSpotChart.getIntrusionPocCrosses()), [], 'Partial volume profiles draw no guessed crossbar');
      const pendingRead = page.waitForResponse(response => response.url().includes(`/${asset}-intrusion-spot-poc`));
      now += 6000;
      await page.evaluate(({ time, day }) => {
        window.__weeklyNow = time;
        window.btcSpotChart.chart.timeScale().setVisibleRange({ from: day / 1000 + 7 * 3600, to: day / 1000 + 8 * 3600 });
      }, { time: now, day });
      await pendingRead;
      assert.deepEqual(await page.evaluate(() => window.btcSpotChart.getIntrusionPocCrosses()), [], 'Offscreen partial profiles stay invisible while still loading');
      pocReady = true;
      for (let pass = 0; pass < 2; pass++) {
        now += 6000; await page.evaluate(time => { window.__weeklyNow = time; }, now);
        await page.evaluate(time => {
          window.btcSpotChart.chart.timeScale().setVisibleRange({ from: time / 1000 - 6 * 3600, to: time / 1000 + 10 * 3600 });
          window.btcSpotChart.candleSeries.priceScale().setVisibleRange({ from: 60, to: 160 });
        }, day);
        try { await page.waitForFunction(minimum => window.btcSpotChart.getIntrusionPocCrosses().length >= minimum, pass + 1); }
        catch (error) {
          console.error('POC fixture state', JSON.stringify(await page.evaluate(() => ({
            records: window.btcSpotChart.getIntrusionSpotPoc(), crosses: window.btcSpotChart.getIntrusionPocCrosses(),
            range: window.btcSpotChart.chart.timeScale().getVisibleRange(),
            status: document.getElementById('spotChartIntrusionDailyStatus').textContent
          }))), { pocReads, errors });
          throw error;
        }
      }
      const pocCrosses = await page.evaluate(() => window.btcSpotChart.getIntrusionPocCrosses());
      assert.deepEqual(pocCrosses.map(cross => cross.price), [119.25, 88.75]);
      for (const cross of pocCrosses) {
        const expected = await page.evaluate(price => window.btcSpotChart.candleSeries.priceToCoordinate(price), cross.price);
        assert(Math.abs(cross.y - expected) <= 1, 'Horizontal marker uses actual POC price coordinate');
      }
      const crossShot = await page.locator('#spotChartCanvas').screenshot();
      fs.writeFileSync(path.join(output, `poc-crosses-${mobile ? 'mobile' : 'desktop'}.png`), crossShot);
      assert(pocCrosses.every(cross => pocPixels(crossShot, cross) >= 8), `Both actual horizontal crossbars must render: ${JSON.stringify(pocCrosses.map(cross => ({ ...cross, pixels: pocPixels(crossShot, cross) })))}`);
      let deltaLabels = [];
      {
        await page.waitForFunction(() => window.btcSpotChart.getIntrusionDeltaLabels().filter(label => label.status === 'ok').length === 2);
        deltaLabels = await page.evaluate(() => window.btcSpotChart.getIntrusionDeltaLabels());
        assert.deepEqual(deltaLabels.map(label => label.text), eth ? ['F5m +30.00%', 'F5m +20.00%'] : ['F5m +25.86%', 'F5m +30.44%']);
        for (const label of deltaLabels) assert(label.x >= 0 && label.y >= 0, 'Labels remain within the pane');
        const shot = PNG.sync.read(await page.locator('#spotChartCanvas').screenshot());
        for (const label of deltaLabels) {
          let pixels = 0;
          for (let y = Math.ceil(label.y); y < Math.floor(label.y + label.height); y++) for (let x = Math.ceil(label.x); x < Math.floor(label.x + label.width); x++) {
            const at = (y * shot.width + x) * 4;
            if (shot.data[at] > 175 && shot.data[at + 1] > 125 && shot.data[at + 2] > 185) pixels++;
          }
          assert(pixels > 12, 'The actual F5m text must render, not just expose metadata');
        }
      }
      const purpleDownloadEvent = page.waitForEvent('download'); await page.locator('#spotChartDownload').click();
      const purpleFile = path.join(output, `intrusion-daily-${mobile ? 'mobile' : 'desktop'}-export.png`);
      await (await purpleDownloadEvent).saveAs(purpleFile);
      assert(purpleXs.every(x => purpleColumn(fs.readFileSync(purpleFile), x) > 200), 'PNG includes purple markers');
      assert(pocCrosses.every(cross => pocPixels(fs.readFileSync(purpleFile), cross) >= 8), 'PNG includes horizontal POC crossbars');
      await page.locator('#btcSpotChartPanel').screenshot({ path: path.join(output, `intrusion-daily-${mobile ? 'mobile' : 'desktop'}.png`) });
      intrusionFailure = true;
      if (eth) await page.evaluate(() => window.__weeklyFixtureRefresh().catch(() => {}));
      else await page.locator('#spotChartRefresh').click();
      await page.waitForFunction(() => document.getElementById('spotChartIntrusionDailyStatus').textContent.includes('stale'));
      assert.equal((await page.evaluate(() => window.btcSpotChart.getIntrusionDailyBreakouts())).signals.length, 2, 'Read failure preserves saved evidence');
      await page.locator('#spotChartIntrusionDailyBreaks').uncheck();
      const readsWhenOff = intrusionReads;
      const deltaReadsWhenOff = deltaReads;
      const pocReadsWhenOff = pocReads;
      await page.locator('#spotChartRefresh').click();
      await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
      assert.equal(intrusionReads, readsWhenOff, 'Disabled intrusion overlay performs no additional dashboard reads');
      assert.equal(deltaReads, deltaReadsWhenOff, 'Disabled overlay stops annotation reads');
      assert.equal(pocReads, pocReadsWhenOff, 'Disabled overlay stops POC reads');
      assert.deepEqual(await page.evaluate(() => window.btcSpotChart.getIntrusionPocCrosses()), []);
      assert.deepEqual(await page.evaluate(() => window.btcSpotChart.getIntrusionDeltaLabels()), []);
      const noPurple = await page.locator('#spotChartCanvas').screenshot();
      assert(purpleXs.every(x => purpleColumn(noPurple, x) < 20), 'Checkbox removes the actual purple lines');
      assert.deepEqual(errors, []); assert.deepEqual(writes, []);
      assert.deepEqual(wrongMarketReads, []);
      results.push({ mobile, symbol, layerPixels, wrongMarketReads, automaticClosedHourUpdate: true, longAndShort: true, previousBoxUsed: true,
        independentCheckboxes: true, orangePixels: pixels, yellowPixels, weeklyWinsOverlap: true,
        pngIncludesBothOverlays: true, purplePixels, intrusionAlertTimeNotMailTime: true,
        readFailureRetainsIntrusions: true, disabledIntrusionStopsReads: true, deltaReads, deltaLabels, pocReads, pocCrosses,
        noHorizontalOverflow: true, writes });
      await context.close();
    }
    console.log(JSON.stringify(results, null, 2));
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
