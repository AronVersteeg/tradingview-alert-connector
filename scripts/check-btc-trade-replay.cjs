const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require('playwright');
const { PNG } = require('pngjs');

const base = Math.floor(Date.parse('2026-10-07T09:00:00Z') / 1000);
const capsules = Array.from({ length: 42 }, (_, i) => {
  const open = 82000 + 170 * Math.sin(i / 3) + 10 * i, close = open + (i % 2 ? 38 : -27);
  return { time: base + i * 3600, open, high: Math.max(open, close) + 20, low: Math.min(open, close) - 20, close, closed: true };
});
const hour = i => (base + i * 3600) * 1000;
const trade = { id: String(hour(13)), direction: 'long', status: 'CLOSED', signalAt: hour(12), openedAt: hour(13),
  closedAt: hour(32), entryPrice: 82050, exitPrice: 82350, exitReason: 'STOP', gapEdge: 82020, sizingStop: 81750,
  initialStop: 81850, size: .05, riskUsd: 10, grossPnl: 15,
  stops: [13, 20, 27].map((index, i) => ({ at: hour(index), price: 81850 + i * 230 })),
  yellows: [{ at: hour(15), result: 'UNCHANGED', stop: 81850 },
    { at: hour(20), result: 'TIGHTENED', stop: 82080, fractal: { at: hour(17), confirmedAt: hour(20), price: 82160 } },
    { at: hour(27), result: 'TIGHTENED', stop: 82310, fractal: { at: hour(23), confirmedAt: hour(26), price: 82385 } }] };
const open = { ...trade, id: String(hour(8)), status: 'OPEN', openedAt: hour(8), signalAt: hour(7),
  closedAt: undefined, exitPrice: undefined, grossPnl: undefined,
  markToMarket: { at: hour(42), price: 82000, grossPnl: -2.5 } };
const incomplete = { ...open, id: String(hour(3)), status: 'INCOMPLETE', markToMarket: undefined };
const trades = [incomplete, open, trade];
const purpleTrade = { ...trade, id: String(hour(12)), openedAt: hour(12), grossPnl: -7.5, dailyLevels: [82000] };
const coverage = { riskBudgetUsd: 15, tradeCount: trades.length, from: Date.parse('2025-01-01T00:00:00Z'),
  through: hour(42), historyCoverage: 'archive-plus-recent', missingDydxHours: 3 };
const pageHtml = fs.readFileSync('public/decentrader_liquidity_timelapse.html', 'utf8')
  .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, tag => /(?:hourly-fractal-boxes|trade-replay)\.js/.test(tag) ? tag : '');

async function run() {
  const browser = await chromium.launch({ headless: true, channel: 'msedge' });
  try {
    const sources = [['BTC', 'open-v2'], ['ETH', 'open-v2-eth'], ['SOL', 'open-v2-sol'], ['ZEC', 'open-v2-zec']];
    for (const [asset, source] of sources) for (const viewport of [{ width: 1280, height: 850 }, { width: 390, height: 844 }]) {
      const market = `${asset}-USD`;
      const page = await browser.newPage({ viewport });
      const errors = [];
      let listed = trades;
      let reads = 0;
      let replyMarket = market;
      page.on('pageerror', error => errors.push(error.message));
      await page.route('**/*', async route => {
        const url = new URL(route.request().url());
        if (url.host !== 'btc-replay.test') return route.abort();
        if (url.pathname === '/decentrader/map') return route.fulfill({ contentType: 'text/html', body: pageHtml });
        if (url.pathname === '/open-liquidity/v2/green-retest') {
          reads++;
          assert.equal(route.request().method(), 'GET');
          assert.equal(route.request().headers()['x-webhook-token'], undefined);
          assert.equal(url.searchParams.get('market'), market);
          const strategy = url.searchParams.get('strategy');
          assert(['green', 'purple'].includes(strategy));
          const sourceTrades = strategy === 'purple' ? [purpleTrade] : listed;
          return route.fulfill({ json: url.searchParams.has('id')
            ? { trade: sourceTrades.find(row => row.id === url.searchParams.get('id')), candles: capsules, ...coverage, market: replyMarket, strategy }
            : { trades: sourceTrades, ...coverage, market: replyMarket, strategy } });
        }
        if (url.pathname.startsWith('/assets/btc-spot-chart/')) {
          const file = path.join('public', url.pathname.replace('/assets/', ''));
          return route.fulfill({ path: file, contentType: file.endsWith('.css') ? 'text/css' : 'text/javascript' });
        }
        return route.abort();
      });
      await page.goto(`https://btc-replay.test/decentrader/map?source=${source}`);
      assert.equal(await page.locator('#btcReplayTitle').textContent(), `${asset} Green Retest`);
      await page.locator('#btcReplayLoad').click();
      await page.waitForFunction(() => document.querySelector('#btcReplayCanvas canvas') && !document.querySelector('#btcReplayLegend').hidden);
      const host = page.locator('#btcReplayCanvas');
      await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
      const screenshot = await host.screenshot();
      if (process.argv[2]) fs.writeFileSync(path.join(process.argv[2], `${asset.toLowerCase()}-trade-replay-${viewport.width}.png`), screenshot);
      const image = PNG.sync.read(screenshot);
      let green = 0, red = 0, blue = 0, yellow = 0;
      for (let i = 0; i < image.data.length; i += 4) {
        const [r, g, b] = image.data.subarray(i, i + 3);
        if (r < 75 && g > 145 && b > 125) green++;
        if (r > 180 && g < 125 && b > 85 && b < 140) red++;
        if (r > 70 && r < 140 && g > 140 && g < 200 && b > 225) blue++;
        if (r > 145 && g > 130 && b < 115) yellow++;
      }
      assert(green > 40 && red > 40 && blue > 40 && yellow > 15, `Candles, SL and Yellow markers not visible: ${green}/${red}/${blue}/${yellow}; ${await page.locator('#btcReplayStatus').textContent()}; ${errors.join('; ')}`);
      assert.equal(await page.locator('#btcReplayEvents li:visible').count(), 5);
      assert.match(await page.locator('#btcReplaySummary').textContent(), /Geschatte bruto P&L.*Sizing-SL.*Initieel prijsrisico/);
      assert.match(await page.locator('#btcReplayStatus').textContent(), /archief \+ recent.*3 gesimuleerde trades.*3 dYdX-uren ontbreken/);
      assert.match(await page.locator('#btcReplayTrade option:checked').textContent(), /\$\s?\+15,00.*gesloten/);
      assert.match(await page.locator('#btcReplayTrade option').nth(1).textContent(), /\$\s?[-\u2212]2,50.*open/u);
      const total = page.locator('#btcReplayTotalPnl');
      assert.match(await total.textContent(), /\$\s?\+12,50/);
      assert.equal(await total.getAttribute('data-result'), 'gain');
      assert.equal(await total.evaluate(node => getComputedStyle(node).borderColor), 'rgb(51, 147, 110)');
      const selectBox = await page.locator('#btcReplayTrade').boundingBox(), totalBox = await total.boundingBox();
      assert(totalBox.x >= selectBox.x + selectBox.width && Math.abs(totalBox.y - selectBox.y) < 2);
      assert(await page.locator('#btcTradeReplay').evaluate(panel => panel.scrollWidth <= panel.clientWidth + 1));
      const previous = await page.locator('#btcReplayTime').textContent();
      await page.locator('#btcReplayCursor').fill('10');
      assert.equal(await page.locator('#btcReplayEvents li:visible').count(), 0);
      assert.notEqual(await page.locator('#btcReplayTime').textContent(), previous);
      const risk = page.locator('#btcReplayTestRisk');
      assert.equal(await page.locator('#btcReplayBaseRisk').textContent(), '$15');
      const originalReads = reads;
      const selectedTrade = await page.locator('#btcReplayTrade').inputValue();
      await risk.fill('60');
      assert.match(await total.textContent(), /\$\s?\+50,00/);
      assert.match(await page.locator('#btcReplayTrade option:checked').textContent(), /\$\s?\+60,00/);
      const scaledSummary = await page.locator('#btcReplaySummary').textContent();
      assert.match(scaledSummary, /\$\s?\+60,00/);
      assert(scaledSummary.includes(`Positiegrootte (model)0,2 ${asset}Initieel prijsrisico$40`));
      assert.match(scaledSummary, /Initi.le SL\$81\.850/);
      assert.match(await page.locator('#btcReplayStatus').textContent(), /\$60 modelbudget/);
      assert.equal(await page.locator('#btcReplayTrade').inputValue(), selectedTrade);
      assert.equal(await page.locator('#btcReplayCursor').inputValue(), '10');
      assert.equal(await page.locator('#btcReplayEvents li:visible').count(), 0);
      assert.equal(reads, originalReads);
      await risk.fill('7,50');
      assert.match(await total.textContent(), /\$\s?\+6,25/);
      await risk.fill('0');
      assert.equal(await risk.getAttribute('aria-invalid'), 'true');
      assert.match(await total.textContent(), /\$\s?\+12,50/);
      await risk.fill('');
      assert.equal(await risk.getAttribute('aria-invalid'), 'false');
      assert.match(await page.locator('#btcReplayStatus').textContent(), /\$15 modelbudget/);
      assert(await page.locator('#btcTradeReplay').evaluate(panel => panel.scrollWidth <= panel.clientWidth + 1));
      await page.locator('#btcReplayPlay').click();
      await page.waitForFunction(() => Number(document.querySelector('#btcReplayCursor').value) > 10);
      await page.locator('#btcReplayPlay').click();
      await page.locator('#btcReplayTrade').selectOption(open.id);
      await page.locator('#btcReplaySummary').getByText('Ongerealiseerde bruto P&L').waitFor();
      assert.match(await page.locator('#btcReplaySummary .btc-replay-loss').textContent(), /\$\s?[-\u2212]2,50.*dYdX-close/u);
      await risk.fill('30');
      assert.match(await page.locator('#btcReplaySummary .btc-replay-loss').textContent(), /\$\s?[-\u2212]5,00.*dYdX-close/u);
      await risk.fill('');
      await page.locator('#btcReplayTrade').selectOption(incomplete.id);
      await page.locator('#btcReplaySummary .btc-replay-pnl').getByText('Onbekend').waitFor();
      assert.match(await total.textContent(), /\$\s?\+12,50/);
      listed = [incomplete, open];
      await page.locator('#btcReplayLoad').click();
      await page.locator('#btcReplayTotalPnl[data-result="loss"]').waitFor();
      assert.match(await total.textContent(), /\$\s?[-\u2212]2,50/u);
      assert.equal(await total.evaluate(node => getComputedStyle(node).borderColor), 'rgb(170, 76, 93)');
      listed = [open, { ...trade, grossPnl: 2.5 }];
      await page.locator('#btcReplayLoad').click();
      await page.locator('#btcReplayTotalPnl[data-result="flat"]').waitFor();
      assert.match(await total.textContent(), /\$\s?0,00/);
      await risk.fill('30');
      await page.locator('#btcReplayStrategy label').filter({ hasText: 'Purple' }).click();
      await page.locator('#btcReplaySummary').getByText('Gebroken Daily (Spot)').waitFor();
      assert.equal(await page.locator('#btcReplayTitle').textContent(), `${asset} Purple Retest`);
      assert.equal(await page.locator('#btcReplayTrade').inputValue(), purpleTrade.id);
      assert.equal(await page.locator('#btcReplayTrade option').count(), 1);
      assert.match(await total.textContent(), /\$\s?[-\u2212]15,00/u);
      assert.equal(await risk.inputValue(), '30');
      assert.equal(await page.locator('.btc-replay-entry').textContent(), 'Purple-entry');
      assert(await page.locator('#btcTradeReplay').evaluate(panel => panel.scrollWidth <= panel.clientWidth + 1));
      await page.locator('#btcReplayStrategy label').filter({ hasText: 'Green' }).click();
      await page.locator('#btcReplayTotalPnl[data-result="flat"]').waitFor();
      assert.equal(await page.locator('#btcReplayTrade option').count(), 2);
      assert.equal(await page.locator('#btcReplayTitle').textContent(), `${asset} Green Retest`);
      if (asset !== 'BTC') {
        replyMarket = 'BTC-USD';
        await page.locator('#btcReplayLoad').click();
        await page.locator('#btcReplayStatus').getByText(`Onverwachte markt of strategie in de ${asset} Green-retest.`).waitFor();
        assert(await page.locator('#btcReplayTradePicker').isHidden());
      }
      assert.deepEqual(errors, []);
      console.log(`${asset} Green retest ${viewport.width}px: chart, P&L, risk, market identity and scrub/play passed.`);
      await page.goto('https://btc-replay.test/decentrader/map?source=open-v2-inj');
      assert(await page.locator('#btcTradeReplay').isHidden());
      await page.close();
    }
  } finally { await browser.close(); }
}
run().catch(error => { console.error(error); process.exitCode = 1; });
