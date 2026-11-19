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
const trade = { id: 'a'.repeat(24), direction: 'long', status: 'CLOSED', openedAt: (base + 13 * 3600 + 10) * 1000,
  closedAt: (base + 32 * 3600 + 5) * 1000, entryPrice: 82050, exitPrice: 82350 };
const evidence = [14, 20, 27].map((index, i) => ({ at: (base + index * 3600 + 1) * 1000, price: 81850 + i * 140 }));
const pageHtml = fs.readFileSync('public/decentrader_liquidity_timelapse.html', 'utf8')
  .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, tag => /(?:hourly-fractal-boxes|trade-replay)\.js/.test(tag) ? tag : '');

async function run() {
  const browser = await chromium.launch({ headless: true, channel: 'msedge' });
  try {
    for (const viewport of [{ width: 1280, height: 850 }, { width: 390, height: 844 }]) {
      const page = await browser.newPage({ viewport });
      const errors = [];
      page.on('pageerror', error => errors.push(error.message));
      await page.route('**/*', async route => {
        const url = new URL(route.request().url());
        if (url.host !== 'btc-replay.test') return route.abort();
        if (url.pathname === '/decentrader/map') return route.fulfill({ contentType: 'text/html', body: pageHtml });
        if (url.pathname === '/open-liquidity/v2/btc-trade-replay') {
          assert.equal(route.request().method(), 'GET');
          assert.equal(route.request().headers()['x-webhook-token'], 'fixture-token');
          return route.fulfill({ json: url.searchParams.has('id') ? { trade, candles: capsules, stops: evidence,
            range: { from: base * 1000, through: (base + capsules.length * 3600) * 1000 } } : { trades: [trade] } });
        }
        if (url.pathname.startsWith('/assets/btc-spot-chart/')) {
          const file = path.join('public', url.pathname.replace('/assets/', ''));
          return route.fulfill({ path: file, contentType: file.endsWith('.css') ? 'text/css' : 'text/javascript' });
        }
        return route.abort();
      });
      await page.goto('https://btc-replay.test/decentrader/map?source=open-v2');
      await page.locator('#btcReplayLoad').click();
      await page.locator('#btcReplayToken').fill('fixture-token');
      await page.locator('#btcReplayLoad').click();
      await page.waitForFunction(() => document.querySelector('#btcReplayCanvas canvas') && !document.querySelector('#btcReplayLegend').hidden);
      const host = page.locator('#btcReplayCanvas');
      await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
      const screenshot = await host.screenshot();
      if (process.argv[2]) fs.writeFileSync(path.join(process.argv[2], `btc-trade-replay-${viewport.width}.png`), screenshot);
      const image = PNG.sync.read(screenshot);
      let green = 0, red = 0, amber = 0;
      for (let i = 0; i < image.data.length; i += 4) {
        const [r, g, b] = image.data.subarray(i, i + 3);
        if (r < 75 && g > 145 && b > 125) green++;
        if (r > 180 && g < 125 && b > 85 && b < 140) red++;
        if (r > 225 && g > 150 && g < 218 && b < 140) amber++;
      }
      assert(green > 40 && red > 40 && amber > 15, `Candles and stop path not visible: ${green}/${red}/${amber}; ${await page.locator('#btcReplayStatus').textContent()}; ${errors.join('; ')}`);
      assert(await page.locator('#btcTradeReplay').evaluate(panel => panel.scrollWidth <= panel.clientWidth + 1));
      const previous = await page.locator('#btcReplayTime').textContent();
      await page.locator('#btcReplayCursor').fill('10');
      assert.notEqual(await page.locator('#btcReplayTime').textContent(), previous);
      await page.locator('#btcReplayPlay').click();
      await page.waitForFunction(() => Number(document.querySelector('#btcReplayCursor').value) > 10);
      await page.locator('#btcReplayPlay').click();
      assert.deepEqual(errors, []);
      console.log(`BTC trade replay ${viewport.width}px: ${green} up, ${red} down, ${amber} stop pixels; scrub/play passed.`);
      await page.goto('https://btc-replay.test/decentrader/map?source=open-v2-eth');
      assert(await page.locator('#btcTradeReplay').isHidden());
      await page.close();
    }
  } finally { await browser.close(); }
}
run().catch(error => { console.error(error); process.exitCode = 1; });
