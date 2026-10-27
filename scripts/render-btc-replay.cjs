const { chromium } = require('playwright');
const { PNG } = require('pngjs');
const fs = require('node:fs');
const path = require('node:path');
const { pathToFileURL } = require('node:url');
const assert = require('node:assert/strict');
const directory = process.argv[2];
if (!directory) throw new Error('Supply replay output directory');
(async () => {
  const browser = await chromium.launch({ headless: true, channel: 'msedge' });
  const results = [];
  try {
    for (const mobile of [false, true]) {
      const page = await browser.newPage({ viewport: mobile ? { width: 390, height: 844 } : { width: 2000, height: 1100 }, deviceScaleFactor: mobile ? 1 : 2 });
      const errors = [], requests = [];
      page.on('pageerror', error => errors.push(error.message));
      await page.route(/^https?:/, route => { requests.push(route.request().url()); return route.abort(); });
      await page.goto(pathToFileURL(path.join(directory, 'rapport.html')).href);
      await page.evaluate(() => document.fonts.ready);
      const overflow = await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1);
      assert(!overflow, 'Report must fit viewport');
      const charts = await page.locator('svg').count();
      assert(charts >= 2, 'Market and equity charts required');
      if (!mobile) {
        await page.screenshot({ path: path.join(directory, 'btc-replay-overzicht.png'), fullPage: true });
        await page.locator('#independent details').evaluate(node => { node.open = true; });
        await page.locator('#independent').screenshot({ path: path.join(directory, 'btc-replay-alle-signalen.png') });
        await page.locator('#independent details').evaluate(node => { node.open = false; });
        if (await page.locator('#trade-overview .trade').count()) {
          await page.locator('#trade-overview').screenshot({ path: path.join(directory, 'btc-replay-trades.png') });
          const buffer = await page.locator('#trade-overview .trade').first().screenshot();
          const png = PNG.sync.read(buffer);
          let colored = 0;
          for (let i = 0; i < png.data.length; i += 4) if (png.data[i + 1] > 85 && Math.abs(png.data[i] - png.data[i + 1]) > 30) colored++;
          assert(colored > 200, 'Trade chart must contain rendered candles and stops');
        }
      } else await page.screenshot({ path: path.join(directory, 'btc-replay-mobile.png'), fullPage: false });
      assert.deepEqual(errors, []); assert.deepEqual(requests, []);
      results.push({ mobile, charts, overflow, externalRequests: requests.length, errors });
      await page.close();
    }
  } finally { await browser.close(); }
  fs.writeFileSync(path.join(directory, 'render-qa.json'), JSON.stringify(results, null, 2));
  console.log(JSON.stringify(results, null, 2));
})().catch(error => { console.error(error); process.exitCode = 1; });
