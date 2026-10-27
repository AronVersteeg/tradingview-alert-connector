import express from 'express';
import path from 'path';
import { binanceSpotChartFeed, ethSpotChartFeed } from '../services/binanceSpotChart';
import { binanceCooldownHttpResponse } from '../services/binanceHttp';
import { btcFractalProjectionFeed, ethFractalProjectionFeed } from '../services/btcFractalProjections';
import { btcIntrusionFuturesDeltaFeed, ethIntrusionFuturesDeltaFeed, validateDeltaHours } from '../services/intrusionFuturesDelta';

export const btcSpotChartRouter = express.Router();

btcSpotChartRouter.use('/assets/btc-spot-chart', express.static(
  path.join(process.cwd(), 'public', 'btc-spot-chart'), { index: false, maxAge: 0 }
));

for (const [asset, feed] of [['btc', btcIntrusionFuturesDeltaFeed], ['eth', ethIntrusionFuturesDeltaFeed]] as const) {
  btcSpotChartRouter.get(`/open-liquidity/v2/${asset}-intrusion-futures-delta`, async (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    const query = req.query.hours;
    let hours: number[];
    try {
      if (typeof query !== 'string' || query.length > 100 || !/^\d+(,\d+){0,7}$/.test(query)) throw new Error('Invalid hours.');
      hours = validateDeltaHours(query.split(',').map(Number));
    } catch {
      res.status(400).send({ ok: false, error: 'Supply 1-8 closed candle-open hours as Unix seconds.' });
      return;
    }
    try { res.send(await feed.snapshot(hours)); }
    catch { res.status(503).send({ ok: false, error: 'Futures delta annotations unavailable.' }); }
  });
}

for (const [asset, candles, fractals] of [
  ['btc', binanceSpotChartFeed, btcFractalProjectionFeed],
  ['eth', ethSpotChartFeed, ethFractalProjectionFeed]
] as const) {
  btcSpotChartRouter.get(`/open-liquidity/v2/${asset}-spot-candles`, async (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('Access-Control-Allow-Origin', '*');
    const before = req.query.before === undefined ? undefined : Number(req.query.before);
    if (before !== undefined && (!Number.isSafeInteger(before) || before <= 0 || before > Date.now())) {
      res.status(400).send({ ok: false, error: 'before must be a past Unix timestamp in milliseconds.' });
      return;
    }
    try {
      res.send(await candles.page(before));
    } catch (error) {
      const cooldown = binanceCooldownHttpResponse(error);
      if (cooldown) {
        res.setHeader('Retry-After', cooldown.retryAfter);
        res.status(cooldown.status).send(cooldown.body);
        return;
      }
      res.status(502).send({ ok: false, error: error instanceof Error ? error.message : String(error) });
    }
  });

  btcSpotChartRouter.get(`/open-liquidity/v2/${asset}-spot-fractal-projections`, async (_req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('Access-Control-Allow-Origin', '*');
    try {
      res.send(await fractals.snapshot());
    } catch (error) {
      const cooldown = binanceCooldownHttpResponse(error);
      if (cooldown) res.setHeader('Retry-After', cooldown.retryAfter);
      res.status(503).send(cooldown?.body || {
        ok: false, error: error instanceof Error ? error.message : String(error)
      });
    }
  });
}
