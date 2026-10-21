import express from 'express';
import { binanceMonthlyFractalHistory, isDailyFractalMarket } from '../services/binanceDailyFractalHistory';
import { binanceCooldownHttpResponse } from '../services/binanceHttp';

export const binanceMonthlyFractalsRouter = express.Router();

binanceMonthlyFractalsRouter.get('/research/binance-monthly-fractals', async (req, res) => {
  const market = String(req.query.market || 'BTC-USD').replace(/_/g, '-').toUpperCase();
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (!isDailyFractalMarket(market)) {
    res.status(400).send({ ok: false, error: 'Monthly fractal history supports BTC-USD, ETH-USD, INJ-USD, SOL-USD, ZEC-USD, PAXG-USD and XAG-USD.' });
    return;
  }
  try {
    res.send(await binanceMonthlyFractalHistory(market, req.query.refresh === '1'));
  } catch (error) {
    const cooldown = binanceCooldownHttpResponse(error);
    if (cooldown) {
      res.setHeader('Retry-After', cooldown.retryAfter);
      res.status(cooldown.status).send(cooldown.body);
      return;
    }
    console.error('Binance monthly fractal history request failed:', error);
    res.status(502).send({ ok: false, error: error instanceof Error ? error.message : String(error) });
  }
});
