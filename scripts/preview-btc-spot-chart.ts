import express from 'express';
import path from 'path';
import helmet from 'helmet';
import { btcSpotChartRouter } from '../src/controllers/btcSpotChart';
import { binanceMonthlyFractalsRouter } from '../src/controllers/binanceMonthlyFractals';
import { BtcGapProjectionRecovery } from '../src/services/btcGapProjectionRecovery';

// Deliberately do not import src/index or controllers/index: this preview has
// no exchange initialization, trading executors, plans, collectors or SMTP.
const app = express();
app.use(helmet({ contentSecurityPolicy: false }));
app.use(btcSpotChartRouter);
app.use(binanceMonthlyFractalsRouter);
const recovery = new BtcGapProjectionRecovery(() => path.join(process.cwd(), 'data', 'preview-gap-archive'));
app.get('/open-liquidity/v2/gap-projection-history', async (_req, res) => {
  try { res.json(await recovery.read()); }
  catch (error) { res.status(503).json({ error: String(error) }); }
});
app.get('/decentrader/map', (_req, res) => {
  res.setHeader('Content-Security-Policy', [
    "default-src 'self'", "script-src 'self' 'unsafe-inline'", "script-src-attr 'none'",
    "style-src 'self' 'unsafe-inline'", "img-src 'self' data:",
    "connect-src 'self' https://arons-tradingview-alert-connector.onrender.com wss://stream.binance.com:9443 wss://stream.binance.com:443",
    "font-src 'self' https: data:", "object-src 'none'", "frame-ancestors 'self'"
  ].join('; '));
  res.sendFile(path.join(process.cwd(), 'public', 'decentrader_liquidity_timelapse.html'));
});
app.use('/decentrader/data', express.static(path.join(process.cwd(), 'public', 'data'), { index: false }));
const port = Number(process.env.SPOT_CHART_PREVIEW_PORT || 4318);
app.listen(port, '127.0.0.1', () => console.log(`Read-only chart preview: http://127.0.0.1:${port}/decentrader/map?source=open-v2#btcSpotChartPanel`));
