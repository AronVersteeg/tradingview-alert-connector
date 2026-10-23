import express from 'express';
import zlib from 'zlib';
import { GapProjectionHistory } from '../services/decentraderGapProjectionHistory';

export function createBtcGapProjectionHistoryRouter(readHistory: () => GapProjectionHistory,
  readRecovery?: () => Promise<GapProjectionHistory>) {
  const router = express.Router();
  router.get('/decentrader/gap-projection-history', async (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Vary', 'Accept-Encoding');
    if (req.query.market !== undefined && req.query.market !== 'BTC-USD') {
      res.status(400).json({ ok: false, error: 'Only BTC-USD gap projections are supported.' });
      return;
    }
    try {
      const history = readHistory();
      if (history.historicalAccuracy !== 'observed-snapshots') {
        res.status(409).json({ ok: false, code: 'HISTORICAL_STATE_UNVERIFIED',
          error: 'Current Decentrader active flags do not establish historical gap edges. Historical boxes withheld.' });
        return;
      }
      const json = JSON.stringify(history);
      if (/\bgzip\b/.test(String(req.headers['accept-encoding'] || ''))) {
        const compressed = await new Promise<Buffer>((resolve, reject) => {
          zlib.gzip(json, (error, result) => error ? reject(error) : resolve(result));
        });
        res.setHeader('Content-Encoding', 'gzip');
        res.type('json').send(compressed);
      } else res.type('json').send(json);
    } catch (error) {
      res.status(503).json({ ok: false, error: error instanceof Error ? error.message : String(error) });
    }
  });
  if (readRecovery) router.get('/open-liquidity/v2/gap-projection-history', async (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('Access-Control-Allow-Origin', '*');
    if (req.query.market !== undefined && req.query.market !== 'BTC-USD') {
      res.status(400).json({ ok: false, error: 'Only BTC-USD gap projections are supported.' });
      return;
    }
    try {
      res.json(await readRecovery());
    } catch (error) {
      res.status(503).json({ ok: false, error: error instanceof Error ? error.message : String(error) });
    }
  });
  return router;
}
