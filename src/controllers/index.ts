import fs from 'fs';
import path from 'path';

import express, { Router } from 'express';
import { validateAlert } from '../services';
import { DexRegistry } from '../services/dexRegistry';
import { decentraderGapMonitor } from '../services/decentraderGapMonitor';
import { getOpenLiquidityTimelapsePayload } from '../services/openLiquidityTimelapse';
import { buildSnoekScout } from '../services/snoekScout';
import { getSnoekStructures } from '../services/snoekStructures';
import { getSnoekWeather } from '../services/snoekWeather';

const STORE_PATH = path.join(process.cwd(), 'data', 'executed-alerts.json');

// ================= STORE HELPERS =================

function loadStore(): Record<string, boolean> {
  try {
    return JSON.parse(fs.readFileSync(STORE_PATH, 'utf8'));
  } catch {
    return {};
  }
}

function saveStore(store: Record<string, boolean>) {
  fs.mkdirSync(path.dirname(STORE_PATH), { recursive: true });
  fs.writeFileSync(STORE_PATH, JSON.stringify(store, null, 2));
}

// Only hash VALID alerts
function alertHash(body: any): string {
  return `${body.strategy}_${body.market}_${body.time}`;
}

function isMonitorRequestAuthorized(req: express.Request): boolean {
  const expected = String(
    process.env.DECENTRADER_API_TOKEN ||
    process.env.TRADINGVIEW_PASSPHRASE ||
    ''
  ).trim();
  if (!expected) return false;

  const headerToken = req.header('X-Webhook-Token');
  const received =
    headerToken ||
    req.body?.passphrase ||
    req.body?.token ||
    req.query?.passphrase ||
    req.query?.token;

  return String(received || '').trim() === expected;
}

function isDecentraderLiveTestAuthorized(req: express.Request): boolean {
  const expected = String(process.env.DECENTRADER_LIVE_TEST_TOKEN || '').trim();
  if (!expected) return false;

  const received =
    req.header('X-Decentrader-Live-Test-Token') ||
    req.body?.liveTestToken ||
    req.body?.token;

  return String(received || '').trim() === expected;
}

// ================= GLOBAL REGISTRY =================

const dexRegistry = new DexRegistry();

// ================= INITIALIZATION =================

function configureDecentraderTradeExecutor() {
  const client = dexRegistry.getDex('dydxv4') as any;

  if (
    !client ||
    typeof client.getAccountSnapshot !== 'function' ||
    typeof client.placeOrder !== 'function'
  ) {
    console.warn('Decentrader auto-trade executor not configured: dYdX v4 client is unavailable.');
    return;
  }

  decentraderGapMonitor.configureTradeExecutor({
    getAccountSnapshot: (markets: string[]) => client.getAccountSnapshot(markets),
    placeOrder: (alert: any) => client.placeOrder(alert),
    syncTakeProfits: (alert: any) => client.syncTakeProfits(alert),
    syncTrailingStop: (alert: any) => client.syncTrailingStop(alert)
  });
}

async function initializeExchanges() {
  console.log("Initializing exchanges...");

  saveStore({});
  console.log("Idempotency store reset.");

  const exchanges = ['dydxv4'];
 
  for (const name of exchanges) {
    const client = dexRegistry.getDex(name);

    if (!client) {
      console.warn(`Exchange ${name} not found in registry.`);
      continue;
    }

    if (typeof (client as any).init === 'function') {
      console.log(`Initializing ${name}...`);
      await (client as any).init();
      console.log(`${name} initialized.`);
    }
  }

  configureDecentraderTradeExecutor();
  console.log("All exchanges initialized.");
}

initializeExchanges()
  .then(() => {
    decentraderGapMonitor.start();
  })
  .catch((err) => {
    console.error("Exchange initialization failed:", err);
    process.exit(1);
  });

// ================= ROUTER =================

const router: Router = express.Router();

// Health check
router.get('/', async (req, res) => {
  res.send('OK');
});

// ================= SNOEK AI TOOLS =================

router.get('/snoek', async (req, res) => {
  res.setHeader(
    'Content-Security-Policy',
    [
      "default-src 'self'",
      "base-uri 'self'",
      "object-src 'none'",
      "script-src 'self' 'unsafe-inline' https://unpkg.com",
      "script-src-attr 'none'",
      "style-src 'self' 'unsafe-inline' https://unpkg.com",
      "img-src 'self' data: blob: https://service.pdok.nl",
      "connect-src 'self'",
      "font-src 'self' https: data:",
      "form-action 'self'",
      "frame-ancestors 'self'",
      'upgrade-insecure-requests'
    ].join('; ')
  );
  res.setHeader('Cache-Control', 'no-store');
  res.sendFile(path.join(process.cwd(), 'public', 'snoek', 'index.html'));
});

router.get('/snoek/api/scout', async (req, res) => {
  res.send(buildSnoekScout(req.query as any));
});

router.post('/snoek/api/scout', async (req, res) => {
  res.send(buildSnoekScout(req.body || {}));
});

router.get('/snoek/api/weather', async (req, res) => {
  try {
    const location = String(req.query.location || 'Velsen-Zuid').trim();
    res.setHeader('Cache-Control', 'public, max-age=600');
    res.send(await getSnoekWeather(location));
  } catch (error) {
    console.error('Snoek weather lookup failed:', error);
    res.status(502).send({
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

router.get('/snoek/api/structures', async (req, res) => {
  try {
    res.setHeader('Cache-Control', 'public, max-age=3600');
    res.send(await getSnoekStructures(req.query));
  } catch (error) {
    console.error('Snoek structures lookup failed:', error);
    res.status(502).send({
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

// ================= ACCOUNTS =================

router.get('/accounts', async (req, res) => {
  console.log('Received GET /accounts request.');

  try {
    const client = dexRegistry.getDex('dydxv4');

    if (!client) {
      return res.status(500).send({ dydxv4: false, error: 'Client not found' });
    }

    const ready = await client.getIsAccountReady();

    res.send({ dydxv4: ready });

  } catch (error) {
    console.error('Failed to get account readiness:', error);
    res.status(500).send('Internal server error');
  }
});

// ================= ALERT HANDLER =================

router.post('/', async (req, res) => {
  console.log('Received TradingView strategy alert:', {
    exchange: req.body?.exchange,
    strategy: req.body?.strategy,
    market: req.body?.market,
    desiredPosition: req.body?.desired_position,
    signal: req.body?.signal,
    time: req.body?.time
  });

  // ---------- VALIDATION FIRST ----------
  const validated = await validateAlert(req.body);

  if (!validated) {
    console.log('❌ Invalid alert received');
    return res.send('Error. alert message is not valid');
  }

  // ---------- IDEMPOTENCY AFTER VALIDATION ----------
  const store = loadStore();
  const hash = alertHash(req.body);

  if (store[hash]) {
    console.log('⏭ Duplicate TradingView alert genegeerd');
    return res.send('duplicate');
  }

  store[hash] = true;
  saveStore(store);

  const exchange = req.body['exchange']?.toLowerCase() || 'dydxv4';
  const dexClient = dexRegistry.getDex(exchange);

  if (!dexClient) {
    return res.send(`Error. Exchange: ${exchange} is not supported`);
  }

  res.send('OK');

  dexClient.placeOrder(req.body).catch((error) => {
    console.error('Order placement failed after webhook acknowledgement:', error);
  });
});

// ================= DECENTRADER GAP MONITOR =================

router.get('/decentrader/gap-status', async (req, res) => {
  res.send(decentraderGapMonitor.getStatus());
});

router.get('/decentrader/map', async (req, res) => {
  res.setHeader(
    'Content-Security-Policy',
    [
      "default-src 'self'",
      "base-uri 'self'",
      "object-src 'none'",
      "script-src 'self' 'unsafe-inline'",
      "script-src-attr 'none'",
      "style-src 'self' 'unsafe-inline'",
      "img-src 'self' data:",
      "connect-src 'self' https://arons-tradingview-alert-connector.onrender.com",
      "frame-src 'self'",
      "font-src 'self' https: data:",
      "form-action 'self'",
      "frame-ancestors 'self'",
      'upgrade-insecure-requests'
    ].join('; ')
  );
  res.setHeader('Cache-Control', 'no-store');
  res.sendFile(path.join(process.cwd(), 'public', 'decentrader_liquidity_timelapse.html'));
});

router.get('/decentrader/liquidity-timelapse', async (req, res) => {
  try {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.send(await decentraderGapMonitor.getTimelapsePayload());
  } catch (error) {
    console.error('Decentrader timelapse payload request failed:', error);
    res.status(500).send({
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

router.get('/open-liquidity/liquidity-timelapse', async (req, res) => {
  try {
    const market = String(req.query.market || 'BTC-USD').replace(/_/g, '-').toUpperCase();
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.send(await getOpenLiquidityTimelapsePayload(market));
  } catch (error) {
    console.error('Open liquidity timelapse payload request failed:', error);
    res.status(500).send({
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

router.get('/decentrader/trade-plan', async (req, res) => {
  try {
    const market = String(req.query.market || 'BTC-USD').replace(/_/g, '-').toUpperCase();
    const client = dexRegistry.getDex('dydxv4') as any;

    if (!client || typeof client.getAccountSnapshot !== 'function') {
      return res.status(503).send({
        ok: false,
        error: 'dYdX v4 account snapshot is unavailable.'
      });
    }

    const account = await client.getAccountSnapshot([market]);
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.send(await decentraderGapMonitor.getTradePlan(account, market));
  } catch (error) {
    console.error('Decentrader trade plan request failed:', error);
    res.status(500).send({
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

router.get('/decentrader/tp-backtest', async (req, res) => {
  try {
    const lookaheadBars = Number(req.query.lookaheadBars || 48);
    const maxTrades = Number(req.query.maxTrades || 300);

    res.setHeader('Access-Control-Allow-Origin', '*');
    res.send(await decentraderGapMonitor.getTpBacktest({
      lookaheadBars,
      maxTrades
    }));
  } catch (error) {
    console.error('Decentrader TP backtest request failed:', error);
    res.status(500).send({
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

router.post('/decentrader/simulate-edge', async (req, res) => {
  if (!isMonitorRequestAuthorized(req)) {
    return res.status(401).send({ ok: false, error: 'Unauthorized' });
  }

  const requestedEdge = String(
    req.body?.edge ||
    req.body?.direction ||
    req.query?.edge ||
    req.query?.direction ||
    ''
  ).trim().toLowerCase();
  const edge = requestedEdge === 'left' || requestedEdge === 'long'
    ? 'left'
    : requestedEdge === 'right' || requestedEdge === 'short'
      ? 'right'
      : undefined;

  if (!edge) {
    return res.status(400).send({
      ok: false,
      error: 'edge must be left/long or right/short.'
    });
  }

  try {
    res.send(await decentraderGapMonitor.simulateEdge(edge));
  } catch (error) {
    console.error('Decentrader edge simulation failed:', error);
    res.status(500).send({
      ok: false,
      dryRun: true,
      orderPlacementAttempted: false,
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

router.post('/decentrader/live-test-edge', async (req, res) => {
  if (!isDecentraderLiveTestAuthorized(req)) {
    return res.status(401).send({
      ok: false,
      error: 'Live test is disabled or DECENTRADER_LIVE_TEST_TOKEN is invalid.'
    });
  }

  if (String(req.body?.confirm || '').trim().toUpperCase() !== 'PLACE_AND_FLAT') {
    return res.status(400).send({
      ok: false,
      error: 'confirm must be PLACE_AND_FLAT.'
    });
  }

  const requestedEdge = String(req.body?.edge || '').trim().toLowerCase();
  const edge = requestedEdge === 'left' || requestedEdge === 'long'
    ? 'left'
    : requestedEdge === 'right' || requestedEdge === 'short'
      ? 'right'
      : undefined;

  if (!edge) {
    return res.status(400).send({
      ok: false,
      error: 'edge must be left/long or right/short.'
    });
  }

  const requestedHoldSeconds = Number(req.body?.holdSeconds ?? 20);
  const holdSeconds = Number.isFinite(requestedHoldSeconds)
    ? Math.max(5, Math.min(60, Math.floor(requestedHoldSeconds)))
    : 20;

  try {
    res.send(await decentraderGapMonitor.runLiveEdgeTest(edge, holdSeconds));
  } catch (error) {
    console.error('Decentrader live edge test failed:', error);
    res.status(500).send({
      ok: false,
      liveTest: true,
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

router.post('/decentrader/gap-check', async (req, res) => {
  if (!isMonitorRequestAuthorized(req)) {
    return res.status(401).send({ ok: false, error: 'Unauthorized' });
  }

  try {
    const result = await decentraderGapMonitor.checkOnce();
    res.send(result);
  } catch (error) {
    console.error('Manual Decentrader gap check failed:', error);
    res.status(500).send({
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

// ================= DEBUG =================

router.get('/debug-sentry', function mainHandler(req, res) {
  throw new Error('My first Sentry error!');
});

export default router;


