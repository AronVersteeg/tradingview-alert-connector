import fs from 'fs';
import path from 'path';

import express, { Router } from 'express';
import { validateAlert } from '../services';
import { DexRegistry } from '../services/dexRegistry';
import { decentraderGapMonitor } from '../services/decentraderGapMonitor';

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
  const expected = String(process.env.TRADINGVIEW_PASSPHRASE || '').trim();
  if (!expected) return true;

  const headerToken = req.header('X-Webhook-Token');
  const received =
    headerToken ||
    req.body?.passphrase ||
    req.body?.token ||
    req.query?.passphrase ||
    req.query?.token;

  return String(received || '').trim() === expected;
}

// ================= GLOBAL REGISTRY =================

const dexRegistry = new DexRegistry();

// ================= INITIALIZATION =================

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

  console.log("All exchanges initialized.");
}

initializeExchanges().catch((err) => {
  console.error("Exchange initialization failed:", err);
  process.exit(1);
});

// ================= ROUTER =================

const router: Router = express.Router();

// Health check
router.get('/', async (req, res) => {
  res.send('OK');
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
  console.log('Received TradingView strategy alert:', req.body);

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

decentraderGapMonitor.start();

export default router;


