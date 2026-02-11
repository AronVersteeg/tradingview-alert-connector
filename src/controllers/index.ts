import crypto from 'crypto';
import fs from 'fs';
import path from 'path';

import express, { Router } from 'express';
import { validateAlert } from '../services';
import { DexRegistry } from '../services/dexRegistry';

const STORE_PATH = path.join(process.cwd(), 'data', 'executed-alerts.json');

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

function alertHash(body: any): string {
  return `${body.strategy}_${body.market}_${body.time}`;
}

// ================= GLOBAL REGISTRY =================

const dexRegistry = new DexRegistry();

// ================= INITIALIZATION =================

async function initializeExchanges() {
  console.log("Initializing exchanges...");

  const exchanges = ['dydxv4']; // momenteel alleen v4 actief

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

// Initialize on server boot
initializeExchanges().catch((err) => {
  console.error("Exchange initialization failed:", err);
  process.exit(1);
});

// ================= ROUTER =================

const router: Router = express.Router();

router.get('/', async (req, res) => {
  res.send('OK');
});

// ================= SAFE ACCOUNTS ROUTE =================

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

  // ---------- IDEMPOTENCY ----------
  const store = loadStore();
  const hash = alertHash(req.body);

  if (store[hash]) {
    console.log('⏭ Duplicate TradingView alert genegeerd');
    return res.send('duplicate');
  }

  store[hash] = true;
  saveStore(store);

  const validated = await validateAlert(req.body);
  if (!validated) {
    return res.send('Error. alert message is not valid');
  }

  const exchange = req.body['exchange']?.toLowerCase() || 'dydxv4';

  const dexClient = dexRegistry.getDex(exchange);

  if (!dexClient) {
    return res.send(`Error. Exchange: ${exchange} is not supported`);
  }

  try {
    await dexClient.placeOrder(req.body);
    res.send('OK');
  } catch (e) {
    console.error('Order placement failed:', e);
    res.status(500).send('error');
  }
});

// ================= DEBUG =================

router.get('/debug-sentry', function mainHandler(req, res) {
  throw new Error('My first Sentry error!');
});

export default router;

