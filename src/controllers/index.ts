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



const router: Router = express.Router();

router.get('/', async (req, res) => {
	res.send('OK');
});

router.get('/accounts', async (req, res) => {
	console.log('Received GET request.');

	const dexRegistry = new DexRegistry();
	const dexNames = ['dydxv3', 'dydxv4', 'perpetual', 'gmx', 'bluefin'];
	const dexClients = dexNames.map((name) => dexRegistry.getDex(name));

	try {
		const accountStatuses = await Promise.all(
			dexClients.map((client) => client.getIsAccountReady())
		);

		const message = {
			dYdX_v3: accountStatuses[0], // dydxv3
			dYdX_v4: accountStatuses[1], // dydxv4
			PerpetualProtocol: accountStatuses[2], // perpetual
			GMX: accountStatuses[3], // gmx
			Bluefin: accountStatuses[4] // bluefin
		};
		res.send(message);
	} catch (error) {
		console.error('Failed to get account readiness:', error);
		res.status(500).send('Internal server error');
	}
});

router.post('/', async (req, res) => {
	console.log('Recieved Tradingview strategy alert:', req.body);

	  // ---------- IDEMPOTENCY CHECK ----------
  const store = loadStore();
  const hash = alertHash(req.body);

  if (store[hash]) {
    console.log('⏭ Duplicate TradingView alert genegeerd');
    return res.send('duplicate');
  }

  // Markeer meteen als verwerkt (beschermt tegen race conditions)
  store[hash] = true;
  saveStore(store);


	const validated = await validateAlert(req.body);
	if (!validated) {
		res.send('Error. alert message is not valid');
		return;
	}
		
	// set dydxv3 by default for backwards compatibility
	const exchange = req.body['exchange']?.toLowerCase() || 'dydxv3';

	const dexClient = new DexRegistry().getDex(exchange);

	if (!dexClient) {
		res.send(`Error. Exchange: ${exchange} is not supported`);
		return;
	}

	// TODO: add check if dex client isReady 

	try {
		const result = await dexClient.placeOrder(req.body);

		res.send('OK');
		// checkAfterPosition(req.body);
	} catch (e) {
		res.send('error');
	}
});

router.get('/debug-sentry', function mainHandler(req, res) {
	throw new Error('My first Sentry error!');
});

export default router;
