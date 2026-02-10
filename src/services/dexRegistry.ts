import { AbstractDexClient } from './abstractDexClient';
import { DydxV4Client } from './dydx_v4/dydxV4Client';

export class DexRegistry {
	getDex(dexKey: string): AbstractDexClient {
		if (dexKey === 'dydxv4') {
			return new DydxV4Client();
		}

		throw new Error(`Exchange ${dexKey} is not supported`);
	}

	getAllDexKeys(): string[] {
		return ['dydxv4'];
	}
}

