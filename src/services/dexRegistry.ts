import { AbstractDexClient } from './abstractDexClient';
import { DydxV4Client } from './dydx_v4/dydxV4Client';

export class DexRegistry {
  private registeredDexs: Map<string, AbstractDexClient>;

  constructor() {
    this.registeredDexs = new Map();
    this.registeredDexs.set('dydxv4', new DydxV4Client());
  }

  getDex(dexKey: string): AbstractDexClient {
    return this.registeredDexs.get(dexKey);
  }

  getAllDexKeys(): string[] {
    return ['dydxv4'];
  }
}


