import {
  BECH32_PREFIX,
  IndexerClient,
  CompositeClient,
  Network,
  SubaccountClient,
  ValidatorConfig,
  LocalWallet,
  OrderExecution,
  OrderSide,
  OrderTimeInForce,
  OrderType,
  IndexerConfig
} from '@dydxprotocol/v4-client-js';

import { AlertObject } from '../../types';
import 'dotenv/config';
import config from 'config';
import crypto from 'crypto';
import { AbstractDexClient } from '../abstractDexClient';

export class DydxV4Client extends AbstractDexClient {

  private wallet!: LocalWallet;
  private client!: CompositeClient;
  private subaccount!: SubaccountClient;
  private indexer!: IndexerClient;
  private initialized = false;

  async init(): Promise<void> {

    this.wallet = await LocalWallet.fromMnemonic(
      process.env.DYDX_V4_MNEMONIC!,
      BECH32_PREFIX
    );

    const validatorConfig = new ValidatorConfig(
      config.get('DydxV4.ValidatorConfig.restEndpoint'),
      'dydx-mainnet-1',
      {
        CHAINTOKEN_DENOM: 'adydx',
        CHAINTOKEN_DECIMALS: 18,
        USDC_DENOM: 'uusdc',
        USDC_GAS_DENOM: 'uusdc',
        USDC_DECIMALS: 6
      }
    );

    const network =
      process.env.NODE_ENV === 'production'
        ? new Network('mainnet', this.getIndexerConfig(), validatorConfig)
        : Network.testnet();

    this.client = await CompositeClient.connect(network);
    this.subaccount = new SubaccountClient(this.wallet, 0);

    this.indexer = new IndexerClient(
      process.env.NODE_ENV === 'production'
        ? this.getIndexerConfig()
        : Network.testnet().indexerConfig
    );

    this.initialized = true;
  }

  async getIsAccountReady(): Promise<boolean> {
    return this.initialized;
  }

  async placeOrder(alert: AlertObject): Promise<void> {

    const market = alert.market.replace(/_/g, '-');
    const desired = alert.desired_position;

    const { current, currentSize } = await this.getCurrentPosition(market);

    console.log("Current:", current, "Size:", currentSize);

    if (current === desired) {
      console.log("Already correct direction.");
      return;
    }

    // CLOSE EXISTING
    if (current !== 'FLAT') {

      await this.sendOrder({
        market,
        side: current === 'LONG' ? OrderSide.SELL : OrderSide.BUY,
        size: Math.abs(currentSize),
        reduceOnly: true,
        price: alert.price
      });

      // 🔥 WAIT UNTIL FLAT
      await this.waitUntilFlat(market);
    }

    // OPEN NEW
    if (desired !== 'FLAT') {

      await this.sendOrder({
        market,
        side: desired === 'LONG' ? OrderSide.BUY : OrderSide.SELL,
        size: alert.size,
        reduceOnly: false,
        price: alert.price
      });
    }
  }

  private async waitUntilFlat(market: string) {

    for (let i = 0; i < 10; i++) {

      const { current } = await this.getCurrentPosition(market);

      if (current === 'FLAT') {
        console.log("Position confirmed closed.");
        return;
      }

      await new Promise(res => setTimeout(res, 1000));
    }

    console.log("Warning: position did not close in time.");
  }

  private async getCurrentPosition(market: string) {

    const response = await this.indexer.account.getSubaccountPerpetualPositions(
      this.wallet.address,
      0
    );

    const positions = response?.positions || [];

    const openPosition = positions.find((p: any) =>
      p.market === market &&
      p.status === 'OPEN' &&
      Number(p.size) !== 0
    );

    let current: 'LONG' | 'SHORT' | 'FLAT' = 'FLAT';
    let currentSize = 0;

    if (openPosition) {
      currentSize = Number(openPosition.size);
      if (currentSize > 0) current = 'LONG';
      if (currentSize < 0) current = 'SHORT';
    }

    return { current, currentSize };
  }

  private async sendOrder(params: {
    market: string;
    side: OrderSide;
    size: number;
    reduceOnly: boolean;
    price: number;
  }) {

    const { market, side, size, reduceOnly, price } = params;

    console.log("Sending:", { market, side, size, reduceOnly });

    await this.client.placeOrder(
      this.subaccount,
      market,
      OrderType.MARKET,
      side,
      price,
      size,
      parseInt(crypto.randomBytes(4).toString('hex'), 16),
      OrderTimeInForce.IOC,
      0,
      OrderExecution.DEFAULT,
      false,
      reduceOnly,
      null
    );
  }

  private getIndexerConfig(): IndexerConfig {
    return new IndexerConfig(
      config.get('DydxV4.IndexerConfig.httpsEndpoint'),
      config.get('DydxV4.IndexerConfig.wssEndpoint')
    );
  }
}



