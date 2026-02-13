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

  private processingMarkets = new Set<string>();

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

    while (this.processingMarkets.has(market)) {
      await new Promise(res => setTimeout(res, 20));
    }

    this.processingMarkets.add(market);

    try {

      const currentSize = await this.getCurrentSize(market);
      const targetSize = this.getTargetSize(alert, alert.size);

      const delta = targetSize - currentSize;

      console.log("Current size:", currentSize);
      console.log("Target size:", targetSize);
      console.log("Delta:", delta);

      if (delta === 0) {
        console.log("No action needed.");
        return;
      }

      const side = delta > 0 ? OrderSide.BUY : OrderSide.SELL;
      const size = Math.abs(delta);

      const price = side === OrderSide.BUY ? 999999 : 1;

      console.log("Sending net order:", { market, side, size });

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
        false,
        null
      );

    } finally {
      this.processingMarkets.delete(market);
    }
  }

  private async getCurrentSize(market: string): Promise<number> {

    const response = await this.indexer.account.getSubaccountPerpetualPositions(
      this.wallet.address,
      0
    );

    const positions = response?.positions || [];

    const marketPositions = positions.filter((p: any) =>
      p.market === market
    );

    if (marketPositions.length === 0) return 0;

    marketPositions.sort(
      (a: any, b: any) =>
        Number(b.createdAtHeight) - Number(a.createdAtHeight)
    );

    return Number(marketPositions[0].size);
  }

  private getTargetSize(alert: AlertObject, baseSize: number): number {

    const dir = alert.desired_position?.toUpperCase();

    // 👇 NIEUW: support voor TradingView strategy order fills
    if (dir === 'BUY') return Math.abs(baseSize);
    if (dir === 'SELL') return -Math.abs(baseSize);

    switch (dir) {
      case 'LONG':
        return Math.abs(baseSize);
      case 'SHORT':
        return -Math.abs(baseSize);
      case 'FLAT':
        return 0;
      default:
        return 0;
    }
  }

  private getIndexerConfig(): IndexerConfig {
    return new IndexerConfig(
      config.get('DydxV4.IndexerConfig.httpsEndpoint'),
      config.get('DydxV4.IndexerConfig.wssEndpoint')
    );
  }
}






