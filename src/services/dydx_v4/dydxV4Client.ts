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

  private STOP_PERCENT: number = 1.0;

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

      await this.cancelOpenOrders(market);

      const currentSize = await this.getCurrentSize(market);
      const targetSize = this.getTargetSize(alert, alert.size);
      const delta = targetSize - currentSize;

      console.log("Current:", currentSize);
      console.log("Target:", targetSize);
      console.log("Delta:", delta);

      if (delta !== 0) {

        const side = delta > 0 ? OrderSide.BUY : OrderSide.SELL;
        const size = Math.abs(delta);     // ✅ number
        const price = side === OrderSide.BUY ? 999999 : 1; // ✅ number

        const clientId = parseInt(
          crypto.randomBytes(4).toString('hex'),
          16
        );

        const response = await this.client.placeOrder(
          this.subaccount,
          market,
          OrderType.MARKET,
          side,
          price,
          size,
          clientId,
          OrderTimeInForce.IOC,
          0,
          OrderExecution.DEFAULT,
          false,
          false,
          undefined
        );

        console.log("✅ Market order:", response);
      }

      const newSize = await this.getCurrentSize(market);

      if (newSize !== 0) {
        await this.placeStopLoss(market, newSize);
      }

    } finally {
      this.processingMarkets.delete(market);
    }
  }

  // ================= STOP =================

  private async placeStopLoss(market: string, positionSize: number) {

    const positions = await this.indexer.account.getSubaccountPerpetualPositions(
      this.wallet.address,
      0
    );

    const pos = positions.positions.find((p: any) => p.market === market);
    if (!pos) return;

    const entryPrice = Number(pos.entryPrice);
    const isLong = positionSize > 0;

    const triggerPrice = isLong
      ? entryPrice * (1 - this.STOP_PERCENT / 100)
      : entryPrice * (1 + this.STOP_PERCENT / 100);

    const size = Math.abs(positionSize);
    const side = isLong ? OrderSide.SELL : OrderSide.BUY;

    const clientId = parseInt(
      crypto.randomBytes(4).toString('hex'),
      16
    );

    const stopResponse = await this.client.placeOrder(
      this.subaccount,
      market,
      OrderType.STOP_MARKET,
      side,
      triggerPrice, // ✅ number
      size,         // ✅ number
      clientId,
      OrderTimeInForce.GTT,
      0,
      OrderExecution.DEFAULT,
      true,
      false,
      undefined
    );

    console.log("🛑 Stop placed:", stopResponse);
  }

  // ================= CANCEL =================

  private async cancelOpenOrders(market: string) {

    const res = await this.indexer.account.getSubaccountOrders(
      this.wallet.address,
      0
    );

    const openOrders = res.orders?.filter((o: any) =>
      o.market === market && o.status === 'OPEN'
    ) || [];

    for (const order of openOrders) {

      await this.client.cancelOrder(
        this.subaccount,
        market,
        Number(order.clientId), // ✅ number
        0,
        undefined
      );
    }
  }

  private async getCurrentSize(market: string): Promise<number> {

    const response = await this.indexer.account.getSubaccountPerpetualPositions(
      this.wallet.address,
      0
    );

    const pos = response.positions.find((p: any) => p.market === market);

    return pos ? Number(pos.size) : 0;
  }

  private getTargetSize(alert: AlertObject, baseSize: number): number {

    const dir = alert.desired_position?.toUpperCase();

    switch (dir) {
      case 'BUY':
      case 'LONG':
        return Math.abs(baseSize);
      case 'SELL':
      case 'SHORT':
        return -Math.abs(baseSize);
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







