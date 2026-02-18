import {
  BECH32_PREFIX,
  IndexerClient,
  CompositeClient,
  Network,
  SubaccountClient,
  ValidatorConfig,
  LocalWallet,
  OrderSide,
  OrderTimeInForce,
  OrderType,
  OrderExecution,
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

  private STOP_PERCENT = 1.0;

  // =====================================================
  // INIT
  // =====================================================

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

  // =====================================================
  // MAIN ORDER LOGIC
  // =====================================================

  async placeOrder(alert: AlertObject): Promise<void> {

    const market = alert.market.replace(/_/g, '-');

    // 1️⃣ Cancel bestaande conditional orders
    await this.cancelOpenOrders(market);

    const currentSize = await this.getCurrentSize(market);
    const targetSize = this.getTargetSize(alert, alert.size);
    const delta = targetSize - currentSize;

    console.log("Current:", currentSize);
    console.log("Target:", targetSize);
    console.log("Delta:", delta);

    // 2️⃣ MARKET EXECUTION
    if (delta !== 0) {

      const side = delta > 0 ? OrderSide.BUY : OrderSide.SELL;
      const size = Math.abs(delta);
      const price = side === OrderSide.BUY ? 999999 : 1;

      const clientId = parseInt(
        crypto.randomBytes(4).toString('hex'),
        16
      );

      await this.client.placeOrder(
        this.subaccount,
        market,
        OrderType.MARKET,
        side,
        price,
        size,
        clientId,
        OrderTimeInForce.IOC,
        0,
        OrderExecution.DEFAULT,  // MARKET mag DEFAULT gebruiken
        false,
        false
      );

      console.log("✅ Market order geplaatst");
    }

    // 3️⃣ STOP LOSS
    const newSize = await this.getCurrentSize(market);

    if (newSize !== 0) {
      await this.placeStopLoss(market, newSize);
    }
  }

  // =====================================================
  // STOP LOSS
  // =====================================================

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

    await this.client.placeOrder(
      this.subaccount,
      market,
      OrderType.STOP_MARKET,
      side,
      triggerPrice,
      size,
      clientId,
      OrderTimeInForce.GTT,
      0,
      OrderExecution.REDUCE_ONLY,  // 🔥 FIX: geen DEFAULT hier
      true,                        // reduceOnly
      false
    );

    console.log("🛑 Stop loss geplaatst");
  }

  // =====================================================
  // CANCEL CONDITIONAL ORDERS
  // =====================================================

  private async cancelOpenOrders(market: string) {

    const res = await this.indexer.account.getSubaccountOrders(
      this.wallet.address,
      0
    );

    const openOrders = res.orders?.filter((o: any) =>
      o.market === market && o.status === 'OPEN'
    ) || [];

    for (const order of openOrders) {

      const clientId =
        typeof order.clientId === 'number'
          ? order.clientId
          : parseInt(order.clientId, 10);

      await this.client.cancelOrder(
        this.subaccount,
        clientId,
        0,
        undefined
      );
    }
  }

  // =====================================================
  // HELPERS
  // =====================================================

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
      case 'FLAT':
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









