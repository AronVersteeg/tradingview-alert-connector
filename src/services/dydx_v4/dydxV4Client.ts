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

  private readonly TOLERANCE = 0.001;
  private readonly MAX_ATTEMPTS = 5;

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
  // MAIN ENTRY POINT
  // =====================================================

  async placeOrder(alert: AlertObject): Promise<void> {

    const market = alert.market.replace(/_/g, '-');
    const targetSize = this.getTargetSize(alert, alert.size);

    console.log("🎯 Target position:", targetSize);

    await this.cancelOpenOrders(market);

    await this.reachTargetPosition(market, targetSize);
  }

  // =====================================================
  // SELF-HEALING DELTA ENGINE
  // =====================================================

  private async reachTargetPosition(market: string, targetSize: number) {

    for (let attempt = 1; attempt <= this.MAX_ATTEMPTS; attempt++) {

      await this.sleep(1500);

      const currentSize = await this.getCurrentSize(market);
      const diffRaw = targetSize - currentSize;
      const diff = Number(diffRaw.toFixed(3));

      console.log(
        `Attempt ${attempt} | Current: ${currentSize} | Target: ${targetSize} | Diff: ${diff}`
      );

      // ✅ Binnen tolerance → klaar
      if (Math.abs(diff) < this.TOLERANCE) {
        console.log("✅ Target bereikt (binnen tolerance).");
        return;
      }

      const side = diff > 0 ? OrderSide.BUY : OrderSide.SELL;
      const size = Math.abs(diff);

      const price = side === OrderSide.BUY ? 999999 : 1;

      const clientId = parseInt(
        crypto.randomBytes(4).toString('hex'),
        16
      );

      console.log("🔄 Correctie order:", { side, size });

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
        OrderExecution.DEFAULT,
        false,
        false
      );

      // extra stabilisatie-wacht
      await this.sleep(2000);
    }

    console.log("⚠️ Max attempts bereikt — positie mogelijk inconsistent.");
  }

  // =====================================================
  // CANCEL OPEN ORDERS
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

  private sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  private getIndexerConfig(): IndexerConfig {
    return new IndexerConfig(
      config.get('DydxV4.IndexerConfig.httpsEndpoint'),
      config.get('DydxV4.IndexerConfig.wssEndpoint')
    );
  }
}












