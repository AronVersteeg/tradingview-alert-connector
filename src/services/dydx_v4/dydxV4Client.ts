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

type PositionState = 'LONG' | 'SHORT' | 'FLAT';

export class DydxV4Client extends AbstractDexClient {

  private wallet!: LocalWallet;
  private client!: CompositeClient;
  private subaccount!: SubaccountClient;
  private indexer!: IndexerClient;
  private initialized = false;

  private processingMarkets = new Set<string>();
  private marketState = new Map<string, PositionState>();

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
      await new Promise(res => setTimeout(res, 50));
    }

    this.processingMarkets.add(market);

    try {

      const desired = alert.desired_position as PositionState;

      let current = this.marketState.get(market);

      if (!current) {
        current = await this.getCurrentPositionFromIndexer(market);
        this.marketState.set(market, current);
      }

      console.log("Current (state-driven):", current);

      if (current === desired) {
        console.log("Already correct direction.");
        return;
      }

      // CLOSE
      if (current !== 'FLAT') {

        await this.sendOrder({
          market,
          side: current === 'LONG' ? OrderSide.SELL : OrderSide.BUY,
          size: alert.size,
          reduceOnly: true,
          price: alert.price
        });

        this.marketState.set(market, 'FLAT');
      }

      // OPEN
      if (desired !== 'FLAT') {

        await this.sendOrder({
          market,
          side: desired === 'LONG' ? OrderSide.BUY : OrderSide.SELL,
          size: alert.size,
          reduceOnly: false,
          price: alert.price
        });

        this.marketState.set(market, desired);
      }

    } finally {
      this.processingMarkets.delete(market);
    }
  }

  private async getCurrentPositionFromIndexer(market: string): Promise<PositionState> {

    const response = await this.indexer.account.getSubaccountPerpetualPositions(
      this.wallet.address,
      0
    );

    const positions = response?.positions || [];

    const marketPositions = positions.filter((p: any) =>
      p.market === market
    );

    if (marketPositions.length === 0) return 'FLAT';

    marketPositions.sort(
      (a: any, b: any) =>
        Number(b.createdAtHeight) - Number(a.createdAtHeight)
    );

    const latest = marketPositions[0];
    const size = Number(latest.size);

    if (size > 0) return 'LONG';
    if (size < 0) return 'SHORT';

    return 'FLAT';
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






