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

import { dydxV4OrderParams, AlertObject, OrderResult } from '../../types';
import { doubleSizeIfReverseOrder } from '../../helper';
import 'dotenv/config';
import config from 'config';
import { AbstractDexClient } from '../abstractDexClient';
import crypto from 'crypto';

/* ================= CONFIG ================= */

const STOP_LOSS_PCT = 0.01;      // 1%
const TRAIL_PCT = 0.005;         // 0.5%
const TRAIL_STEP_PCT = 0.002;    // 0.2%
const TRAIL_INTERVAL_MS = 5000;

/* ========================================== */

type TrailingState = {
  market: string;
  side: OrderSide;
  size: number;
  bestPrice: number;
  stopClientId: number;
};

export class DydxV4Client extends AbstractDexClient {
  private trailingState = new Map<string, TrailingState>();

  /* ================= ACCOUNT ================= */

  async getIsAccountReady() {
    const acc = await this.getSubAccount();
    return !!acc && Number(acc.freeCollateral) > 0;
  }

  async getSubAccount() {
    const client = this.buildIndexerClient();
    const wallet = await this.generateLocalWallet();
    if (!wallet) return;
    const res = await client.account.getSubaccount(wallet.address, 0);
    return res.subaccount;
  }

  /* ================= PARAMS ================= */

  async buildOrderParams(alert: AlertObject): Promise<dydxV4OrderParams> {
    const side =
      alert.order === 'buy' ? OrderSide.BUY : OrderSide.SELL;

    let size = Number(alert.size);
    size = doubleSizeIfReverseOrder(alert, size);

    return {
      market: alert.market.replace(/_/g, '-'),
      side,
      size,
      price: Number(alert.price)
    };
  }

  /* ================= ENTRY + SL ================= */

  async placeOrder(alert: AlertObject) {
    const order = await this.buildOrderParams(alert);
    const { client, subaccount } = await this.buildCompositeClient();

    const isLong = order.side === OrderSide.BUY;

    const entryPrice = isLong
      ? order.price * 1.05
      : order.price * 0.95;

    const entryClientId = this.generateDeterministicClientId(alert);

    // ENTRY
    await client.placeOrder(
      subaccount,
      order.market,
      OrderType.MARKET,
      order.side,
      entryPrice,
      order.size,
      entryClientId,
      OrderTimeInForce.GTT,
      120000,
      OrderExecution.DEFAULT,
      false,
      false,
      null
    );

    // INITIAL STOP
    const stopSide = isLong ? OrderSide.SELL : OrderSide.BUY;
    const stopPrice = isLong
      ? order.price * (1 - STOP_LOSS_PCT)
      : order.price * (1 + STOP_LOSS_PCT);

    const stopClientId = this.generateRandomInt32();

    await client.placeOrder(
      subaccount,
      order.market,
      OrderType.LIMIT,
      stopSide,
      stopPrice,
      order.size,
      stopClientId,
      OrderTimeInForce.GTT,
      24 * 60 * 60 * 1000,
      OrderExecution.DEFAULT,
      false,
      true,
      stopPrice
    );

    // INIT TRAILING STATE
    this.trailingState.set(
      `${alert.strategy}-${order.market}`,
      {
        market: order.market,
        side: order.side,
        size: order.size,
        bestPrice: order.price,
        stopClientId
      }
    );

    const result: OrderResult = {
      side: order.side,
      size: order.size,
      orderId: String(entryClientId)
    };

    await this.exportOrder(
      'DydxV4',
      alert.strategy,
      result,
      alert.price,
      alert.market
    );

    return result;
  }

  /* ================= TRAILING LOOP ================= */

  startTrailingLoop() {
    setInterval(async () => {
      for (const state of this.trailingState.values()) {
        try {
          const price = await this.getLastPrice(state.market);
          const isLong = state.side === OrderSide.BUY;

          const improved = isLong
            ? price > state.bestPrice * (1 + TRAIL_STEP_PCT)
            : price < state.bestPrice * (1 - TRAIL_STEP_PCT);

          if (!improved) continue;

          const newStop = isLong
            ? price * (1 - TRAIL_PCT)
            : price * (1 + TRAIL_PCT);

          const { client, subaccount } = await this.buildCompositeClient();

          // ✅ CORRECT cancel signature
          await client.cancelOrder(
            subaccount,
            state.market,
            state.stopClientId,
            undefined
          );

          const newStopId = this.generateRandomInt32();

          await client.placeOrder(
            subaccount,
            state.market,
            OrderType.LIMIT,
            isLong ? OrderSide.SELL : OrderSide.BUY,
            newStop,
            state.size,
            newStopId,
            OrderTimeInForce.GTT,
            24 * 60 * 60 * 1000,
            OrderExecution.DEFAULT,
            false,
            true,
            newStop
          );

          state.bestPrice = price;
          state.stopClientId = newStopId;
        } catch (e) {
          console.error('Trailing error:', e);
        }
      }
    }, TRAIL_INTERVAL_MS);
  }

  /* ================= PRICE ================= */

  async getLastPrice(market: string): Promise<number> {
    const client = this.buildIndexerClient();
    const res = await client.markets.getPerpetualMarkets();

    const m = res.markets.find(
      (x: any) => x.market === market
    );

    if (!m) throw new Error(`Market ${market} not found`);

    return Number(m.oraclePrice);
  }

  /* ================= HELPERS ================= */

  private buildCompositeClient = async () => {
    const validator = new ValidatorConfig(
      config.get('DydxV4.ValidatorConfig.restEndpoint'),
      'dydx-mainnet-1',
      {
        CHAINTOKEN_DENOM: 'adydx',
        CHAINTOKEN_DECIMALS: 18,
        USDC_DENOM:
          'ibc/8E27BA2D5493AF5636760E354E46004562C46AB7EC0CC4C1CA14E9E20E2545B5',
        USDC_GAS_DENOM: 'uusdc',
        USDC_DECIMALS: 6
      }
    );

    const network = new Network(
      'mainnet',
      this.getIndexerConfig(),
      validator
    );

    const client = await CompositeClient.connect(network);
    const wallet = await this.generateLocalWallet();
    const subaccount = new SubaccountClient(wallet!, 0);

    return { client, subaccount };
  };

  private generateLocalWallet = async () => {
    if (!process.env.DYDX_V4_MNEMONIC) {
      throw new Error('DYDX_V4_MNEMONIC not set');
    }

    return LocalWallet.fromMnemonic(
      process.env.DYDX_V4_MNEMONIC,
      BECH32_PREFIX
    );
  };

  private buildIndexerClient = () => {
    return new IndexerClient(this.getIndexerConfig());
  };

  private getIndexerConfig = () => {
    return new IndexerConfig(
      config.get('DydxV4.IndexerConfig.httpsEndpoint'),
      config.get('DydxV4.IndexerConfig.wssEndpoint')
    );
  };

  private generateDeterministicClientId(alert: AlertObject): number {
    const base = [
      alert.strategy,
      alert.market,
      alert.order,
      alert.position,
      alert.size,
      alert.price
    ].join('|');

    const hash = crypto
      .createHash('sha256')
      .update(base)
      .digest('hex');

    return parseInt(hash.slice(0, 8), 16);
  }

  private generateRandomInt32(): number {
    return Math.floor(Math.random() * 2_147_483_647);
  }
}




