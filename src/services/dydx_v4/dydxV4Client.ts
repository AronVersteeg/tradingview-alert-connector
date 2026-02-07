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

/* ===================== CONFIG ===================== */

// % afstand van stoploss t.o.v. entry
const STOP_LOSS_PCT = 0.01;        // 1%

// wanneer stoploss mag trailen
const TRAIL_STEP_PCT = 0.005;      // 0.5%

/* ================================================== */

export class DydxV4Client extends AbstractDexClient {

  /* ===================== REQUIRED ABSTRACT METHODS ===================== */

  async getIsAccountReady(): Promise<boolean> {
    try {
      const sub = await this.getSubAccount();
      return !!sub && Number(sub.freeCollateral) > 0;
    } catch {
      return false;
    }
  }

  async cancelOrder(market: string, orderId: number): Promise<void> {
    const { client, subaccount } = await this.buildCompositeClient();
    await client.cancelOrder(subaccount, market, orderId);
  }

  async getOrders() {
    const client = this.buildIndexerClient();
    const wallet = await this.generateLocalWallet();
    if (!wallet) return [];
    return client.account.getSubaccountOrders(wallet.address, 0);
  }

  /* ===================== ACCOUNT ===================== */

  async getSubAccount() {
    const client = this.buildIndexerClient();
    const wallet = await this.generateLocalWallet();
    if (!wallet) return;
    const res = await client.account.getSubaccount(wallet.address, 0);
    return res.subaccount;
  }

  /* ===================== ORDER PARAMS ===================== */

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

  /* ===================== MAIN ENTRY ===================== */

  async placeOrder(alert: AlertObject) {
    const order = await this.buildOrderParams(alert);
    const { client, subaccount } = await this.buildCompositeClient();

    const side = order.side;
    const market = order.market;
    const size = order.size;

    const entryPrice =
      side === OrderSide.BUY
        ? order.price * 1.05
        : order.price * 0.95;

    const entryClientId = this.generateDeterministicClientId(alert);
    console.log('ENTRY clientId:', entryClientId);

    /* ---------- ENTRY MARKET ORDER ---------- */

    await client.placeOrder(
      subaccount,
      market,
      OrderType.MARKET,
      side,
      entryPrice,
      size,
      entryClientId,
      OrderTimeInForce.GTT,
      120000,
      OrderExecution.DEFAULT,
      false,
      false,
      null
    );

    /* ---------- INITIAL LIMIT STOP-LOSS ---------- */

    const stopSide =
      side === OrderSide.BUY ? OrderSide.SELL : OrderSide.BUY;

    let stopPrice =
      side === OrderSide.BUY
        ? order.price * (1 - STOP_LOSS_PCT)
        : order.price * (1 + STOP_LOSS_PCT);

    let stopClientId = this.generateRandomInt32();

    await client.placeOrder(
      subaccount,
      market,
      OrderType.LIMIT,
      stopSide,
      stopPrice,
      size,
      stopClientId,
      OrderTimeInForce.GTT,
      24 * 60 * 60 * 1000,
      OrderExecution.DEFAULT,
      false,
      true,           // reduceOnly
      stopPrice       // triggerPrice
    );

    console.log('STOP placed at', stopPrice);

    /* ---------- TRAILING LOGIC (LIGHT STATE) ---------- */

    const trailIfNeeded = async (latestPrice: number) => {
      let newStop =
        side === OrderSide.BUY
          ? latestPrice * (1 - STOP_LOSS_PCT)
          : latestPrice * (1 + STOP_LOSS_PCT);

      const movedEnough =
        side === OrderSide.BUY
          ? newStop > stopPrice * (1 + TRAIL_STEP_PCT)
          : newStop < stopPrice * (1 - TRAIL_STEP_PCT);

      if (!movedEnough) return;

      console.log('TRAIL stop from', stopPrice, 'to', newStop);

      await this.cancelOrder(market, stopClientId);

      stopClientId = this.generateRandomInt32();
      stopPrice = newStop;

      await client.placeOrder(
        subaccount,
        market,
        OrderType.LIMIT,
        stopSide,
        stopPrice,
        size,
        stopClientId,
        OrderTimeInForce.GTT,
        24 * 60 * 60 * 1000,
        OrderExecution.DEFAULT,
        false,
        true,
        stopPrice
      );
    };

    // optioneel: hier later indexer / price feed aan hangen

    /* ---------- EXPORT ---------- */

    const result: OrderResult = {
      side,
      size,
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

  /* ===================== CLIENT BUILDERS ===================== */

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

  /* ===================== IDS ===================== */

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

