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

const STOP_LOSS_PCT = 0.01;   // 1% stop
const SLIPPAGE_PCT = 0.05;

/* ================================================= */

export class DydxV4Client extends AbstractDexClient {

  async getIsAccountReady(): Promise<boolean> {
    const sub = await this.getSubAccount();
    return !!sub && Number(sub.freeCollateral) > 0;
  }

  async getSubAccount() {
    const client = this.buildIndexerClient();
    const wallet = await this.generateLocalWallet();
    if (!wallet) return;
    const res = await client.account.getSubaccount(wallet.address, 0);
    return res.subaccount;
  }

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

  async placeOrder(alert: AlertObject) {
    const order = await this.buildOrderParams(alert);
    const { client, subaccount } = await this.buildCompositeClient();

    const side = order.side;
    const market = order.market;
    const size = order.size;

    const entryPrice =
      side === OrderSide.BUY
        ? order.price * (1 + SLIPPAGE_PCT)
        : order.price * (1 - SLIPPAGE_PCT);

    const entryClientId = this.generateDeterministicClientId(alert);
    console.log('ENTRY clientId:', entryClientId);

    /* ---------- ENTRY ---------- */

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

    /* ---------- STOP LOSS (LIMIT + TRIGGER) ---------- */

    const stopSide =
      side === OrderSide.BUY ? OrderSide.SELL : OrderSide.BUY;

    const stopPrice =
      side === OrderSide.BUY
        ? order.price * (1 - STOP_LOSS_PCT)
        : order.price * (1 + STOP_LOSS_PCT);

    const stopClientId = this.generateRandomInt32();
    console.log('STOP clientId:', stopClientId);

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
      true,        // reduceOnly
      stopPrice    // trigger
    );

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

  /* ===================== CLIENT ===================== */

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


