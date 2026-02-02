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

type PositionState = 'FLAT' | 'LONG' | 'SHORT';

export class DydxV4Client extends AbstractDexClient {

  /* ==================== ACCOUNT ==================== */

  async getIsAccountReady() {
    const subAccount = await this.getSubAccount();
    if (!subAccount) return false;
    return Number(subAccount.freeCollateral) > 0;
  }

  async getSubAccount() {
    const client = this.buildIndexerClient();
    const wallet = await this.generateLocalWallet();
    if (!wallet) return;
    const res = await client.account.getSubaccount(wallet.address, 0);
    return res.subaccount;
  }

  /* ==================== STATE ==================== */

  private async getPositionState(market: string): Promise<PositionState> {
    const client = this.buildIndexerClient();
    const wallet = await this.generateLocalWallet();
    if (!wallet) return 'FLAT';

    const positions =
      await client.account.getSubaccountPerpetualPositions(
        wallet.address,
        0
      );

    const pos = positions.find(p => p.market === market);
    if (!pos || Number(pos.size) === 0) return 'FLAT';
    if (Number(pos.size) > 0) return 'LONG';
    return 'SHORT';
  }

  /* ==================== PARAMS ==================== */

  async buildOrderParams(alertMessage: AlertObject) {
    const side =
      alertMessage.order === 'buy'
        ? OrderSide.BUY
        : OrderSide.SELL;

    let size: number;

    if (alertMessage.sizeByLeverage) {
      const account = await this.getSubAccount();
      size =
        (Number(account.equity) * Number(alertMessage.sizeByLeverage)) /
        Number(alertMessage.price);
    } else if (alertMessage.sizeUsd) {
      size = Number(alertMessage.sizeUsd) / Number(alertMessage.price);
    } else {
      size = Number(alertMessage.size);
    }

    size = doubleSizeIfReverseOrder(alertMessage, size);

    return {
      market: alertMessage.market.replace(/_/g, '-'),
      side,
      size,
      price: Number(alertMessage.price)
    };
  }

  /* ==================== ORDER ==================== */

  async placeOrder(alertMessage: AlertObject) {
    const orderParams = await this.buildOrderParams(alertMessage);
    const { client, subaccount } = await this.buildCompositeClient();

    const market = orderParams.market;
    const side = orderParams.side;
    const state = await this.getPositionState(market);

    console.log(`STATE CHECK → ${state} | ALERT → ${side}`);

    // -------- STATE MACHINE LIGHT --------

    if (state === 'LONG' && side === OrderSide.BUY) {
      console.log('Blocked BUY: already LONG');
      return;
    }

    if (state === 'SHORT' && side === OrderSide.SELL) {
      console.log('Blocked SELL: already SHORT');
      return;
    }

    // ------------------------------------

    const slippage = 0.05;
    const price =
      side === OrderSide.BUY
        ? orderParams.price * (1 + slippage)
        : orderParams.price * (1 - slippage);

    const clientId = this.generateDeterministicClientId(alertMessage);
    console.log('Client ID:', clientId);

    const tx = await client.placeOrder(
      subaccount,
      market,
      OrderType.MARKET,
      side,
      price,
      orderParams.size,
      clientId,
      OrderTimeInForce.GTT,
      120000,
      OrderExecution.DEFAULT,
      false,
      false,
      null
    );

    console.log('Transaction Result:', tx);

    const result: OrderResult = {
      side,
      size: orderParams.size,
      orderId: String(clientId)
    };

    await this.exportOrder(
      'DydxV4',
      alertMessage.strategy,
      result,
      alertMessage.price,
      alertMessage.market
    );

    return result;
  }

  /* ==================== CLIENTS ==================== */

  private buildCompositeClient = async () => {
    const validatorConfig = new ValidatorConfig(
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
      validatorConfig
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

  private buildIndexerClient = () =>
    new IndexerClient(this.getIndexerConfig());

  private getIndexerConfig = () =>
    new IndexerConfig(
      config.get('DydxV4.IndexerConfig.httpsEndpoint'),
      config.get('DydxV4.IndexerConfig.wssEndpoint')
    );

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
}
