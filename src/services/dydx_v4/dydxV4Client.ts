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

export class DydxV4Client extends AbstractDexClient {

  /* -------------------- SUBACCOUNT -------------------- */

  async getSubAccount() {
    const client = this.buildIndexerClient();
    const localWallet = await this.generateLocalWallet();
    if (!localWallet) return;
    const response = await client.account.getSubaccount(
      localWallet.address,
      0
    );
    return response.subaccount;
  }

  /* -------------------- 🟨 NIEUW: CURRENT POSITION -------------------- */

  async getCurrentPosition(market: string) {
    const client = this.buildIndexerClient();
    const wallet = await this.generateLocalWallet();
    if (!wallet) return null;

    const positions =
      await client.account.getSubaccountPerpetualPositions(
        wallet.address,
        0
      );

    return positions.find(
      (p) => p.market === market
    ) || null;
  }

  /* -------------------- ORDER PARAMS -------------------- */

  async buildOrderParams(alertMessage: AlertObject) {
    const orderSide =
      alertMessage.order === 'buy'
        ? OrderSide.BUY
        : OrderSide.SELL;

    let orderSize = Number(alertMessage.size);
    orderSize = doubleSizeIfReverseOrder(alertMessage, orderSize);

    const market = alertMessage.market.replace(/_/g, '-');

    return {
      market,
      side: orderSide,
      size: orderSize,
      price: Number(alertMessage.price)
    };
  }

  /* -------------------- PLACE ORDER -------------------- */

  async placeOrder(alertMessage: AlertObject) {
    const orderParams = await this.buildOrderParams(alertMessage);
    const { client, subaccount } = await this.buildCompositeClient();

    const market = orderParams.market;
    const side = orderParams.side;
    const size = orderParams.size;

    /* -------- 🟨 NIEUW: POSITION GUARD -------- */

    const currentPosition = await this.getCurrentPosition(market);

    if (side === OrderSide.SELL) {
      if (!currentPosition || Number(currentPosition.size) <= 0) {
        console.log('🟨 Blocked SELL: no long position');
        return;
      }
    }

    if (side === OrderSide.BUY) {
      if (currentPosition && Number(currentPosition.size) >= 0) {
        console.log('🟨 Blocked BUY: no short position');
        return;
      }
    }

    /* -------- ENTRY ORDER (ongewijzigd) -------- */

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
      size,
      clientId,
      OrderTimeInForce.GTT,
      120000,
      OrderExecution.DEFAULT,
      false,
      false,
      null
    );

    console.log('Transaction Result:', tx);

    const orderResult: OrderResult = {
      side,
      size,
      orderId: String(clientId)
    };

    await this.exportOrder(
      'DydxV4',
      alertMessage.strategy,
      orderResult,
      alertMessage.price,
      alertMessage.market
    );

    return orderResult;
  }

  /* -------------------- CLIENT BUILDERS -------------------- */

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

  private buildIndexerClient = () => {
    return new IndexerClient(this.getIndexerConfig());
  };

  private getIndexerConfig = () => {
    return new IndexerConfig(
      config.get('DydxV4.IndexerConfig.httpsEndpoint'),
      config.get('DydxV4.IndexerConfig.wssEndpoint')
    );
  };

  /* -------------------- IDS -------------------- */

  private generateDeterministicClientId(alert: AlertObject): number {
    const baseString = [
      alert.strategy,
      alert.market,
      alert.order,
      alert.position,
      alert.size,
      alert.price
    ].join('|');

    const hash = crypto
      .createHash('sha256')
      .update(baseString)
      .digest('hex');

    return parseInt(hash.slice(0, 8), 16);
  }
}

