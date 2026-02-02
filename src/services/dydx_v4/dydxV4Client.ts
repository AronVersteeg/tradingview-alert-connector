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

  /* ==================== REQUIRED ABSTRACT METHODS ==================== */

  async getIsAccountReady(): Promise<boolean> {
    const subAccount = await this.getSubAccount();
    if (!subAccount) return false;
    return Number(subAccount.freeCollateral) > 0;
  }

  async placeOrder(alertMessage: AlertObject): Promise<OrderResult | void> {
    const orderParams = await this.buildOrderParams(alertMessage);
    const { client, subaccount } = await this.buildCompositeClient();

    const market = orderParams.market;
    const side = orderParams.side;
    const size = orderParams.size;

    /* ---------- POSITION GUARD (CRUCIAAL) ---------- */

    const currentPosition = await this.getCurrentPosition(market);

    if (side === OrderSide.SELL) {
      if (!currentPosition || Number(currentPosition.size) <= 0) {
        console.log('Blocked SELL: no long position');
        return;
      }
    }

    if (side === OrderSide.BUY) {
      if (currentPosition && Number(currentPosition.size) >= 0) {
        console.log('Blocked BUY: no short position');
        return;
      }
    }

    /* ---------- ENTRY MARKET ORDER ---------- */

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

    const result: OrderResult = {
      side,
      size,
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

  /* ==================== POSITION & PARAMS ==================== */

  async buildOrderParams(alertMessage: AlertObject): Promise<dydxV4OrderParams> {
    const orderSide =
      alertMessage.order === 'buy'
        ? OrderSide.BUY
        : OrderSide.SELL;

    let orderSize: number;

    if (alertMessage.sizeByLeverage) {
      const account = await


