// FILE: dydxV4Client.ts

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
import { _sleep } from '../../helper';
import 'dotenv/config';
import config from 'config';
import crypto from 'crypto';
import { AbstractDexClient } from '../abstractDexClient';

const processedSignals = new Set<string>();

export class DydxV4Client extends AbstractDexClient {

  private wallet!: LocalWallet;
  private client!: CompositeClient;
  private subaccount!: SubaccountClient;
  private indexer!: IndexerClient;

  constructor() {
    super();
    this.initialize();
  }

  // ================= INIT =================

  private async initialize() {
    console.log("Initializing dYdX V4 client...");

    if (!process.env.DYDX_V4_MNEMONIC) {
      throw new Error("DYDX_V4_MNEMONIC missing in environment variables");
    }

    // Create wallet once
    this.wallet = LocalWallet.fromMnemonic(
      process.env.DYDX_V4_MNEMONIC,
      BECH32_PREFIX
    );

    console.log("Wallet created:", this.wallet.address);

    // Validator config
    const validatorConfig = new ValidatorConfig(
      config.get('DydxV4.ValidatorConfig.restEndpoint'),
      'dydx-mainnet-1',
      {
        CHAINTOKEN_DENOM: 'adydx',
        CHAINTOKEN_DECIMALS: 18,
        USDC_DENOM: config.get('DydxV4.USDC_DENOM'),
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

    const indexerConfig =
      process.env.NODE_ENV === 'production'
        ? this.getIndexerConfig()
        : Network.testnet().indexerConfig;

    this.indexer = new IndexerClient(indexerConfig);

    console.log("dYdX client initialized successfully.");
  }

  async getIsAccountReady(): Promise<boolean> {
    return !!this.wallet && !!this.client;
  }

  // ================= MAIN ENTRY =================

  async placeOrder(alert: AlertObject) {

    if (!this.client || !this.wallet) {
      throw new Error("Client not initialized");
    }

    const signalId = `${alert.strategy}|${alert.market}|${alert.time}`;
    if (processedSignals.has(signalId)) {
      console.log('Duplicate signal ignored:', signalId);
      return;
    }
    processedSignals.add(signalId);

    console.log("Processing signal:", signalId);

    // ----- CURRENT POSITION -----
    const orders = await this.indexer.account.getSubaccountOrders(
      this.wallet.address,
      0
    );

    const open = orders.find(o => o.status === 'OPEN');

    let current: 'LONG' | 'SHORT' | 'FLAT' = 'FLAT';
    if (open) current = open.side === 'BUY' ? 'LONG' : 'SHORT';

    const desired = alert.desired_position;

    if (current === desired) {
      console.log('Already in desired position:', desired);
      return;
    }

    // ----- EXIT -----
    if (current !== 'FLAT' && open) {
      await this.sendOrder({
        alert,
        side: current === 'LONG' ? OrderSide.SELL : OrderSide.BUY,
        size: Math.abs(open.size),
        reduceOnly: true
      });
    }

    // ----- ENTRY -----
    if (desired !== 'FLAT') {
      await this.sendOrder({
        alert,
        side: desired === 'LONG' ? OrderSide.BUY : OrderSide.SELL,
        size: alert.size,
        reduceOnly: false
      });
    }
  }

  // ================= ORDER SENDER =================

  private async sendOrder(params: {
    alert: AlertObject;
    side: OrderSide;
    size: number;
    reduceOnly: boolean;
  }) {

    const { alert, side, size, reduceOnly } = params;

    const clientId = this.deterministicClientId(alert, side);

    console.log("Sending order:", {
      market: alert.market,
      side,
      size,
      reduceOnly
    });

    const result = await this.client.placeOrder(
      this.subaccount,
      alert.market.replace(/_/g, '-'),
      OrderType.MARKET,
      side,
      alert.price,
      size,
      clientId,
      OrderTimeInForce.GTT,
      120000,
      OrderExecution.DEFAULT,
      false,
      reduceOnly,
      null
    );

    console.log("Order result:", result);

    await _sleep(1000);
  }

  // ================= HELPERS =================

  private deterministicClientId(alert: AlertObject, side: OrderSide): number {
    const raw = `${alert.strategy}|${alert.market}|${alert.time}|${side}`;
    return parseInt(
      crypto.createHash('sha256').update(raw).digest('hex').slice(0, 8),
      16
    );
  }

  private getIndexerConfig() {
    return new IndexerConfig(
      config.get('DydxV4.IndexerConfig.httpsEndpoint'),
      config.get('DydxV4.IndexerConfig.wssEndpoint')
    );
  }
}









