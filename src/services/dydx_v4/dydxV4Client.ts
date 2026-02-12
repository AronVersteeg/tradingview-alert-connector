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

export class DydxV4Client extends AbstractDexClient {

  private wallet!: LocalWallet;
  private client!: CompositeClient;
  private subaccount!: SubaccountClient;
  private indexer!: IndexerClient;
  private initialized = false;

  // ================= INIT =================

  async init(): Promise<void> {
    console.log("Initializing dYdX V4 client...");

    if (!process.env.DYDX_V4_MNEMONIC) {
      throw new Error("DYDX_V4_MNEMONIC missing");
    }

    this.wallet = await LocalWallet.fromMnemonic(
      process.env.DYDX_V4_MNEMONIC,
      BECH32_PREFIX
    );

    console.log("Wallet address:", this.wallet.address);

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

    const indexerConfig =
      process.env.NODE_ENV === 'production'
        ? this.getIndexerConfig()
        : Network.testnet().indexerConfig;

    this.indexer = new IndexerClient(indexerConfig);

    this.initialized = true;
    console.log("dYdX V4 client initialized.");
  }

  async getIsAccountReady(): Promise<boolean> {
    return this.initialized;
  }

  // ================= MAIN ORDER LOGIC =================

  async placeOrder(alert: AlertObject): Promise<void> {

    if (!this.initialized) {
      throw new Error("Client not initialized");
    }

    const market = alert.market.replace(/_/g, '-');
    const desired = alert.desired_position;

    console.log("Processing signal:", alert);

    // ===== GET CURRENT POSITION =====

    const positions = await this.indexer.account.getSubaccountPositions(
      this.wallet.address,
      0
    );

    const position = positions.find(p => p.market === market);

    let current: 'LONG' | 'SHORT' | 'FLAT' = 'FLAT';
    let currentSize = 0;

    if (position) {
      currentSize = Number(position.size);
      if (currentSize > 0) current = 'LONG';
      if (currentSize < 0) current = 'SHORT';
    }

    console.log("Current position:", current, "Size:", currentSize);

    // ===== NO CHANGE =====

    if (current === desired) {
      console.log("Already in desired position:", desired);
      return;
    }

    // ===== CLOSE EXISTING =====

    if (current !== 'FLAT') {
      console.log("Closing existing position...");

      await this.sendOrder({
        market,
        side: current === 'LONG' ? OrderSide.SELL : OrderSide.BUY,
        size: Math.abs(currentSize),
        reduceOnly: true
      });

      await _sleep(1000);
    }

    // ===== OPEN NEW =====

    if (desired !== 'FLAT') {
      console.log("Opening new position:", desired);

      await this.sendOrder({
        market,
        side: desired === 'LONG' ? OrderSide.BUY : OrderSide.SELL,
        size: alert.size,
        reduceOnly: false
      });
    }
  }

  // ================= ORDER SENDER =================

  private async sendOrder(params: {
    market: string;
    side: OrderSide;
    size: number;
    reduceOnly: boolean;
  }): Promise<void> {

    const { market, side, size, reduceOnly } = params;

    const clientId = parseInt(
      crypto.randomBytes(4).toString('hex'),
      16
    );

    console.log("Sending order:", {
      market,
      side,
      size,
      reduceOnly
    });

    const result = await this.client.placeOrder(
      this.subaccount,
      market,
      OrderType.MARKET,
      side,
      0, // price ignored for market
      size,
      clientId,
      OrderTimeInForce.GTT,
      120000,
      OrderExec









