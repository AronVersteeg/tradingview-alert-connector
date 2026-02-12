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

  async init(): Promise<void> {
    console.log("Initializing dYdX V4 client...");

    this.wallet = await LocalWallet.fromMnemonic(
      process.env.DYDX_V4_MNEMONIC!,
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

    this.indexer = new IndexerClient(
      process.env.NODE_ENV === 'production'
        ? this.getIndexerConfig()
        : Network.testnet().indexerConfig
    );

    this.initialized = true;
    console.log("dYdX V4 client initialized.");
  }

  async getIsAccountReady(): Promise<boolean> {
    return this.initialized;
  }

  async placeOrder(alert: AlertObject): Promise<void> {

    const market = alert.market.replace(/_/g, '-');
    const desired = alert.desired_position;

    console.log("===== NEW SIGNAL =====");
    console.log("Alert:", alert);

    const response = await this.indexer.account.getSubaccountPerpetualPositions(
      this.wallet.address,
      0
    );

    const positions = response?.positions || [];

    const openPosition = positions.find((p: any) =>
      p.market === market &&
      p.status === 'OPEN' &&
      Number(p.size) !== 0
    );

    let current: 'LONG' | 'SHORT' | 'FLAT' = 'FLAT';
    let currentSize = 0;

    if (openPosition) {
      currentSize = Number(openPosition.size);
      if (currentSize > 0) current = 'LONG';
      if (currentSize < 0) current = 'SHORT';
    }

    console.log("Detected current:", current, "Size:", currentSize);

    if (current === desired) {
      console.log("Already correct direction.");
      return;
    }

    i

