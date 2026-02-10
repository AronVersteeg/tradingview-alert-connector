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

	// ===== REQUIRED BY AbstractDexClient =====
	async getIsAccountReady(): Promise<boolean> {
		return true;
	}

	// ===== MAIN ENTRY POINT =====
	async placeOrder(alert: AlertObject) {

		// ---------- IDEMPOTENCY ----------
		const signalId = `${alert.strategy}|${alert.market}|${alert.time}`;
		if (processedSignals.has(signalId)) {
			console.log('Duplicate signal ignored:', signalId);
			return;
		}
		processedSignals.add(signalId);

		// ---------- CLIENTS ----------
		const indexer = this.buildIndexerClient();
		const { client, subaccount } = await this.buildCompositeClient();
		const wallet = await this.generateLocalWallet();
		if (!wallet) return;

		// ---------- CURRENT POSITION ----------
		const orders = await indexer.account.getSubaccountOrders(wallet.address, 0);
		const open = orders.find(o => o.status === 'OPEN');

		let current: 'LONG' | 'SHORT' | 'FLAT' = 'FLAT';
		if (open) current = open.side === 'BUY' ? 'LONG' : 'SHORT';

		const desired = alert.desired_position;
		if (current === desired) {
			console.log('Already in desired position:', desired);
			return;
		}

		// ---------- EXIT ----------
		if (current !== 'FLAT' && open) {
			await this.sendOrder({
				alert,
				client,
				subaccount,
				side: current === 'LONG' ? OrderSide.SELL : OrderSide.BUY,
				size: Math.abs(open.size),
				reduceOnly: true
			});
		}

		// ---------- ENTRY ----------
		if (desired !== 'FLAT') {
			await this.sendOrder({
				alert,
				client,
				subaccount,
				side: desired === 'LONG' ? OrderSide.BUY : OrderSide.SELL,
				size: alert.size,
				reduceOnly: false
			});
		}
	}

	// ===== ORDER SENDER =====
	private async sendOrder(params: {
		alert: AlertObject;
		client: CompositeClient;
		subaccount: SubaccountClient;
		side: OrderSide;
		size: number;
		reduceOnly: boolean;
	}) {
		const { alert, client, subaccount, side, size, reduceOnly } = params;
		const clientId = this.deterministicClientId(alert, side);

		await client.placeOrder(
			subaccount,
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

		await _sleep(1000);
	}

	// ===== HELPERS =====
	private deterministicClientId(alert: AlertObject, side: OrderSide): number {
		const raw = `${alert.strategy}|${alert.market}|${alert.time}|${side}`;
		return parseInt(
			crypto.createHash('sha256').update(raw).digest('hex').slice(0, 8),
			16
		);
	}

	private async buildCompositeClient() {
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

		const client = await CompositeClient.connect(network);
		const wallet = await this.generateLocalWallet();
		if (!wallet) throw new Error('Wallet not found');

		const subaccount = new SubaccountClient(wallet, 0);
		return { client, subaccount };
	}

	private async generateLocalWallet() {
		if (!process.env.DYDX_V4_MNEMONIC) return;
		return LocalWallet.fromMnemonic(process.env.DYDX_V4_MNEMONIC, BECH32_PREFIX);
	}

	private buildIndexerClient() {
		const cfg =
			process.env.NODE_ENV === 'production'
				? this.getIndexerConfig()
				: Network.testnet().indexerConfig;
		return new IndexerClient(cfg);
	}

	private getIndexerConfig() {
		return new IndexerConfig(
			config.get('DydxV4.IndexerConfig.httpsEndpoint'),
			config.get('DydxV4.IndexerConfig.wssEndpoint')
		);
	}
}








