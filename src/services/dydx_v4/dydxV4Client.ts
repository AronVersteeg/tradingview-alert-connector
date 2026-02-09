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
import { _sleep, doubleSizeIfReverseOrder } from '../../helper';
import 'dotenv/config';
import config from 'config';
import crypto from 'crypto';
import { AbstractDexClient } from '../abstractDexClient';

export class DydxV4Client extends AbstractDexClient {

	// =========================
	// ACCOUNT
	// =========================

	async getIsAccountReady() {
		const subAccount = await this.getSubAccount();
		if (!subAccount) return false;
		return Number(subAccount.freeCollateral) > 0;
	}

	async getSubAccount() {
		try {
			const client = this.buildIndexerClient();
			const localWallet = await this.generateLocalWallet();
			if (!localWallet) return;
			const response = await client.account.getSubaccount(
				localWallet.address,
				0
			);
			return response.subaccount;
		} catch (error) {
			console.error(error);
		}
	}

	// =========================
	// ORDER PARAMS
	// =========================

	async buildOrderParams(alert: AlertObject) {
		const side =
			alert.order === 'buy' ? OrderSide.BUY : OrderSide.SELL;

		const price = Number(alert.price);

		let size: number;
		if (alert.sizeByLeverage) {
			const account = await this.getSubAccount();
			size =
				(Number(account.equity) * Number(alert.sizeByLeverage)) /
				price;
		} else if (alert.sizeUsd) {
			size = Number(alert.sizeUsd) / price;
		} else {
			size = Number(alert.size);
		}

		size = doubleSizeIfReverseOrder(alert, size);

		return {
			market: alert.market.replace(/_/g, '-'),
			side,
			size,
			price
		} as dydxV4OrderParams;
	}

	// =========================
	// PLACE ORDER (IDEMPOTENT)
	// =========================

	async placeOrder(alert: AlertObject) {
		const params = await this.buildOrderParams(alert);
		const { client, subaccount } = await this.buildCompositeClient();

		// 🔑 DETERMINISTIC CLIENT ID
		const clientId = this.generateDeterministicClientId(alert);
		console.log('ClientId (deterministic):', clientId);

		const slippage = 0.05;
		const execPrice =
			params.side === OrderSide.BUY
				? params.price * (1 + slippage)
				: params.price * (1 - slippage);

		const maxTries = 3;
		let attempt = 0;

		while (attempt <= maxTries) {
			try {
				const tx = await client.placeOrder(
					subaccount,
					params.market,
					OrderType.MARKET,
					params.side,
					execPrice,
					params.size,
					clientId,
					OrderTimeInForce.GTT,
					120000,
					OrderExecution.DEFAULT,
					false, // postOnly
					false, // reduceOnly
					null
				);

				console.log('TX result:', tx);

				await _sleep(60000);

				const filled = await this.isOrderFilled(String(clientId));
				if (!filled) throw new Error('Order not filled yet');

				const result: OrderResult = {
					side: params.side,
					size: params.size,
					orderId: String(clientId)
				};

				await this.exportOrder(
					'DydxV4',
					alert.strategy,
					result,
					alert.price,
					alert.market
				);

				return result;

			} catch (err) {
				console.error(err);
				attempt++;
				if (attempt > maxTries) throw err;
				await _sleep(5000);
			}
		}
	}

	// =========================
	// CLIENT BUILDERS
	// =========================

	private buildCompositeClient = async () => {
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
		const subaccount = new SubaccountClient(wallet, 0);

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
		const cfg =
			process.env.NODE_ENV === 'production'
				? this.getIndexerConfig()
				: Network.testnet().indexerConfig;
		return new IndexerClient(cfg);
	};

	private getIndexerConfig = () =>
		new IndexerConfig(
			config.get('DydxV4.IndexerConfig.httpsEndpoint'),
			config.get('DydxV4.IndexerConfig.wssEndpoint')
		);

	// =========================
	// 🔑 CLIENT ID (KEY FIX)
	// =========================

	private generateDeterministicClientId(alert: AlertObject): number {
		const raw = `${alert.strategy}|${alert.market}|${alert.order}|${alert.time}`;
		const hash = crypto.createHash('sha256').update(raw).digest('hex');
		return parseInt(hash.substring(0, 8), 16);
	}

	// =========================
	// ORDER CHECKS
	// =========================

	private isOrderFilled = async (clientId: string): Promise<boolean> => {
		const orders = await this.getOrders();
		const order = orders.find(o => o.clientId === clientId);
		return order?.status === 'FILLED';
	};

	getOrders = async () => {
		const client = this.buildIndexerClient();
		const wallet = await this.generateLocalWallet();
		return client.account.getSubaccountOrders(wallet.address, 0);
	};
}





