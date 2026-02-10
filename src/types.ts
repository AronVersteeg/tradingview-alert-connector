import {
	OrderSide,
	OrderType,
	TimeInForce,
	Market
} from '@dydxprotocol/v3-client';
import { PositionSide } from '@perp/sdk-curie';
import { gmxOrderType } from './services/gmx/constants';
import { OrderSide as v4OrderSide } from '@dydxprotocol/v4-client-js';

// =======================
// ALERT OBJECT (INTENT-BASED)
// =======================
export type AlertObject = {
	exchange: string;
	strategy: string;
	market: string;
	price: number;
	size?: number;
	sizeUsd?: number;
	sizeByLeverage?: number;
	time: number;
	desired_position: 'LONG' | 'SHORT' | 'FLAT';
	passphrase?: string;
};

// =======================
// DYDX V3
// =======================
export type dydxOrderParams = {
	market: Market;
	side: OrderSide;
	type: OrderType.MARKET;
	timeInForce: TimeInForce.FOK;
	postOnly: false;
	size: string;
	price: string;
	limitFee: string;
	expiration: string;
};

// =======================
// DYDX V4
// =======================
export type dydxV4OrderParams = {
	market: string;
	side: v4OrderSide;
	size: number;
	price: number;
};

// =======================
// PERP
// =======================
export type perpOrderParams = {
	tickerSymbol: string;
	side: PositionSide;
	amountInput: number;
	isAmountInputBase: boolean;
	referralCode: string;
};

// =======================
// GMX
// =======================
export type gmxOrderParams = {
	marketAddress: string;
	isLong: boolean;
	sizeUsd: number;
	price: number;
	collateral?: string;
};

export type gmxOrderResult = {
	txHash: string;
	sizeUsd: number;
	isLong: boolean;
};

export type GmxPositionResponse = {
	orderType: gmxOrderType;
	hasLongPosition?: boolean;
	positionSizeUsd?: number;
	collateralAmount?: number;
};

// =======================
// GENERIC RESULT
// =======================
export interface OrderResult {
	size: number;
	side: string;
	orderId: string;
}

