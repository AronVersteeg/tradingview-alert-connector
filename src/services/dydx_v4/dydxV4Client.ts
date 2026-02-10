import {
  CompositeClient,
  Network,
  SubaccountClient,
  OrderSide,
  OrderType,
  OrderTimeInForce,
  OrderExecution
} from "@dydxprotocol/v4-client-js";
import crypto from "crypto";

type Alert = {
  exchange: string;
  strategy: string;
  market: string;
  desired_position: "LONG" | "SHORT" | "FLAT";
  price: number;
  time: number;
};

const processedSignals = new Set<string>();

export async function handleAlert(alert: Alert) {
  const signalId = `${alert.strategy}|${alert.market}|${alert.time}`;
  if (processedSignals.has(signalId)) return;
  processedSignals.add(signalId);

  const client = await CompositeClient.connect(Network.mainnet());
  const wallet = await SubaccountClient.fromEnv(0);

  const positions = await client.getSubaccountPositions(wallet);
  const pos = positions.find(p => p.market === alert.market.replace("_", "-"));

  const current =
    !pos ? "FLAT" :
    pos.size > 0 ? "LONG" : "SHORT";

  if (current === alert.desired_position) return;

  // ---- EXIT ----
  if (current !== "FLAT") {
    await placeOrder(
      client,
      wallet,
      alert.market,
      current === "LONG" ? OrderSide.SELL : OrderSide.BUY,
      Math.abs(pos.size),
      true,
      alert
    );
  }

  // ---- ENTRY ----
  if (alert.desired_position !== "FLAT") {
    await placeOrder(
      client,
      wallet,
      alert.market,
      alert.desired_position === "LONG" ? OrderSide.BUY : OrderSide.SELL,
      0.1,
      false,
      alert
    );
  }
}

async function placeOrder(
  client: any,
  wallet: any,
  market: string,
  side: OrderSide,
  size: number,
  reduceOnly: boolean,
  alert: Alert
) {
  const clientId = deterministicId(alert, side);

  await client.placeOrder(
    wallet,
    market.replace("_", "-"),
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
}

function deterministicId(alert: Alert, side: OrderSide) {
  const raw = `${alert.strategy}|${alert.market}|${alert.time}|${side}`;
  return parseInt(
    crypto.createHash("sha256").update(raw).digest("hex").slice(0, 8),
    16
  );
}






