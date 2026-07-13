import { AlertObject } from '../types';
import { getStrategiesDB } from '../helper';
import { DexRegistry } from './dexRegistry';

export const validateAlert = async (
  alertMessage: AlertObject
): Promise<boolean> => {

  // ---------- BASIC CHECK ----------
  if (!Object.keys(alertMessage).length) {
    console.error('Tradingview alert is empty or not JSON.');
    return false;
  }

  // ---------- WEBHOOK AUTH ----------
  const expectedToken = String(
    process.env.WEBHOOK_TOKEN ||
    process.env.TRADINGVIEW_PASSPHRASE ||
    ''
  ).trim();
  const receivedToken = String(
    alertMessage.passphrase ||
    (alertMessage as any).token ||
    ''
  ).trim();

  if (!expectedToken) {
    console.error('Webhook rejected because WEBHOOK_TOKEN/TRADINGVIEW_PASSPHRASE is not configured.');
    return false;
  }
  if (!receivedToken || receivedToken !== expectedToken) {
    console.error('Webhook token is missing or does not match.');
    return false;
  }

  // ---------- EXCHANGE ----------
  if (alertMessage.exchange) {
    const validExchanges = new DexRegistry().getAllDexKeys();
    if (!validExchanges.includes(alertMessage.exchange)) {
      console.error('Exchange is not supported:', alertMessage.exchange);
      return false;
    }
  }

  // ---------- STRATEGY ----------
  if (!alertMessage.strategy) {
    console.error('Strategy field must not be empty.');
    return false;
  }

  // =====================================================
  // ✅ INTENT-BASED ALERT (UPDATED)
  // =====================================================

  if (alertMessage.desired_position) {

    const pos = alertMessage.desired_position.toUpperCase();

    const allowed = [
      'LONG',
      'SHORT',
      'FLAT',
      'BUY',
      'SELL'
    ];

    if (!allowed.includes(pos)) {
      console.error(
        'desired_position must be one of LONG | SHORT | FLAT | BUY | SELL'
      );
      return false;
    }

    return true;
  }

  // =====================================================
  // ⚠️ LEGACY ORDER-BASED ALERT (OLD LOGIC)
  // =====================================================

  if (alertMessage.order !== 'buy' && alertMessage.order !== 'sell') {
    console.error(
      'Side field of tradingview alert is not correct. Must be buy or sell'
    );
    return false;
  }

  if (
    alertMessage.position !== 'long' &&
    alertMessage.position !== 'short' &&
    alertMessage.position !== 'flat'
  ) {
    console.error('Position field of tradingview alert is not correct.');
    return false;
  }

  if (typeof alertMessage.reverse !== 'boolean') {
    console.error(
      'Reverse field of tradingview alert must be boolean.'
    );
    return false;
  }

  const [db, rootData] = getStrategiesDB();
  const rootPath = '/' + alertMessage.strategy;

  if (!rootData[alertMessage.strategy]) {
    db.push(rootPath + '/reverse', alertMessage.reverse);
    db.push(rootPath + '/isFirstOrder', 'true');
  }

  if (
    alertMessage.position === 'flat' &&
    rootData[alertMessage.strategy]?.isFirstOrder === 'true'
  ) {
    console.log(
      'First alert is flat → ignoring close without open.'
    );
    return false;
  }

  return true;
};


