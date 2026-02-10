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

  // ---------- PASSPHRASE ----------
  if (process.env.TRADINGVIEW_PASSPHRASE) {
    if (!alertMessage.passphrase) {
      console.error('Passphrase is missing in alert message.');
      return false;
    }
    if (alertMessage.passphrase !== process.env.TRADINGVIEW_PASSPHRASE) {
      console.error('Tradingview passphrase does not match.');
      return false;
    }
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
  // ✅ INTENT-BASED ALERT (NEW LOGIC)
  // =====================================================
  if (alertMessage.desired_position) {
    const allowed = ['LONG', 'SHORT', 'FLAT'];
    if (!allowed.includes(alertMessage.desired_position)) {
      console.error(
        'desired_position must be one of LONG | SHORT | FLAT'
      );
      return false;
    }

    // intent-alerts zijn altijd geldig
    return true;
  }

  // =====================================================
  // ⚠️ LEGACY ORDER-BASED ALERT (OLD LOGIC)
  // =====================================================

  // check order side
  if (alertMessage.order !== 'buy' && alertMessage.order !== 'sell') {
    console.error(
      'Side field of tradingview alert is not correct. Must be buy or sell'
    );
    return false;
  }

  // check position
  if (
    alertMessage.position !== 'long' &&
    alertMessage.position !== 'short' &&
    alertMessage.position !== 'flat'
  ) {
    console.error('Position field of tradingview alert is not correct.');
    return false;
  }

  // check reverse
  if (typeof alertMessage.reverse !== 'boolean') {
    console.error(
      'Reverse field of tradingview alert must be boolean.'
    );
    return false;
  }

  // ---------- STRATEGY STATE (LEGACY) ----------
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

