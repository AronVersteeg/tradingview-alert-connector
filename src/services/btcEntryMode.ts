import fs from 'fs';
import path from 'path';

export const BTC_ENTRY_MODE_ENV = 'BTC_ENTRY_MODE';
export type BtcEntryMode = 'manual' | 'purple' | 'off';

export function btcEntryMode(): BtcEntryMode {
  const value = String(process.env[BTC_ENTRY_MODE_ENV] || 'manual').trim().toLowerCase();
  return value === 'manual' || value === 'purple' ? value : 'off';
}

export function btcEntryModeStatus() {
  const value = String(process.env[BTC_ENTRY_MODE_ENV] || 'manual').trim().toLowerCase();
  return { env: BTC_ENTRY_MODE_ENV, mode: btcEntryMode(), valid: ['manual', 'purple', 'off'].includes(value),
    default: 'manual', positionManagement: 'existing owner continues; switching never changes a live position' };
}

function readOwnerState(file: string): any {
  try {
    const state = JSON.parse(fs.readFileSync(file, 'utf8'));
    if (!state || typeof state !== 'object' || Array.isArray(state)) throw new Error('Invalid owner state.');
    return state;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return {};
    throw new Error('BTC entry ownership cannot be verified; existing state is unreadable. No new entry allowed.');
  }
}

// Read only at the entry boundary. Never clear another manager's position/intent.
export function btcCompanionEntryBlockReason(target: 'manual' | 'purple'): string | undefined {
  const explicitManual = String(process.env.DECENTRADER_GAP_ALERT_STATE_FILE || '').trim();
  const manualStateFile = explicitManual
    || path.join(process.cwd(), 'data', 'decentrader-gap-alert-state.json');
  const renderDisk = path.join(path.parse(process.cwd()).root, 'app', 'data');
  const base = explicitManual ? path.dirname(manualStateFile)
    : fs.existsSync(renderDisk) ? renderDisk : path.join(process.cwd(), 'data');
  const purpleStateFile = String(process.env.OPEN_LIQUIDITY_V2_BTC_INTRUSION_STATE_FILE || '').trim()
    || path.join(base, 'open-liquidity-v2-btc-intrusion-state.json');
  if (path.resolve(manualStateFile) === path.resolve(purpleStateFile)) {
    throw new Error('BTC manual and purple owner files must be distinct; no new entry allowed.');
  }
  const other = readOwnerState(target === 'manual' ? purpleStateFile : manualStateFile);
  if (other.managedPosition) return `The BTC ${target === 'manual' ? 'purple' : 'manual'} owner still manages a position; no addition, reversal or takeover.`;
  if (target === 'manual' && ['SUBMITTING', 'UNKNOWN'].includes(other.gapDailyEntry?.intent?.outcome)) {
    return 'An earlier BTC purple submission is still uncertain; no manual entry allowed.';
  }
  if (target === 'purple') {
    const plansFile = String(process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE || '').trim()
      || path.join(process.cwd(), 'data', 'btc-manual-trade-override.json');
    const plans = readOwnerState(plansFile);
    if (Object.keys(plans).length && (plans.market !== 'BTC-USD' ||
      !Array.isArray(plans.overrides) && plans.version !== 1)) {
      throw new Error('BTC manual plan ownership has an unexpected format; no new purple entry allowed.');
    }
    const rows = Array.isArray(plans.overrides) ? plans.overrides : [plans];
    if (rows.some(row => row.status === 'EXECUTING')) return 'A BTC manual execution remains unfinished; inspect/reconcile it before enabling another entry.';
  }
  return undefined;
}
