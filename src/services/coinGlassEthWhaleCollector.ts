import fs from 'fs';
import path from 'path';

import {
  CoinGlassWhaleHistoryLevel,
  CoinGlassWhaleLevel,
  CoinGlassWhaleObservation,
  CoinGlassWhaleObservationLevel,
  CoinGlassWhaleSnapshot,
  fetchCoinGlassWhaleLevelsViaWebSocket
} from './decentraderGapMonitor';

type CoinGlassWhaleCollectorConfig = {
  asset: 'ETH' | 'INJ';
  defaultSymbol: string;
  defaultMinUsd: number;
  defaultStrongUsd: number;
  historySuffix: string;
};

const ETH_CONFIG: CoinGlassWhaleCollectorConfig = {
  asset: 'ETH',
  defaultSymbol: 'Binance_ETHUSDT',
  defaultMinUsd: 10_000_000,
  defaultStrongUsd: 20_000_000,
  historySuffix: 'eth'
};

const INJ_CONFIG: CoinGlassWhaleCollectorConfig = {
  asset: 'INJ',
  defaultSymbol: 'Binance_INJUSDT',
  defaultMinUsd: 250_000,
  defaultStrongUsd: 1_000_000,
  historySuffix: 'inj'
};

type ObservationContext = {
  frameTimestamp?: string;
  currentPrice?: number;
  gap?: { left: number; right: number; width: number } | null;
};

function boolEnv(name: string, fallback: boolean): boolean {
  const value = process.env[name];
  if (value === undefined || value === '') return fallback;
  return String(value).trim().toLowerCase() === 'true';
}

function numberEnv(name: string, fallback: number): number {
  const value = Number(process.env[name]);
  return Number.isFinite(value) && value >= 0 ? value : fallback;
}

function positiveIntegerEnv(name: string, fallback: number, minimum: number, maximum: number): number {
  const value = Number(process.env[name]);
  return Number.isFinite(value) && value > 0
    ? Math.max(minimum, Math.min(maximum, Math.floor(value)))
    : fallback;
}

function levelKey(level: CoinGlassWhaleLevel): string {
  if (level.key) return level.key;
  const price = Number(level.price).toFixed(8).replace(/0+$/, '').replace(/\.$/, '');
  return `${level.instrument || level.symbol}|${level.side}|${price}`;
}

function normalizedTimestampMs(value: number | undefined, fallbackMs: number): number {
  if (!Number.isFinite(value) || (value as number) <= 0) return fallbackMs;
  return (value as number) < 1_000_000_000_000 ? (value as number) * 1000 : (value as number);
}

export class CoinGlassEthWhaleCollector {
  private interval: NodeJS.Timeout | undefined;
  private initialTimer: NodeJS.Timeout | undefined;
  private refreshPromise: Promise<void> | undefined;
  private loaded = false;
  private levels: CoinGlassWhaleLevel[] = [];
  private history: CoinGlassWhaleHistoryLevel[] = [];
  private observations: CoinGlassWhaleObservation[] = [];
  private historyUpdatedAt: string | undefined;
  private fetchedAt: number | undefined;
  private lastAttemptAt: number | undefined;
  private error: string | undefined;
  private observationProvider: (() => Promise<ObservationContext>) | undefined;

  constructor(private readonly config: CoinGlassWhaleCollectorConfig = ETH_CONFIG) {}

  private env(suffix: string): string {
    return `COINGLASS_WHALE_${this.config.asset}_${suffix}`;
  }

  private enabled(): boolean {
    return boolEnv('COINGLASS_WHALE_LEVELS_ENABLED', true) &&
      boolEnv(this.env('ENABLED'), true);
  }

  private symbol(): string {
    return String(process.env[this.env('SYMBOL')] || this.config.defaultSymbol).trim() || this.config.defaultSymbol;
  }

  private intervalName(): string {
    return String(process.env[this.env('INTERVAL')] || process.env.COINGLASS_WHALE_INTERVAL || 'm1').trim() || 'm1';
  }

  private minUsd(): number {
    const fallback = this.config.asset === 'ETH'
      ? numberEnv('COINGLASS_WHALE_LEVEL_MIN_USD', this.config.defaultMinUsd)
      : this.config.defaultMinUsd;
    return numberEnv(
      this.env('LEVEL_MIN_USD'),
      fallback
    );
  }

  private strongUsd(): number {
    const fallback = this.config.asset === 'ETH'
      ? numberEnv('COINGLASS_WHALE_LEVEL_STRONG_USD', this.config.defaultStrongUsd)
      : this.config.defaultStrongUsd;
    return Math.max(
      this.minUsd(),
      numberEnv(this.env('LEVEL_STRONG_USD'), fallback)
    );
  }

  private timeoutMs(): number {
    return positiveIntegerEnv(
      this.env('TIMEOUT_MS'),
      positiveIntegerEnv('COINGLASS_WHALE_TIMEOUT_MS', 12_000, 3_000, 30_000),
      3_000,
      30_000
    );
  }

  private pollMs(): number {
    const minutes = numberEnv(
      this.env('POLL_MINUTES'),
      numberEnv('COINGLASS_WHALE_POLL_MINUTES', 10)
    );
    return Math.max(60_000, Math.max(1, minutes) * 60_000);
  }

  private retentionHours(): number {
    return positiveIntegerEnv('COINGLASS_WHALE_HISTORY_RETENTION_HOURS', 720, 24, 2_160);
  }

  private historyMaxRecords(): number {
    return positiveIntegerEnv('COINGLASS_WHALE_HISTORY_MAX_RECORDS', 1_500, 100, 5_000);
  }

  private observationMaxRecords(): number {
    return positiveIntegerEnv('COINGLASS_WHALE_OBSERVATION_MAX_RECORDS', 1_000, 100, 2_500);
  }

  historyFile(): string {
    const explicit = String(process.env[this.env('HISTORY_FILE')] || '').trim();
    if (explicit) return explicit;

    const btcFile = String(process.env.COINGLASS_WHALE_HISTORY_FILE || '').trim();
    if (btcFile) {
      const extension = path.extname(btcFile) || '.json';
      return path.join(path.dirname(btcFile), `${path.basename(btcFile, extension)}-${this.config.historySuffix}${extension}`);
    }

    const renderDisk = path.join(path.parse(process.cwd()).root, 'app', 'data');
    const base = fs.existsSync(renderDisk) ? renderDisk : path.join(process.cwd(), 'data');
    return path.join(base, `coinglass-whale-history-${this.config.historySuffix}.json`);
  }

  private load(): void {
    if (this.loaded) return;
    this.loaded = true;
    try {
      const parsed = JSON.parse(fs.readFileSync(this.historyFile(), 'utf8'));
      this.historyUpdatedAt = typeof parsed?.updatedAt === 'string' ? parsed.updatedAt : undefined;
      this.history = Array.isArray(parsed?.levels) ? parsed.levels : [];
      this.observations = Array.isArray(parsed?.observations) ? parsed.observations : [];
    } catch {
      this.history = [];
      this.observations = [];
    }
  }

  private persist(updatedAt: string): void {
    const file = this.historyFile();
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const temporary = `${file}.${process.pid}.tmp`;
    fs.writeFileSync(temporary, JSON.stringify({
      updatedAt,
      levels: this.history,
      observations: this.observations
    }));
    fs.renameSync(temporary, file);
    this.historyUpdatedAt = updatedAt;
  }

  private mergeHistory(levels: CoinGlassWhaleLevel[]): void {
    this.load();
    const nowMs = Date.now();
    const nowIso = new Date(nowMs).toISOString();
    const byKey = new Map<string, CoinGlassWhaleHistoryLevel>();
    for (const level of this.history) byKey.set(level.key, { ...level, active: false });

    for (const level of levels) {
      const key = levelKey(level);
      const previous = byKey.get(key);
      const firstSeenMs = normalizedTimestampMs(
        level.startedAt,
        previous?.firstSeenAt ? Date.parse(previous.firstSeenAt) : nowMs
      );
      const volumeUsd = Number(level.volumeUsd) || 0;
      byKey.set(key, {
        ...previous,
        ...level,
        key,
        firstSeenAt: Number.isFinite(firstSeenMs)
          ? new Date(firstSeenMs).toISOString()
          : previous?.firstSeenAt || nowIso,
        firstObservedAt: previous?.firstObservedAt || nowIso,
        lastSeenAt: nowIso,
        maxVolumeUsd: Math.max(volumeUsd, Number(previous?.maxVolumeUsd) || 0, Number(previous?.volumeUsd) || 0),
        active: true
      });
    }

    const cutoff = nowMs - this.retentionHours() * 3_600_000;
    this.history = [...byKey.values()]
      .filter((level) => Date.parse(level.lastSeenAt) >= cutoff)
      .sort((a, b) => Date.parse(b.lastSeenAt) - Date.parse(a.lastSeenAt) || b.maxVolumeUsd - a.maxVolumeUsd)
      .slice(0, this.historyMaxRecords());
    this.persist(nowIso);
  }

  start(initialDelayMs = 0): void {
    if (!this.enabled() || this.interval || this.initialTimer) return;
    this.load();
    const begin = () => {
      this.initialTimer = undefined;
      this.refresh('startup').catch(() => undefined);
      this.interval = setInterval(() => this.refresh('poll').catch(() => undefined), this.pollMs());
    };
    if (initialDelayMs > 0) this.initialTimer = setTimeout(begin, initialDelayMs);
    else begin();
    console.log(`CoinGlass ${this.config.asset} whale collector started:`, {
      symbol: this.symbol(),
      interval: this.intervalName(),
      minUsd: this.minUsd(),
      pollMinutes: this.pollMs() / 60_000,
      historyFile: this.historyFile()
    });
  }

  configureObservationProvider(provider: () => Promise<ObservationContext>): void {
    this.observationProvider = provider;
  }

  async refresh(reason: string): Promise<void> {
    if (!this.enabled()) return;
    if (this.refreshPromise) return this.refreshPromise;
    this.refreshPromise = this.refreshInternal(reason).finally(() => {
      this.refreshPromise = undefined;
    });
    return this.refreshPromise;
  }

  private async refreshInternal(reason: string): Promise<void> {
    this.lastAttemptAt = Date.now();
    try {
      const levels = await fetchCoinGlassWhaleLevelsViaWebSocket(
        this.symbol(),
        this.intervalName(),
        this.minUsd(),
        this.timeoutMs()
      );
      this.levels = levels;
      this.fetchedAt = Date.now();
      this.error = undefined;
      this.mergeHistory(levels);
      if (this.observationProvider) {
        try {
          const context = await this.observationProvider();
          this.recordObservation(context.frameTimestamp, context.currentPrice, context.gap);
        } catch (error) {
          console.warn(`CoinGlass ${this.config.asset} whale observation context unavailable; level history remains stored.`, {
            reason,
            error: error instanceof Error ? error.message : String(error)
          });
        }
      }
      console.log(`CoinGlass ${this.config.asset} whale levels refreshed:`, {
        reason,
        symbol: this.symbol(),
        interval: this.intervalName(),
        minUsd: this.minUsd(),
        levels: levels.length,
        history: this.history.length
      });
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
      console.warn(`CoinGlass ${this.config.asset} whale levels refresh failed; using cached levels if available.`, {
        reason,
        symbol: this.symbol(),
        error: this.error
      });
    }
  }

  recordObservation(
    frameTimestamp: string | undefined,
    currentPrice: number | undefined,
    gap?: { left: number; right: number; width: number } | null
  ): void {
    this.load();
    if (!this.enabled() || !this.fetchedAt || !frameTimestamp || !Number.isFinite(currentPrice)) return;
    const observedAt = new Date().toISOString();
    const levels: CoinGlassWhaleObservationLevel[] = this.levels
      .map((level) => ({
        key: levelKey(level),
        side: level.side,
        price: level.price,
        volumeUsd: level.volumeUsd,
        startedAt: level.startedAt
      }))
      .sort((a, b) => a.price - b.price || b.volumeUsd - a.volumeUsd);
    const previous = this.observations[this.observations.length - 1];
    const previousByKey = new Map((previous?.levels || []).map((level) => [level.key, level]));
    const materiallyChanged = !previous || previous.levels.length !== levels.length || levels.some((level) => {
      const earlier = previousByKey.get(level.key);
      if (!earlier) return true;
      return earlier.side !== level.side ||
        Math.abs(earlier.price - level.price) >= 0.01 ||
        Math.abs(earlier.volumeUsd - level.volumeUsd) >= Math.max(1_000_000, earlier.volumeUsd * 0.02);
    });
    if (previous?.frameTimestamp === frameTimestamp && !materiallyChanged) return;

    this.observations = [...this.observations, {
      observedAt,
      frameTimestamp,
      currentPrice: currentPrice as number,
      gap: gap || undefined,
      levels
    }]
      .filter((item) => Date.parse(item.observedAt) >= Date.now() - this.retentionHours() * 3_600_000)
      .slice(-this.observationMaxRecords());
    this.persist(observedAt);
  }

  snapshot(): CoinGlassWhaleSnapshot {
    this.load();
    return {
      enabled: this.enabled(),
      source: 'coinglass',
      url: `https://www.coinglass.com/large-orderbook-statistics?symbol=${this.config.asset}`,
      symbol: this.symbol(),
      interval: this.intervalName(),
      minUsd: this.minUsd(),
      strongUsd: this.strongUsd(),
      fetchedAt: this.fetchedAt ? new Date(this.fetchedAt).toISOString() : undefined,
      lastAttemptAt: this.lastAttemptAt ? new Date(this.lastAttemptAt).toISOString() : undefined,
      error: this.error,
      levels: this.levels,
      history: this.history,
      observations: this.observations,
      historyUpdatedAt: this.historyUpdatedAt,
      historyRetentionHours: this.retentionHours()
    };
  }
}

export const coinGlassEthWhaleCollector = new CoinGlassEthWhaleCollector();
export const coinGlassInjWhaleCollector = new CoinGlassEthWhaleCollector(INJ_CONFIG);
