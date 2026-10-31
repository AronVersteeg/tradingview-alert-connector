import fs from 'fs';
import path from 'path';
import { PurpleLineMarket, PURPLE_LINE_SETUPS } from './purpleLineSetup';

const MAX_GAP_MS = 5 * 60_000;
type Lease = { version: 1; market: string; modelVersion: string; active: boolean;
  activatedAt: string; observedAt: string };

// A small durable heartbeat, separate from large chart/research state. It proves
// that the model was already armed before a short interruption, not a new entry.
export class PurpleLineContinuity {
  private timer?: NodeJS.Timeout;
  private activatedAt?: string;
  private lastSeen?: number;
  private status: any = { ready: false };
  constructor(private market: PurpleLineMarket, private file: string, private enabled: () => boolean) {}

  start() {
    if (this.timer) return;
    this.heartbeat();
    this.timer = setInterval(() => this.heartbeat(), 60_000);
    this.timer.unref?.();
  }
  stop() { if (this.timer) clearInterval(this.timer); this.timer = undefined; }
  snapshot() { return { ...this.status }; }
  activation(): string | undefined {
    const now = Date.now();
    return this.status.ready && this.enabled() && this.lastSeen !== undefined &&
      now >= this.lastSeen && now - this.lastSeen <= MAX_GAP_MS ? this.activatedAt : undefined;
  }

  heartbeat() {
    const now = Date.now(), config = PURPLE_LINE_SETUPS[this.market];
    try {
      if (!this.enabled()) {
        this.activatedAt = undefined;
        this.status = { ready: false, reason: 'ENTRY_DISABLED' };
      } else if (!this.activatedAt || this.lastSeen !== undefined && (now < this.lastSeen || now - this.lastSeen > MAX_GAP_MS)) {
        let previous: Lease | undefined;
        try {
          if (fs.statSync(this.file).size <= 4096) previous = JSON.parse(fs.readFileSync(this.file, 'utf8'));
        } catch { /* Missing/invalid continuity never grants retrospective eligibility. */ }
        const seen = Date.parse(previous?.observedAt || ''), activated = Date.parse(previous?.activatedAt || '');
        const resume = previous?.version === 1 && previous.market === this.market && previous.modelVersion === config.modelVersion &&
          previous.active === true && Number.isFinite(activated) && activated <= seen && seen <= now && now - seen <= MAX_GAP_MS;
        this.activatedAt = resume ? previous!.activatedAt : new Date(now).toISOString();
        this.status = { ready: false, resumedAfterRestart: resume, reason: resume ? 'SHORT_INTERRUPTION_RESUMED' : 'NEW_ACTIVATION',
          previousHeartbeatAt: previous?.observedAt, maxInterruptionSeconds: MAX_GAP_MS / 1000 };
      }
      const record: Lease = { version: 1, market: this.market, modelVersion: config.modelVersion,
        active: Boolean(this.activatedAt), activatedAt: this.activatedAt || new Date(now).toISOString(), observedAt: new Date(now).toISOString() };
      fs.mkdirSync(path.dirname(this.file), { recursive: true });
      const temporary = `${this.file}.${process.pid}.tmp`;
      let fd: number | undefined;
      try {
        fd = fs.openSync(temporary, 'w'); fs.writeFileSync(fd, JSON.stringify(record)); fs.fsyncSync(fd);
        fs.closeSync(fd); fd = undefined; fs.renameSync(temporary, this.file);
      } finally {
        if (fd !== undefined) fs.closeSync(fd);
        try { fs.unlinkSync(temporary); } catch { /* A successful rename already removed it. */ }
      }
      this.lastSeen = now;
      this.status = { ...this.status, ready: Boolean(this.activatedAt), activatedAt: this.activatedAt, lastHeartbeatAt: record.observedAt };
    } catch (error) {
      this.status = { ready: false, reason: 'CONTINUITY_WRITE_FAILED', error: error instanceof Error ? error.message : String(error) };
      console.error(`${this.market} entry continuity unavailable; restart recovery will not assume eligibility:`, this.status);
    }
  }
}
