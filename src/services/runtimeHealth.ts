import v8 from 'v8';

// Liveness only: an upstream outage must not cause a trading-process restart loop.
export class RuntimeHealth {
  private timer?: NodeJS.Timeout;
  snapshot() {
    const usage = process.memoryUsage(), heapLimit = v8.getHeapStatistics().heap_size_limit;
    const mb = (bytes: number) => Math.round(bytes / 1024 / 1024 * 10) / 10;
    return { ok: true, service: 'tradingview-alert-connector',
      uptimeSeconds: Math.floor(process.uptime()), pid: process.pid, nodeVersion: process.version,
      supervised: process.env.SERVICE_SUPERVISED === '1',
      restarts: Number(process.env.SERVICE_RESTART_COUNT || 0),
      memoryMb: { rss: mb(usage.rss), heapUsed: mb(usage.heapUsed), heapTotal: mb(usage.heapTotal),
        external: mb(usage.external), arrayBuffers: mb(usage.arrayBuffers || 0), heapLimit: mb(heapLimit) },
      heapPressure: heapLimit > 0 && usage.heapUsed / heapLimit >= 0.85 };
  }
  private fatal = (error: Error, origin: string) => {
    console.error('Runtime uncaught exception; process will terminate:', { name: error.name, origin, ...this.snapshot() });
  };
  start() {
    if (this.timer) return;
    const report = () => {
      const status = this.snapshot();
      if (status.heapPressure) console.warn('Runtime memory pressure:', status);
      else console.log('Runtime health:', status);
    };
    report();
    this.timer = setInterval(report, 60_000);
    this.timer.unref?.();
    // Observe failures without swallowing them or continuing an unsafe process.
    process.on('uncaughtExceptionMonitor', this.fatal);
  }
  stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    process.removeListener('uncaughtExceptionMonitor', this.fatal);
  }
}
