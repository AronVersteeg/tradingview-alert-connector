import { pressureMetricsFromHourly } from '../src/services/snoekWeather';

function hourlyTimes(startIso: string, count: number): string[] {
  const start = new Date(`${startIso}Z`).getTime();
  return Array.from({ length: count }, (_, index) => (
    new Date(start + index * 60 * 60 * 1000).toISOString().slice(0, 16)
  ));
}

describe('pressureMetricsFromHourly', () => {
  it('calculates a sharp 24-hour pressure drop and the short forecast trend', () => {
    const time = hourlyTimes('2026-09-21T12:00', 28);
    const pressure = Array(28).fill(1014);
    pressure[0] = 1020.5;
    pressure[27] = 1012.9;

    expect(pressureMetricsFromHourly(
      { time, pressure_msl: pressure },
      1014,
      time[24]
    )).toEqual({
      trend: 'falling',
      change24hHpa: -6.5
    });
  });

  it('leaves the 24-hour change empty when history is unavailable', () => {
    const time = hourlyTimes('2026-09-22T12:00', 4);

    expect(pressureMetricsFromHourly(
      { time, pressure_msl: [1014, 1014.2, 1014.4, 1014.5] },
      1014,
      time[0]
    )).toEqual({
      trend: 'steady',
      change24hHpa: null
    });
  });
});
