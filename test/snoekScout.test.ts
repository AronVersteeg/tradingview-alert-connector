import { buildSnoekScout } from '../src/services/snoekScout';

describe('buildSnoekScout', () => {
  it('scores cloudy falling-pressure snoek weather as a strong chance', () => {
    const result = buildSnoekScout({
      target: 'snoek',
      temperatureC: 16,
      windBft: 3,
      cloudCoverPct: 85,
      pressureTrend: 'falling',
      rain: 'light',
      timeOfDay: 'evening'
    });

    expect(result.score).toBeGreaterThanOrEqual(75);
    expect(result.spots.length).toBeGreaterThan(0);
    expect(result.mapFeatures.length).toBeGreaterThan(0);
    expect(result.dataSources.some((source) => source.id === 'esri-rws-bathymetry')).toBe(true);
    expect(result.communityReviews.length).toBeGreaterThan(0);
  });

  it('warns when warm weather makes snoek handling risky', () => {
    const result = buildSnoekScout({
      target: 'snoek',
      temperatureC: 27,
      windBft: 1,
      cloudCoverPct: 10,
      timeOfDay: 'midday'
    });

    expect(result.warnings.length).toBeGreaterThan(0);
    expect(result.score).toBeLessThan(60);
  });
});
