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
    expect(result.dataSources.some((source) => source.id === 'kadaster-brt-top10nl')).toBe(true);
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

  it('prioritizes local current spots for snoekbaars dropshot sessions', () => {
    const result = buildSnoekScout({
      target: 'snoekbaars',
      temperatureC: 18,
      windBft: 3,
      cloudCoverPct: 70,
      pressureTrend: 'falling',
      rain: 'none',
      timeOfDay: 'evening'
    });

    expect(result.score).toBeGreaterThanOrEqual(75);
    expect(result.spots[0].id).toBe('pontje-velsen-zuid');
    expect(result.tactics.join(' ')).toContain('dropshot');
    expect(result.communityReviews.some((review) => review.source === 'Engelhart Hengelsport advies')).toBe(true);
  });

  it('places De Ven on the Velsen-Zuid model boat lake', () => {
    const result = buildSnoekScout({ target: 'method_feeder' });
    const deVen = result.spots.find((spot) => spot.id === 'de-ven');
    const buitenhuizerplas = result.spots.find((spot) => spot.id === 'buitenhuizerplas');
    const schoonenberg = result.spots.find((spot) => spot.id === 'park-schoonenberg');

    expect(deVen).toMatchObject({
      lat: 52.4549,
      lon: 4.6642,
      area: 'De Ven, Velsen-Zuid'
    });
    expect(buitenhuizerplas).toMatchObject({ lat: 52.42914, lon: 4.70786 });
    expect(schoonenberg).toMatchObject({ lat: 52.4523246, lon: 4.6356894 });
  });

  it('uses surveyed coordinates for the local current and lock spots', () => {
    const result = buildSnoekScout({ target: 'snoekbaars' });

    expect(result.spots.find((spot) => spot.id === 'pontje-velsen-zuid'))
      .toMatchObject({ lat: 52.4626581, lon: 4.6323097 });
    expect(result.spots.find((spot) => spot.id === 'pontje-buitenhuizen'))
      .toMatchObject({ lat: 52.433, lon: 4.7255 });
    expect(result.spots.find((spot) => spot.id === 'sluis-spaarndam'))
      .toMatchObject({ lat: 52.4129566, lon: 4.6814088 });
  });
});
