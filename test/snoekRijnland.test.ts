import {
  parseRijnlandPumpFeatures,
  parseRijnlandTemperatureFeatures
} from '../src/services/snoekRijnland';

describe('Rijnland Snoek map data', () => {
  it('groups temperature readings into a sorted depth profile', () => {
    const features = [
      {
        attributes: {
          featureIdentifier: '476-105-00001_oppervlaktewater_0200MINWS',
          name: 'Meetboei Spaarndam Zijkanaal C, 2.00m onder waterspiegel',
          value: 18.1,
          classification: '18 - 21',
          chartUrl: 'https://example.test/deep'
        },
        geometry: { x: 4.6823, y: 52.41406 }
      },
      {
        attributes: {
          featureIdentifier: '476-105-00001_oppervlaktewater_0050MINWS',
          name: 'Meetboei Spaarndam Zijkanaal C, 0.50m onder waterspiegel',
          value: 20.4,
          classification: '18 - 21',
          chartUrl: 'https://example.test/surface'
        },
        geometry: { x: 4.6823, y: 52.41406 }
      }
    ];

    const profiles = parseRijnlandTemperatureFeatures(features, '2026-09-02T12:00:00.000Z');

    expect(profiles).toHaveLength(1);
    expect(profiles[0]).toMatchObject({
      name: 'Meetboei Spaarndam Zijkanaal C',
      minC: 18.1,
      maxC: 20.4,
      rangeC: 2.3,
      depthHintM: 1.25
    });
    expect(profiles[0].readings.map((reading) => reading.depthM)).toEqual([0.5, 2]);
    expect(profiles[0].presentationHint).toContain('middenlaag');
  });

  it('prefers a depth profile over a generic logger at the same location', () => {
    const features = [
      {
        attributes: {
          featureIdentifier: 'logger-spaarndam',
          name: 'Logger Boezem Spaarndam',
          value: 20.1
        },
        geometry: { x: 4.67427, y: 52.411 }
      },
      {
        attributes: {
          featureIdentifier: 'meetboei-spaarndam_0050MINWS',
          name: 'Meetboei Spaarndam Boezem, 0.50m onder waterspiegel',
          value: 20.3
        },
        geometry: { x: 4.67427, y: 52.411 }
      },
      {
        attributes: {
          featureIdentifier: 'meetboei-spaarndam_0200MINWS',
          name: 'Meetboei Spaarndam Boezem, 2.00m onder waterspiegel',
          value: 19.8
        },
        geometry: { x: 4.67427, y: 52.411 }
      }
    ];

    const profiles = parseRijnlandTemperatureFeatures(features);

    expect(profiles).toHaveLength(1);
    expect(profiles[0].name).toBe('Meetboei Spaarndam Boezem');
    expect(profiles[0].readings.map((reading) => reading.depthM)).toEqual([0.5, 2]);
  });

  it('maps official pump status and signed flow direction', () => {
    const pumps = parseRijnlandPumpFeatures([{
      attributes: {
        featureIdentifier: '464-036-00021',
        name: 'Boezemgemaal Spaarndam',
        value: -12.4,
        classification: 'aan',
        chartUrl: 'https://example.test/pump'
      },
      geometry: { x: 4.67427, y: 52.411 }
    }], '2026-09-02T12:00:00.000Z');

    expect(pumps).toHaveLength(1);
    expect(pumps[0]).toMatchObject({
      active: true,
      status: 'aan',
      flowM3s: 12.4,
      flowSignedM3s: -12.4,
      flowDirection: 'afvoer'
    });
    expect(pumps[0].currentNote).toContain('uitstroom');
  });
});
