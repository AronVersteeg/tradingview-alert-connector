import { parseRwsTemperatureResponse } from '../src/services/snoekRwsTemperature';

function series(overrides: Record<string, unknown> = {}) {
  return {
    AquoMetadata: {
      Eenheid: { Code: 'oC' },
      Grootheid: { Code: 'T' },
      WaardeBepalingsMethode: { Omschrijving: '10-minutengemiddelde' }
    },
    Locatie: {
      Code: 'velsen.velserkom',
      Naam: 'Velserkom',
      Lat: 52.467611,
      Lon: 4.623665
    },
    MetingenLijst: [{
      Meetwaarde: { Waarde_Numeriek: 18.9 },
      Tijdstip: '2026-09-23T01:30:00.000Z',
      WaarnemingMetadata: {
        Bemonsteringshoogte: '-100',
        Kwaliteitswaardecode: '00',
        Referentievlak: 'WATSGL',
        Statuswaarde: 'Ongecontroleerd'
      }
    }],
    ...overrides
  };
}

describe('RWS Waterinfo temperature parsing', () => {
  it('builds a fresh multi-depth profile and rejects stale or bad-quality values', () => {
    const deep = series({
      MetingenLijst: [{
        Meetwaarde: { Waarde_Numeriek: 17.8 },
        Tijdstip: '2026-09-23T01:30:00.000Z',
        WaarnemingMetadata: {
          Bemonsteringshoogte: '-600',
          Kwaliteitswaardecode: '10',
          Referentievlak: 'WATSGL',
          Statuswaarde: 'Gecontroleerd'
        }
      }]
    });
    const stale = series({
      MetingenLijst: [{
        Meetwaarde: { Waarde_Numeriek: 6.6 },
        Tijdstip: '1992-12-15T09:30:00.000Z',
        WaarnemingMetadata: {
          Bemonsteringshoogte: '-100',
          Kwaliteitswaardecode: '00',
          Referentievlak: 'WATSGL'
        }
      }]
    });
    const rejected = series({
      MetingenLijst: [{
        Meetwaarde: { Waarde_Numeriek: 30 },
        Tijdstip: '2026-09-23T01:30:00.000Z',
        WaarnemingMetadata: {
          Bemonsteringshoogte: '-1200',
          Kwaliteitswaardecode: '99',
          Referentievlak: 'WATSGL'
        }
      }]
    });

    const profiles = parseRwsTemperatureResponse(
      { WaarnemingenLijst: [series(), deep, stale, rejected] },
      new Date('2026-09-23T02:00:00.000Z'),
      9
    );

    expect(profiles).toHaveLength(1);
    expect(profiles[0]).toMatchObject({
      source: 'rws-waterwebservices',
      locationCode: 'velsen.velserkom',
      minC: 17.8,
      maxC: 18.9,
      rangeC: 1.1
    });
    expect(profiles[0].readings.map((reading) => reading.depthM)).toEqual([1, 6]);
    expect(profiles[0].roofvisAdvice).toHaveLength(3);
  });

  it('keeps NAP sample height separate from depth below the water surface', () => {
    const napSeries = series({
      Locatie: {
        Code: 'spaarndam.zijkanaalc',
        Naam: 'Spaarndam, Zijkanaal C',
        Lat: 52.420077,
        Lon: 4.693508
      },
      MetingenLijst: [{
        Meetwaarde: { Waarde_Numeriek: 19.1 },
        Tijdstip: '2026-09-23T01:30:00.000Z',
        WaarnemingMetadata: {
          Bemonsteringshoogte: '-440',
          Kwaliteitswaardecode: '00',
          Referentievlak: 'NAP',
          Statuswaarde: 'Ongecontroleerd'
        }
      }]
    });

    const [profile] = parseRwsTemperatureResponse(
      { WaarnemingenLijst: [napSeries] },
      new Date('2026-09-23T02:00:00.000Z'),
      9
    );

    expect(profile.readings[0]).toMatchObject({
      depthM: null,
      sampleHeightM: -4.4,
      referencePlane: 'NAP'
    });
  });
});
