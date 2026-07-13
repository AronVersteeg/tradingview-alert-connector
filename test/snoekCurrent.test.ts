import { parseRwsCurrentResponse } from '../src/services/snoekCurrent';

function series(quantity: string, value: number, time: string, quality = '00') {
  return {
    AquoMetadata: {
      Grootheid: { Code: quantity },
      WaardeBepalingsMethode: { Omschrijving: 'test' }
    },
    Locatie: { Code: 'ijgeul.1', Naam: 'IJGeul, 1', Lat: 52.463943, Lon: 4.517596 },
    MetingenLijst: [{
      Meetwaarde: { Waarde_Numeriek: value },
      Tijdstip: time,
      WaarnemingMetadata: { Kwaliteitswaardecode: quality, Statuswaarde: 'Ongecontroleerd' }
    }]
  };
}

describe('parseRwsCurrentResponse', () => {
  it('combines fresh speed and direction into a force-aware vector', () => {
    const currents = parseRwsCurrentResponse({
      WaarnemingenLijst: [
        series('STROOMSHD', 0.649, '2026-07-13T15:50:00.000+01:00'),
        series('STROOMRTG', 15.5, '2026-07-13T15:50:00.000+01:00'),
        series('STROOMRTG', 16.5, '2026-07-13T15:50:00.000+01:00')
      ]
    }, new Date('2026-07-13T15:55:00.000+01:00'));

    expect(currents).toHaveLength(1);
    expect(currents[0]).toMatchObject({
      speedMs: 0.649,
      bearingDeg: 16,
      directionLabel: 'NNO',
      strength: 'strong',
      ageMinutes: 5
    });
  });

  it('rejects stale readings and disallowed quality codes', () => {
    const currents = parseRwsCurrentResponse({
      WaarnemingenLijst: [
        series('STROOMSHD', 0.4, '2026-07-13T12:00:00.000+01:00'),
        series('STROOMRTG', 180, '2026-07-13T12:00:00.000+01:00'),
        series('STROOMSHD', 0.6, '2026-07-13T15:50:00.000+01:00', '99'),
        series('STROOMRTG', 20, '2026-07-13T15:50:00.000+01:00', '99')
      ]
    }, new Date('2026-07-13T16:00:00.000+01:00'));

    expect(currents).toEqual([]);
  });
});
