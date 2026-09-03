import { buildRoofvisAdvice } from '../src/services/snoekFishAdvice';
import {
  enrichRijnlandTemperatureProfiles,
  parseRijnlandTemperatureFeatures
} from '../src/services/snoekRijnland';

describe('scientific roofvis advice', () => {
  it('does not invent a narrow pike depth in a homogeneous water column', () => {
    const advice = buildRoofvisAdvice([
      { depthM: 0.5, temperatureC: 19.8 },
      { depthM: 2, temperatureC: 19.6 },
      { depthM: 3.5, temperatureC: 19.7 }
    ], 9);

    const pike = advice.find((item) => item.species === 'snoek');
    expect(pike).toMatchObject({
      confidence: 'laag',
      dayDepth: { minM: 0.5, maxM: 3.5 }
    });
    expect(pike?.reasons.join(' ')).toContain('selecteert geen smalle laag');
    expect(pike?.seasonalPreyCm).toEqual({ min: 7, max: 11 });
    expect(pike?.lureLengthCm).toEqual({ min: 8, max: 14 });
  });

  it('uses light period for zander without claiming fish detection', () => {
    const advice = buildRoofvisAdvice([
      { depthM: 0.5, temperatureC: 20.5 },
      { depthM: 2, temperatureC: 19.2 },
      { depthM: 3.5, temperatureC: 18.7 }
    ], 9);

    const zander = advice.find((item) => item.species === 'snoekbaars');
    expect(zander?.dayDepth?.minM).toBeGreaterThan(zander?.lowLightDepth?.minM ?? 99);
    expect(zander?.limitation).toContain('geen visdetectie');
  });

  it('joins chloride and conductivity by the exact Rijnland feature identifier', () => {
    const idTop = '476-105-00001_oppervlaktewater_0050MINWS';
    const idBottom = '476-105-00001_oppervlaktewater_0350MINWS';
    const profiles = parseRijnlandTemperatureFeatures([
      {
        attributes: { featureIdentifier: idTop, name: 'Meetboei Zijkanaal C, 0.50m onder waterspiegel', value: 20.2 },
        geometry: { x: 4.6823, y: 52.41406 }
      },
      {
        attributes: { featureIdentifier: idBottom, name: 'Meetboei Zijkanaal C, 3.50m onder waterspiegel', value: 19.1 },
        geometry: { x: 4.6823, y: 52.41406 }
      }
    ]);

    const enriched = enrichRijnlandTemperatureProfiles(
      profiles,
      [
        { attributes: { featureIdentifier: idTop, value: 1733 } },
        { attributes: { featureIdentifier: idBottom, value: 2979 } }
      ],
      [
        { attributes: { featureIdentifier: idTop, value: 6062 } },
        { attributes: { featureIdentifier: idBottom, value: 9957 } }
      ],
      9
    );

    expect(enriched[0].readings).toMatchObject([
      { depthM: 0.5, chlorideMgL: 1733, conductivityUsCm: 6062 },
      { depthM: 3.5, chlorideMgL: 2979, conductivityUsCm: 9957 }
    ]);
    expect(enriched[0].measuredVariables).toEqual(['watertemperatuur', 'chloride', 'EGV/geleiding']);
    expect(enriched[0].roofvisAdvice[0].reasons.join(' ')).toContain('Chloride verschilt');
  });
});
