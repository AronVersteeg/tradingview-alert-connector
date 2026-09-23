import { buildRoofvisAdvice, FISH_ADVICE_SOURCES, FishAdviceSource, RoofvisAdvice } from './snoekFishAdvice';
import { profileAdvice, RijnlandTemperatureReading } from './snoekRijnland';

export type RwsTemperatureReading = RijnlandTemperatureReading & {
  sampleHeightM: number | null;
  referencePlane: string;
  observedAt: string;
  ageMinutes: number;
  qualityCode: string;
  status: string;
  method: string;
};

export type RwsTemperatureProfile = {
  id: string;
  source: 'rws-waterwebservices';
  sourceLabel: 'RWS Waterinfo watertemperatuur';
  locationCode: string;
  name: string;
  lat: number;
  lon: number;
  readings: RwsTemperatureReading[];
  minC: number;
  maxC: number;
  rangeC: number;
  depthHintM: number | null;
  profileNote: string;
  presentationHint: string;
  roofvisAdvice: RoofvisAdvice[];
  scientificSources: FishAdviceSource[];
  measuredVariables: string[];
  missingVariables: string[];
  sourceUpdatedAt: string | null;
};

export type SnoekRwsTemperatureResult = {
  ok: true;
  source: 'rws-waterwebservices';
  attribution: string;
  generatedAt: string;
  maxAgeMinutes: number;
  temperatureProfiles: RwsTemperatureProfile[];
  coverageNote: string;
};

type ReadingCandidate = RwsTemperatureReading & {
  observedDate: Date;
};

const RWS_URL = 'https://ddapi20-waterwebservices.rijkswaterstaat.nl/ONLINEWAARNEMINGENSERVICES/OphalenLaatsteWaarnemingen';
const RWS_TEMPERATURE_LOCATIONS = [
  'ijmuiden.buitenhaven',
  'velsen.velserkom',
  'velsenzuid.spaarndammerpolder',
  'spaarndam.zijkanaalc'
];
const MAX_AGE_MINUTES = 180;
const CACHE_MS = 5 * 60 * 1000;
const ACCEPTED_QUALITY_CODES = new Set(['00', '10', '20', '25', '30', '40']);

let cache: { expiresAt: number; result: SnoekRwsTemperatureResult } | null = null;

function round(value: number, digits = 1): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function finiteNumber(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function depthBelowSurface(sampleHeightCm: number | null, referencePlane: string): number | null {
  if (sampleHeightCm === null || referencePlane.toUpperCase() !== 'WATSGL') return null;
  return round(Math.abs(sampleHeightCm) / 100, 2);
}

function chartUrl(locationCode: string): string {
  return `https://waterinfo.rws.nl/publiek/watertemperatuur/${encodeURIComponent(locationCode)}/details`;
}

export function parseRwsTemperatureResponse(
  payload: any,
  now = new Date(),
  month = now.getMonth() + 1
): RwsTemperatureProfile[] {
  const groups = new Map<string, {
    name: string;
    lat: number;
    lon: number;
    readings: Map<string, ReadingCandidate>;
  }>();
  const maximumAgeMs = MAX_AGE_MINUTES * 60 * 1000;
  const futureToleranceMs = 15 * 60 * 1000;

  for (const series of payload?.WaarnemingenLijst || []) {
    if (series?.AquoMetadata?.Grootheid?.Code !== 'T') continue;
    if (series?.AquoMetadata?.Eenheid?.Code !== 'oC') continue;

    const locationCode = String(series?.Locatie?.Code || '');
    const lat = finiteNumber(series?.Locatie?.Lat);
    const lon = finiteNumber(series?.Locatie?.Lon);
    if (!RWS_TEMPERATURE_LOCATIONS.includes(locationCode) || lat === null || lon === null) continue;

    const group = groups.get(locationCode) || {
      name: String(series?.Locatie?.Naam || locationCode),
      lat,
      lon,
      readings: new Map<string, ReadingCandidate>()
    };

    for (const measurement of series?.MetingenLijst || []) {
      const temperatureC = finiteNumber(measurement?.Meetwaarde?.Waarde_Numeriek);
      const observedDate = new Date(measurement?.Tijdstip);
      const qualityCode = String(measurement?.WaarnemingMetadata?.Kwaliteitswaardecode || '');
      const sampleHeightCm = finiteNumber(measurement?.WaarnemingMetadata?.Bemonsteringshoogte);
      const referencePlane = String(measurement?.WaarnemingMetadata?.Referentievlak || 'NVT');
      const ageMs = now.getTime() - observedDate.getTime();
      if (temperatureC === null || temperatureC < -2 || temperatureC > 40) continue;
      if (Number.isNaN(observedDate.getTime()) || ageMs > maximumAgeMs || ageMs < -futureToleranceMs) continue;
      if (!ACCEPTED_QUALITY_CODES.has(qualityCode)) continue;

      const depthM = depthBelowSurface(sampleHeightCm, referencePlane);
      const sampleHeightM = sampleHeightCm === null ? null : round(sampleHeightCm / 100, 2);
      const depthKey = `${referencePlane}:${sampleHeightCm ?? 'unknown'}`;
      const candidate: ReadingCandidate = {
        depthM,
        sampleHeightM,
        referencePlane,
        temperatureC: round(temperatureC),
        chlorideMgL: null,
        conductivityUsCm: null,
        classification: String(measurement?.WaarnemingMetadata?.Statuswaarde || 'Onbekend'),
        featureIdentifier: `${locationCode}:${depthKey}`,
        chartUrl: chartUrl(locationCode),
        observedAt: observedDate.toISOString(),
        observedDate,
        ageMinutes: Math.max(0, Math.round(ageMs / 60000)),
        qualityCode,
        status: String(measurement?.WaarnemingMetadata?.Statuswaarde || 'Onbekend'),
        method: String(series?.AquoMetadata?.WaardeBepalingsMethode?.Omschrijving || 'RWS meting')
      };
      const previous = group.readings.get(depthKey);
      if (!previous || candidate.observedDate > previous.observedDate) group.readings.set(depthKey, candidate);
    }

    if (group.readings.size) groups.set(locationCode, group);
  }

  return Array.from(groups.entries()).map(([locationCode, group]) => {
    const readings = Array.from(group.readings.values())
      .sort((a, b) => {
        if (a.depthM !== null && b.depthM !== null) return a.depthM - b.depthM;
        if (a.depthM !== null) return -1;
        if (b.depthM !== null) return 1;
        return (b.sampleHeightM ?? 0) - (a.sampleHeightM ?? 0);
      })
      .map(({ observedDate: _observedDate, ...reading }) => reading);
    const temperatures = readings.map((reading) => reading.temperatureC);
    const minC = round(Math.min(...temperatures));
    const maxC = round(Math.max(...temperatures));
    const newest = readings.reduce((latest, reading) => (
      reading.observedAt > latest ? reading.observedAt : latest
    ), readings[0].observedAt);
    return {
      id: `rws-temperature-${locationCode.replace(/[^a-z0-9_-]/gi, '-')}`,
      source: 'rws-waterwebservices' as const,
      sourceLabel: 'RWS Waterinfo watertemperatuur' as const,
      locationCode,
      name: group.name,
      lat: group.lat,
      lon: group.lon,
      readings,
      minC,
      maxC,
      rangeC: round(maxC - minC),
      ...profileAdvice(readings),
      roofvisAdvice: buildRoofvisAdvice(readings, month),
      scientificSources: FISH_ADVICE_SOURCES,
      measuredVariables: ['watertemperatuur', ...(readings.some((reading) => reading.depthM !== null) ? ['meetdiepte'] : ['meetniveau'])],
      missingVariables: ['zuurstof', 'troebelheid', 'vegetatie', 'lokale prooivisbemonstering'],
      sourceUpdatedAt: newest
    };
  }).sort((a, b) => a.name.localeCompare(b.name, 'nl'));
}

async function fetchRwsTemperature(): Promise<any> {
  const body = {
    LocatieLijst: RWS_TEMPERATURE_LOCATIONS.map((Code) => ({ Code })),
    AquoPlusWaarnemingMetadataLijst: [
      {
        AquoMetadata: {
          Compartiment: { Code: 'OW' },
          Grootheid: { Code: 'T' },
          ProcesType: 'meting'
        }
      }
    ]
  };
  const response = await fetch(RWS_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'User-Agent': 'snoek-ai-scout/1.0'
    },
    body: JSON.stringify(body)
  });
  if (!response.ok) throw new Error(`RWS watertemperatuur request failed: ${response.status}`);
  return response.json();
}

export async function getSnoekRwsTemperature(now = new Date()): Promise<SnoekRwsTemperatureResult> {
  if (cache && cache.expiresAt > now.getTime()) return cache.result;
  const temperatureProfiles = parseRwsTemperatureResponse(await fetchRwsTemperature(), now);
  const result: SnoekRwsTemperatureResult = {
    ok: true,
    source: 'rws-waterwebservices',
    attribution: 'Actuele watertemperatuur: Rijkswaterstaat Waterinfo / WaterWebservices (CC0).',
    generatedAt: now.toISOString(),
    maxAgeMinutes: MAX_AGE_MINUTES,
    temperatureProfiles,
    coverageNote: 'Dit zijn actuele RWS-puntmetingen. Er wordt niet tussen meetstations geinterpoleerd; meetdiepte wordt alleen afgeleid wanneer het referentievlak de waterspiegel is.'
  };
  cache = { expiresAt: now.getTime() + CACHE_MS, result };
  return result;
}
