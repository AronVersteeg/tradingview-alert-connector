export type SnoekCurrentStrength = 'weak' | 'moderate' | 'powerful' | 'strong';

export type SnoekCurrentMeasurement = {
  id: string;
  source: 'rws-waterwebservices';
  measured: true;
  locationCode: string;
  name: string;
  lat: number;
  lon: number;
  speedMs: number;
  bearingDeg: number;
  directionLabel: string;
  strength: SnoekCurrentStrength;
  strengthLabel: string;
  forcePct: number;
  observedAt: string;
  ageMinutes: number;
  qualityCode: string;
  status: string;
  method: string;
  fishingNote: string;
};

export type SnoekCurrentResult = {
  ok: true;
  source: 'rws-waterwebservices';
  attribution: string;
  generatedAt: string;
  maxAgeMinutes: number;
  currents: SnoekCurrentMeasurement[];
  coverageNote: string;
};

type Candidate = {
  locationCode: string;
  name: string;
  lat: number;
  lon: number;
  quantity: 'STROOMSHD' | 'STROOMRTG';
  value: number;
  observedAt: Date;
  qualityCode: string;
  status: string;
  method: string;
};

const RWS_URL = 'https://ddapi20-waterwebservices.rijkswaterstaat.nl/ONLINEWAARNEMINGENSERVICES/OphalenLaatsteWaarnemingen';
const MAX_AGE_MINUTES = 90;
const CACHE_MS = 5 * 60 * 1000;
const ACCEPTED_QUALITY_CODES = new Set(['00', '10', '20', '25', '30', '40']);
const RWS_CURRENT_LOCATIONS = [
  'ijgeul',
  'ijgeul.1',
  'ijmuiden.stroommeetpaal.backup',
  'ijmuiden.erosiegeul'
];

let cache: { expiresAt: number; result: SnoekCurrentResult } | null = null;

function round(value: number, digits = 2): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function normalizedBearing(value: number): number {
  return ((value % 360) + 360) % 360;
}

function compassLabel(bearing: number): string {
  const labels = ['N', 'NNO', 'NO', 'ONO', 'O', 'OZO', 'ZO', 'ZZO', 'Z', 'ZZW', 'ZW', 'WZW', 'W', 'WNW', 'NW', 'NNW'];
  return labels[Math.round(normalizedBearing(bearing) / 22.5) % labels.length];
}

function meanBearing(values: number[]): number {
  const vector = values.reduce((sum, value) => {
    const radians = normalizedBearing(value) * Math.PI / 180;
    sum.x += Math.sin(radians);
    sum.y += Math.cos(radians);
    return sum;
  }, { x: 0, y: 0 });
  return normalizedBearing(Math.atan2(vector.x, vector.y) * 180 / Math.PI);
}

function strengthFor(speedMs: number): Pick<SnoekCurrentMeasurement, 'strength' | 'strengthLabel' | 'forcePct' | 'fishingNote'> {
  const forcePct = Math.min(100, Math.round(speedMs * 100));
  if (speedMs < 0.1) {
    return {
      strength: 'weak',
      strengthLabel: 'Zwak',
      forcePct,
      fishingNote: 'Weinig waterbeweging; zoek vooral harde randen, schaduw en lokale uitstroom.'
    };
  }
  if (speedMs < 0.3) {
    return {
      strength: 'moderate',
      strengthLabel: 'Matig',
      forcePct,
      fishingNote: 'Beheersbare stroming voor dropshot en shad; zoek de overgang naar rustiger water.'
    };
  }
  if (speedMs < 0.6) {
    return {
      strength: 'powerful',
      strengthLabel: 'Krachtig',
      forcePct,
      fishingNote: 'Duidelijke stromingsnaden; vis de luwte achter kades, taluds en kunstwerken.'
    };
  }
  return {
    strength: 'strong',
    strengthLabel: 'Sterk',
    forcePct,
    fishingNote: 'Hoofdstroom is stevig; richt je op keerwater, binnenbochten en de rustige zijde van obstakels.'
  };
}

function candidatesFromResponse(payload: any, now: Date): Candidate[] {
  const maximumAgeMs = MAX_AGE_MINUTES * 60 * 1000;
  const futureToleranceMs = 15 * 60 * 1000;
  const candidates: Candidate[] = [];

  for (const series of payload?.WaarnemingenLijst || []) {
    const quantity = series?.AquoMetadata?.Grootheid?.Code;
    if (quantity !== 'STROOMSHD' && quantity !== 'STROOMRTG') continue;

    for (const measurement of series?.MetingenLijst || []) {
      const value = Number(measurement?.Meetwaarde?.Waarde_Numeriek);
      const observedAt = new Date(measurement?.Tijdstip);
      const qualityCode = String(measurement?.WaarnemingMetadata?.Kwaliteitswaardecode || '');
      const ageMs = now.getTime() - observedAt.getTime();
      if (!Number.isFinite(value) || Number.isNaN(observedAt.getTime())) continue;
      if (!ACCEPTED_QUALITY_CODES.has(qualityCode)) continue;
      if (ageMs > maximumAgeMs || ageMs < -futureToleranceMs) continue;

      candidates.push({
        locationCode: String(series?.Locatie?.Code || ''),
        name: String(series?.Locatie?.Naam || series?.Locatie?.Code || 'RWS stroommeetpunt'),
        lat: Number(series?.Locatie?.Lat),
        lon: Number(series?.Locatie?.Lon),
        quantity,
        value,
        observedAt,
        qualityCode,
        status: String(measurement?.WaarnemingMetadata?.Statuswaarde || 'Onbekend'),
        method: String(series?.AquoMetadata?.WaardeBepalingsMethode?.Omschrijving || 'RWS meting')
      });
    }
  }

  return candidates.filter((candidate) => candidate.locationCode && Number.isFinite(candidate.lat) && Number.isFinite(candidate.lon));
}

export function parseRwsCurrentResponse(payload: any, now = new Date()): SnoekCurrentMeasurement[] {
  const candidates = candidatesFromResponse(payload, now);
  const locationCodes = Array.from(new Set(candidates.map((candidate) => candidate.locationCode)));
  const currents: SnoekCurrentMeasurement[] = [];

  for (const locationCode of locationCodes) {
    const locationCandidates = candidates.filter((candidate) => candidate.locationCode === locationCode);
    const speed = locationCandidates
      .filter((candidate) => candidate.quantity === 'STROOMSHD')
      .sort((a, b) => b.observedAt.getTime() - a.observedAt.getTime())[0];
    if (!speed) continue;

    const matchingDirections = locationCandidates.filter((candidate) => (
      candidate.quantity === 'STROOMRTG' &&
      Math.abs(candidate.observedAt.getTime() - speed.observedAt.getTime()) <= 15 * 60 * 1000
    ));
    if (!matchingDirections.length) continue;

    const bearingDeg = round(meanBearing(matchingDirections.map((candidate) => candidate.value)), 1);
    const ageMinutes = Math.max(0, Math.round((now.getTime() - speed.observedAt.getTime()) / 60000));
    const strength = strengthFor(speed.value);
    currents.push({
      id: `rws-current-${locationCode}`,
      source: 'rws-waterwebservices',
      measured: true,
      locationCode,
      name: speed.name,
      lat: speed.lat,
      lon: speed.lon,
      speedMs: round(speed.value, 3),
      bearingDeg,
      directionLabel: compassLabel(bearingDeg),
      ...strength,
      observedAt: speed.observedAt.toISOString(),
      ageMinutes,
      qualityCode: speed.qualityCode,
      status: speed.status,
      method: speed.method
    });
  }

  const preferred = currents.sort((a, b) => {
    const backupOrder = Number(a.locationCode.includes('backup')) - Number(b.locationCode.includes('backup'));
    return backupOrder || b.speedMs - a.speedMs;
  });
  const deduped = preferred.filter((current, index) => !preferred.slice(0, index).some((earlier) => {
    const northMetres = (current.lat - earlier.lat) * 111000;
    const eastMetres = (current.lon - earlier.lon) * 68000;
    return Math.hypot(northMetres, eastMetres) < 75;
  }));
  return deduped.sort((a, b) => b.speedMs - a.speedMs);
}

async function fetchRwsCurrent(): Promise<any> {
  const body = {
    LocatieLijst: RWS_CURRENT_LOCATIONS.map((Code) => ({ Code })),
    AquoPlusWaarnemingMetadataLijst: [
      { AquoMetadata: { Compartiment: { Code: 'OW' }, Grootheid: { Code: 'STROOMSHD' } } },
      { AquoMetadata: { Compartiment: { Code: 'OW' }, Grootheid: { Code: 'STROOMRTG' } } }
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
  if (!response.ok) {
    throw new Error(`RWS stromingsdata request failed: ${response.status}`);
  }
  return response.json();
}

export async function getSnoekCurrent(now = new Date()): Promise<SnoekCurrentResult> {
  if (cache && cache.expiresAt > now.getTime()) return cache.result;

  const payload = await fetchRwsCurrent();
  const result: SnoekCurrentResult = {
    ok: true,
    source: 'rws-waterwebservices',
    attribution: 'Actuele waterdata: Rijkswaterstaat WaterWebservices (CC0).',
    generatedAt: now.toISOString(),
    maxAgeMinutes: MAX_AGE_MINUTES,
    currents: parseRwsCurrentResponse(payload, now),
    coverageNote: 'Pijlen zijn echte puntmetingen. Tussen meetpunten wordt geen stroming geinterpoleerd.'
  };
  cache = { expiresAt: now.getTime() + CACHE_MS, result };
  return result;
}
