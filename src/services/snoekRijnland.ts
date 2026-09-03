import { getSnoekStructures, SnoekStructure } from './snoekStructures';
import { buildRoofvisAdvice, FISH_ADVICE_SOURCES, FishAdviceSource, RoofvisAdvice } from './snoekFishAdvice';

export type RijnlandTemperatureReading = {
  depthM: number | null;
  temperatureC: number;
  chlorideMgL: number | null;
  conductivityUsCm: number | null;
  classification: string;
  featureIdentifier: string;
  chartUrl: string;
};

export type RijnlandTemperatureProfile = {
  id: string;
  name: string;
  lat: number;
  lon: number;
  readings: RijnlandTemperatureReading[];
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
  waterQualityUpdatedAt: {
    chloride: string | null;
    conductivity: string | null;
  };
};

export type RijnlandPumpStatus = {
  id: string;
  name: string;
  lat: number;
  lon: number;
  active: boolean | null;
  hasLiveStatus: boolean;
  status: string;
  flowM3s: number | null;
  flowSignedM3s: number | null;
  flowDirection: 'afvoer' | 'aanvoer' | 'geen' | 'onbekend';
  featureIdentifier: string;
  pdokId: string | null;
  statusSource: 'rijnland-live' | 'pdok-only';
  chartUrl: string;
  currentNote: string;
  sourceUpdatedAt: string | null;
};

export type SnoekRijnlandResult = {
  ok: true;
  source: 'rijnland-arcgis';
  attribution: string;
  generatedAt: string;
  temperatureProfiles: RijnlandTemperatureProfile[];
  pumps: RijnlandPumpStatus[];
  errors: string[];
  coverageNote: string;
};

type ArcGisFeature = {
  attributes?: Record<string, any>;
  geometry?: { x?: number; y?: number };
};

const TEMPERATURE_LAYER = 'https://services1.arcgis.com/KXsJqtRt2xEqyDWx/arcgis/rest/services/c2f887f8-c73e-4c63-a8d5-2947995f44c4/FeatureServer/0';
const CHLORIDE_LAYER = 'https://services1.arcgis.com/KXsJqtRt2xEqyDWx/arcgis/rest/services/9f44ec39-93cf-49b2-86a9-c0fb1c695ca4/FeatureServer/0';
const CONDUCTIVITY_LAYER = 'https://services1.arcgis.com/KXsJqtRt2xEqyDWx/arcgis/rest/services/61c1bf48-e5d1-4bdd-9313-e0762046a0df/FeatureServer/0';
const PUMP_STATUS_LAYER = 'https://services1.arcgis.com/KXsJqtRt2xEqyDWx/arcgis/rest/services/e2d54d5c-4cd4-4476-898e-2effd50d9019/FeatureServer/0';
const INTEREST_ENVELOPE = '4.47,52.355,4.82,52.505';
const CACHE_MS = 5 * 60 * 1000;
const ERROR_CACHE_MS = 60 * 1000;

let cache: { expiresAt: number; result: SnoekRijnlandResult } | null = null;

function round(value: number, digits = 1): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function finiteNumber(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function sourceUpdatedAt(layerInfo: any): string | null {
  const milliseconds = finiteNumber(layerInfo?.editingInfo?.dataLastEditDate);
  if (milliseconds === null) return null;
  const date = new Date(milliseconds);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

async function fetchArcGisJson(url: string, params: Record<string, string> = {}): Promise<any> {
  const query = new URLSearchParams({ f: 'json', _: String(Date.now()), ...params });
  const response = await fetch(`${url}?${query}`, {
    headers: { 'User-Agent': 'snoek-ai-scout/1.0' }
  });
  if (!response.ok) {
    throw new Error(`Rijnland ArcGIS request failed: ${response.status}`);
  }
  const payload = await response.json();
  if (payload?.error) {
    throw new Error(`Rijnland ArcGIS error: ${payload.error.message || 'onbekend'}`);
  }
  return payload;
}

async function fetchLayer(layerUrl: string): Promise<{ features: ArcGisFeature[]; updatedAt: string | null }> {
  const [layerInfo, query] = await Promise.all([
    fetchArcGisJson(layerUrl),
    fetchArcGisJson(`${layerUrl}/query`, {
      where: '1=1',
      outFields: 'featureIdentifier,name,value,classification,chartUrl,ObjectId',
      returnGeometry: 'true',
      outSR: '4326',
      geometry: INTEREST_ENVELOPE,
      geometryType: 'esriGeometryEnvelope',
      inSR: '4326',
      spatialRel: 'esriSpatialRelIntersects',
      resultRecordCount: '1000'
    })
  ]);
  if (query?.exceededTransferLimit) {
    throw new Error('Rijnland ArcGIS result exceeds the configured record limit.');
  }
  return {
    features: Array.isArray(query?.features) ? query.features : [],
    updatedAt: sourceUpdatedAt(layerInfo)
  };
}

function depthFromFeature(name: string, featureIdentifier: string): number | null {
  const nameMatch = name.match(/([0-9]+(?:[.,][0-9]+)?)\s*m\s+onder\s+waterspiegel/i);
  if (nameMatch) return round(Number(nameMatch[1].replace(',', '.')), 2);
  const identifierMatch = featureIdentifier.match(/_([0-9]{4})MINWS$/i);
  return identifierMatch ? round(Number(identifierMatch[1]) / 100, 2) : null;
}

function profileName(name: string): string {
  return name.replace(/,\s*[0-9]+(?:[.,][0-9]+)?\s*m\s+onder\s+waterspiegel.*$/i, '').trim();
}

function profileAdvice(readings: RijnlandTemperatureReading[]): Pick<RijnlandTemperatureProfile, 'depthHintM' | 'profileNote' | 'presentationHint'> {
  const knownDepths = readings.filter((reading) => reading.depthM !== null) as Array<RijnlandTemperatureReading & { depthM: number }>;
  if (knownDepths.length < 2) {
    return {
      depthHintM: null,
      profileNote: 'Een meetdiepte; combineer deze temperatuur met bodemhoogte en stroming.',
      presentationHint: 'Begin zoekend in middenwater en controleer daarna het talud of de bodem.'
    };
  }

  let largestStep = { delta: -1, depthM: 0 };
  for (let index = 1; index < knownDepths.length; index += 1) {
    const upper = knownDepths[index - 1];
    const lower = knownDepths[index];
    const delta = Math.abs(lower.temperatureC - upper.temperatureC);
    if (delta > largestStep.delta) {
      largestStep = { delta, depthM: round((upper.depthM + lower.depthM) / 2, 2) };
    }
  }

  const temperatures = knownDepths.map((reading) => reading.temperatureC);
  const rangeC = Math.max(...temperatures) - Math.min(...temperatures);
  if (rangeC < 0.6) {
    return {
      depthHintM: null,
      profileNote: 'De gemeten waterkolom is vrijwel gelijk van temperatuur; geen duidelijke thermische dieptevoorkeur.',
      presentationHint: 'Vis meerdere lagen en laat stromingsnaad, talud en dekking de kunstaasdiepte bepalen.'
    };
  }

  const depthHintM = largestStep.depthM;
  return {
    depthHintM,
    profileNote: `Grootste temperatuurstap rond ${depthHintM.toFixed(2)} m; test net boven en onder deze overgang.`,
    presentationHint: depthHintM >= 2
      ? 'Gebruik een zwaardere shad of dropshot om de diepere overgang gecontroleerd te bereiken.'
      : depthHintM >= 1
        ? 'Gebruik een licht verzwaarde shad of softbait voor de middenlaag.'
        : 'Gebruik een licht kunstaas of ondiep lopende jerkbait voor de bovenste laag.'
  };
}

export function parseRijnlandTemperatureFeatures(features: ArcGisFeature[], updatedAt: string | null = null): RijnlandTemperatureProfile[] {
  const grouped = new Map<string, { name: string; lat: number; lon: number; readings: RijnlandTemperatureReading[] }>();
  for (const feature of features) {
    const attributes = feature.attributes || {};
    const lat = finiteNumber(feature.geometry?.y);
    const lon = finiteNumber(feature.geometry?.x);
    const temperatureC = finiteNumber(attributes.value);
    const name = String(attributes.name || 'Rijnland temperatuurmeetpunt');
    const featureIdentifier = String(attributes.featureIdentifier || '');
    if (lat === null || lon === null || temperatureC === null || !featureIdentifier) continue;

    const baseName = profileName(name);
    const key = `${lat.toFixed(5)}:${lon.toFixed(5)}:${baseName}`;
    const group = grouped.get(key) || { name: baseName, lat, lon, readings: [] };
    group.readings.push({
      depthM: depthFromFeature(name, featureIdentifier),
      temperatureC: round(temperatureC),
      chlorideMgL: null,
      conductivityUsCm: null,
      classification: String(attributes.classification || ''),
      featureIdentifier,
      chartUrl: String(attributes.chartUrl || '')
    });
    grouped.set(key, group);
  }

  const profiles = Array.from(grouped.values()).map((group) => {
    const readings = group.readings.sort((a, b) => {
      if (a.depthM === null) return 1;
      if (b.depthM === null) return -1;
      return a.depthM - b.depthM;
    });
    const temperatures = readings.map((reading) => reading.temperatureC);
    const minC = round(Math.min(...temperatures));
    const maxC = round(Math.max(...temperatures));
    return {
      id: `rijnland-temperature-${readings[0].featureIdentifier.replace(/[^a-z0-9_-]/gi, '-')}`,
      name: group.name,
      lat: group.lat,
      lon: group.lon,
      readings,
      minC,
      maxC,
      rangeC: round(maxC - minC),
      ...profileAdvice(readings),
      roofvisAdvice: buildRoofvisAdvice(readings, new Date().getMonth() + 1),
      scientificSources: FISH_ADVICE_SOURCES,
      measuredVariables: ['watertemperatuur'],
      missingVariables: ['zuurstof', 'troebelheid', 'vegetatie', 'lokale prooivisbemonstering'],
      sourceUpdatedAt: updatedAt,
      waterQualityUpdatedAt: {
        chloride: null,
        conductivity: null
      }
    };
  });

  const locationsWithDepthProfiles = new Set(
    profiles
      .filter((profile) => profile.readings.some((reading) => reading.depthM !== null))
      .map((profile) => `${profile.lat.toFixed(5)}:${profile.lon.toFixed(5)}`)
  );

  return profiles
    .filter((profile) => {
      const locationKey = `${profile.lat.toFixed(5)}:${profile.lon.toFixed(5)}`;
      const hasDepth = profile.readings.some((reading) => reading.depthM !== null);
      return hasDepth || !locationsWithDepthProfiles.has(locationKey);
    })
    .sort((a, b) => a.name.localeCompare(b.name, 'nl'));
}

type WaterQualityMetric = 'chlorideMgL' | 'conductivityUsCm';

export function parseRijnlandWaterQualityFeatures(features: ArcGisFeature[]): Map<string, number> {
  const values = new Map<string, number>();
  for (const feature of features) {
    const featureIdentifier = String(feature.attributes?.featureIdentifier || '');
    const value = finiteNumber(feature.attributes?.value);
    if (featureIdentifier && value !== null) values.set(featureIdentifier, round(value));
  }
  return values;
}

export function enrichRijnlandTemperatureProfiles(
  profiles: RijnlandTemperatureProfile[],
  chlorideFeatures: ArcGisFeature[],
  conductivityFeatures: ArcGisFeature[],
  month: number,
  updatedAt: { chloride: string | null; conductivity: string | null } = { chloride: null, conductivity: null }
): RijnlandTemperatureProfile[] {
  const qualityMaps: Array<{ metric: WaterQualityMetric; values: Map<string, number> }> = [
    { metric: 'chlorideMgL', values: parseRijnlandWaterQualityFeatures(chlorideFeatures) },
    { metric: 'conductivityUsCm', values: parseRijnlandWaterQualityFeatures(conductivityFeatures) }
  ];

  return profiles.map((profile) => {
    const readings = profile.readings.map((reading) => {
      const enriched = { ...reading };
      qualityMaps.forEach(({ metric, values }) => {
        enriched[metric] = values.get(reading.featureIdentifier) ?? null;
      });
      return enriched;
    });
    const hasChloride = readings.some((reading) => reading.chlorideMgL !== null);
    const hasConductivity = readings.some((reading) => reading.conductivityUsCm !== null);
    return {
      ...profile,
      readings,
      roofvisAdvice: buildRoofvisAdvice(readings, month),
      measuredVariables: [
        'watertemperatuur',
        ...(hasChloride ? ['chloride'] : []),
        ...(hasConductivity ? ['EGV/geleiding'] : [])
      ],
      waterQualityUpdatedAt: updatedAt
    };
  });
}

export function parseRijnlandPumpFeatures(features: ArcGisFeature[], updatedAt: string | null = null): RijnlandPumpStatus[] {
  const pumps: RijnlandPumpStatus[] = [];
  for (const feature of features) {
    const attributes = feature.attributes || {};
    const lat = finiteNumber(feature.geometry?.y);
    const lon = finiteNumber(feature.geometry?.x);
    const signedFlow = finiteNumber(attributes.value);
    const featureIdentifier = String(attributes.featureIdentifier || '');
    if (lat === null || lon === null || signedFlow === null || !featureIdentifier) continue;

    const status = String(attributes.classification || 'onbekend').trim().toLowerCase();
    const active = status === 'aan' ? true : status === 'uit' ? false : null;
    const flowDirection = signedFlow < -0.001 ? 'afvoer' : signedFlow > 0.001 ? 'aanvoer' : 'geen';
    pumps.push({
      id: `rijnland-pump-${featureIdentifier.replace(/[^a-z0-9_-]/gi, '-')}`,
      name: String(attributes.name || featureIdentifier),
      lat,
      lon,
      active,
      hasLiveStatus: true,
      status,
      flowM3s: round(Math.abs(signedFlow), 3),
      flowSignedM3s: round(signedFlow, 3),
      flowDirection,
      featureIdentifier,
      pdokId: null,
      statusSource: 'rijnland-live',
      chartUrl: String(attributes.chartUrl || ''),
      currentNote: active === true
        ? 'Gemaal is actief: controleer de uitstroom, stroomnaad en luwte direct ernaast.'
        : active === false
          ? 'Gemaal staat uit: volgens deze bron ontstaat hier nu geen pompstroming.'
          : 'Status is onbekend; trek zonder aanvullende meting geen conclusie over stroming.',
      sourceUpdatedAt: updatedAt
    });
  }
  return pumps.sort((a, b) => Number(b.active) - Number(a.active) || a.name.localeCompare(b.name, 'nl'));
}

function normalizedCode(value: string): string {
  return value.trim().toUpperCase();
}

export function mergeRijnlandPumpCoverage(
  livePumps: RijnlandPumpStatus[],
  pdokStructures: SnoekStructure[]
): RijnlandPumpStatus[] {
  const liveByCode = new Map(
    livePumps.map((pump) => [normalizedCode(pump.featureIdentifier), pump])
  );
  const matchedLiveIds = new Set<string>();
  const merged = pdokStructures
    .filter((structure) => structure.sourceLayer === 'gemaal')
    .map((structure) => {
      const sourceCode = String(structure.sourceCode || '').trim();
      const live = sourceCode ? liveByCode.get(normalizedCode(sourceCode)) : undefined;
      if (live) {
        matchedLiveIds.add(live.id);
        return {
          ...live,
          id: `rijnland-pump-pdok-${structure.id}`,
          name: structure.name,
          lat: structure.lat,
          lon: structure.lon,
          featureIdentifier: sourceCode,
          pdokId: structure.id
        };
      }

      return {
        id: `rijnland-pump-pdok-${structure.id}`,
        name: structure.name,
        lat: structure.lat,
        lon: structure.lon,
        active: null,
        hasLiveStatus: false,
        status: 'onbekend',
        flowM3s: null,
        flowSignedM3s: null,
        flowDirection: 'onbekend' as const,
        featureIdentifier: sourceCode,
        pdokId: structure.id,
        statusSource: 'pdok-only' as const,
        chartUrl: '',
        currentNote: 'Dit gemaal staat in PDOK, maar de gekoppelde Rijnland-bron publiceert hiervoor geen actuele AAN/UIT-status.',
        sourceUpdatedAt: null
      };
    });

  livePumps.forEach((pump) => {
    if (!matchedLiveIds.has(pump.id)) merged.push(pump);
  });

  return merged.sort((a, b) => {
    const statusOrder = (item: RijnlandPumpStatus) => item.active === true ? 0 : item.active === false ? 1 : 2;
    return statusOrder(a) - statusOrder(b) || a.name.localeCompare(b.name, 'nl');
  });
}

export async function getSnoekRijnland(now = new Date()): Promise<SnoekRijnlandResult> {
  if (cache && cache.expiresAt > now.getTime()) return cache.result;

  const errors: string[] = [];
  let temperatureProfiles: RijnlandTemperatureProfile[] = [];
  let livePumps: RijnlandPumpStatus[] = [];
  let pdokPumps: SnoekStructure[] = [];
  const [temperatureResult, chlorideResult, conductivityResult, pumpResult, pdokResult] = await Promise.allSettled([
    fetchLayer(TEMPERATURE_LAYER),
    fetchLayer(CHLORIDE_LAYER),
    fetchLayer(CONDUCTIVITY_LAYER),
    fetchLayer(PUMP_STATUS_LAYER),
    getSnoekStructures({
      west: 4.47,
      south: 52.355,
      east: 4.82,
      north: 52.505,
      layers: 'gemaal',
      limit: 30
    })
  ]);

  if (temperatureResult.status === 'fulfilled') {
    temperatureProfiles = parseRijnlandTemperatureFeatures(temperatureResult.value.features, temperatureResult.value.updatedAt);
  } else {
    errors.push(`Temperatuurlaag: ${temperatureResult.reason instanceof Error ? temperatureResult.reason.message : String(temperatureResult.reason)}`);
  }
  if (temperatureProfiles.length) {
    const chlorideFeatures = chlorideResult.status === 'fulfilled' ? chlorideResult.value.features : [];
    const conductivityFeatures = conductivityResult.status === 'fulfilled' ? conductivityResult.value.features : [];
    temperatureProfiles = enrichRijnlandTemperatureProfiles(
      temperatureProfiles,
      chlorideFeatures,
      conductivityFeatures,
      now.getMonth() + 1,
      {
        chloride: chlorideResult.status === 'fulfilled' ? chlorideResult.value.updatedAt : null,
        conductivity: conductivityResult.status === 'fulfilled' ? conductivityResult.value.updatedAt : null
      }
    );
  }
  if (chlorideResult.status === 'rejected') {
    errors.push(`Chloridelaag: ${chlorideResult.reason instanceof Error ? chlorideResult.reason.message : String(chlorideResult.reason)}`);
  }
  if (conductivityResult.status === 'rejected') {
    errors.push(`EGV-laag: ${conductivityResult.reason instanceof Error ? conductivityResult.reason.message : String(conductivityResult.reason)}`);
  }
  if (pumpResult.status === 'fulfilled') {
    livePumps = parseRijnlandPumpFeatures(pumpResult.value.features, pumpResult.value.updatedAt);
  } else {
    errors.push(`Gemaalstatuslaag: ${pumpResult.reason instanceof Error ? pumpResult.reason.message : String(pumpResult.reason)}`);
  }
  if (pdokResult.status === 'fulfilled') {
    pdokPumps = pdokResult.value.structures;
  } else {
    errors.push(`PDOK gemaallaag: ${pdokResult.reason instanceof Error ? pdokResult.reason.message : String(pdokResult.reason)}`);
  }
  const pumps = pdokPumps.length ? mergeRijnlandPumpCoverage(livePumps, pdokPumps) : livePumps;
  if (!temperatureProfiles.length && !pumps.length) {
    throw new Error(errors.join(' | ') || 'Rijnland leverde geen meetpunten binnen het kaartgebied.');
  }

  const result: SnoekRijnlandResult = {
    ok: true,
    source: 'rijnland-arcgis',
    attribution: 'Temperatuur, chloride, EGV en gemaalstatus: Hoogheemraadschap van Rijnland via ArcGIS Online en HydroNET. Gemaallocaties: PDOK Waterschappen Kunstwerken IMWA.',
    generatedAt: now.toISOString(),
    temperatureProfiles,
    pumps,
    errors,
    coverageNote: `Temperatuurpunten zijn waar mogelijk verrijkt met chloride en EGV op dezelfde meetdiepte. Alle ${pumps.length} PDOK-gemalen krijgen een statusmarker; ${pumps.filter((pump) => pump.hasLiveStatus).length} hebben een gekoppelde live AAN/UIT-status. Advieszones zijn hypotheses uit meting plus literatuur, geen visdetectie.`
  };
  cache = {
    expiresAt: now.getTime() + (errors.length ? ERROR_CACHE_MS : CACHE_MS),
    result
  };
  return result;
}
