export type SnoekStructureType =
  | 'culvert'
  | 'pumping_station'
  | 'bridge'
  | 'weir'
  | 'lock'
  | 'fish_passage'
  | 'siphon'
  | 'trash_rack'
  | 'gate'
  | 'drop'
  | 'water_control';

export type SnoekStructuresBbox = {
  west: number;
  south: number;
  east: number;
  north: number;
};

export type SnoekStructure = {
  id: string;
  type: SnoekStructureType;
  source: 'pdok-imwa' | 'rws-arcgis';
  sourceLayer: string;
  name: string;
  label: string;
  lat: number;
  lon: number;
  x: number;
  y: number;
  score: number;
  reasons: string[];
};

export type SnoekStructuresResult = {
  ok: true;
  bbox: SnoekStructuresBbox;
  total: number;
  rendered: number;
  structures: SnoekStructure[];
  counts: Record<string, number>;
  sources: Array<{
    id: string;
    label: string;
    attribution: string;
  }>;
};

const DEFAULT_BBOX: SnoekStructuresBbox = {
  west: 4.575,
  south: 52.365,
  east: 4.815,
  north: 52.495
};

const PDOK_TYPES: Array<{ layer: string; type: SnoekStructureType; label: string }> = [
  { layer: 'duiker', type: 'culvert', label: 'Duiker' },
  { layer: 'gemaal', type: 'pumping_station', label: 'Gemaal' },
  { layer: 'brug', type: 'bridge', label: 'Brug' },
  { layer: 'stuw', type: 'weir', label: 'Stuw' },
  { layer: 'sluis', type: 'lock', label: 'Sluis' },
  { layer: 'vispassage', type: 'fish_passage', label: 'Vispassage' },
  { layer: 'sifon', type: 'siphon', label: 'Sifon' },
  { layer: 'vuilvang', type: 'trash_rack', label: 'Vuilvang' },
  { layer: 'afsluitmiddel', type: 'gate', label: 'Afsluitmiddel' },
  { layer: 'bodemval', type: 'drop', label: 'Bodemval' }
];

const RWS_LAYERS: Array<{ id: number; layer: string; type: SnoekStructureType; label: string }> = [
  { id: 4, layer: 'brug_beweegbaar', type: 'bridge', label: 'Beweegbare brug' },
  { id: 5, layer: 'brug_vast', type: 'bridge', label: 'Vaste brug' },
  { id: 8, layer: 'duiker', type: 'culvert', label: 'Duiker' },
  { id: 11, layer: 'gemaal', type: 'pumping_station', label: 'Gemaal' },
  { id: 17, layer: 'keersluis', type: 'lock', label: 'Keersluis' },
  { id: 23, layer: 'schutsluis', type: 'lock', label: 'Schutsluis' },
  { id: 24, layer: 'sifon', type: 'siphon', label: 'Sifon' },
  { id: 25, layer: 'spuisluis', type: 'lock', label: 'Spuisluis' },
  { id: 27, layer: 'stuw', type: 'weir', label: 'Stuw' },
  { id: 31, layer: 'waterreguleringswerk', type: 'water_control', label: 'Waterregulering' }
];

function toNumber(value: unknown, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function normalizeBbox(query: any): SnoekStructuresBbox {
  const bbox = {
    west: toNumber(query?.west, DEFAULT_BBOX.west),
    south: toNumber(query?.south, DEFAULT_BBOX.south),
    east: toNumber(query?.east, DEFAULT_BBOX.east),
    north: toNumber(query?.north, DEFAULT_BBOX.north)
  };

  if (bbox.west >= bbox.east || bbox.south >= bbox.north) {
    return DEFAULT_BBOX;
  }

  return bbox;
}

function projectToMap(lon: number, lat: number, bbox: SnoekStructuresBbox) {
  const west = lonToMercatorX(bbox.west);
  const east = lonToMercatorX(bbox.east);
  const south = latToMercatorY(bbox.south);
  const north = latToMercatorY(bbox.north);
  const x = lonToMercatorX(lon);
  const y = latToMercatorY(lat);

  return {
    x: clamp(((x - west) / (east - west)) * 100, 0, 100),
    y: clamp((1 - ((y - south) / (north - south))) * 100, 0, 100)
  };
}

function lonToMercatorX(lon: number): number {
  return 6378137 * lon * Math.PI / 180;
}

function latToMercatorY(lat: number): number {
  const radians = lat * Math.PI / 180;
  return 6378137 * Math.log(Math.tan(Math.PI / 4 + radians / 2));
}

function collectCoordinates(geometry: any): Array<[number, number]> {
  const points: Array<[number, number]> = [];

  function walk(value: any) {
    if (!Array.isArray(value)) return;
    if (value.length >= 2 && typeof value[0] === 'number' && typeof value[1] === 'number') {
      points.push([value[0], value[1]]);
      return;
    }
    value.forEach(walk);
  }

  walk(geometry?.coordinates);
  return points.filter(([lon, lat]) => Number.isFinite(lon) && Number.isFinite(lat));
}

function centroid(geometry: any): { lon: number; lat: number } | undefined {
  const points = collectCoordinates(geometry);
  if (!points.length) return undefined;
  const totals = points.reduce((acc, [lon, lat]) => {
    acc.lon += lon;
    acc.lat += lat;
    return acc;
  }, { lon: 0, lat: 0 });

  return {
    lon: totals.lon / points.length,
    lat: totals.lat / points.length
  };
}

function firstText(...values: unknown[]): string {
  for (const value of values) {
    const text = String(value || '').trim();
    if (text && text.toLowerCase() !== 'naam onbekend') return text;
  }
  return '';
}

function scoreStructure(type: SnoekStructureType, props: any): { score: number; reasons: string[] } {
  const reasons: string[] = [];
  let score = 45;

  const add = (points: number, reason: string) => {
    score += points;
    reasons.push(reason);
  };

  if (type === 'pumping_station') add(34, 'stroming, zuurstof en aasvis rond gemaal/pomp');
  if (type === 'culvert') add(28, 'versmalling en hinderlaag bij duiker');
  if (type === 'weir') add(26, 'stroming en hoogteverschil bij stuw');
  if (type === 'lock') add(23, 'stroming, harde randen en schaduw bij sluis');
  if (type === 'bridge') add(18, 'schaduw en harde structuur bij brug');
  if (type === 'fish_passage') add(18, 'vismigratie en stroming bij vispassage');
  if (type === 'siphon') add(15, 'onderdoorgang/vernauwing bij sifon');
  if (type === 'trash_rack') add(13, 'vuilvang verzamelt stroming en klein aas');
  if (type === 'gate' || type === 'water_control') add(12, 'waterregeling geeft stromingskans');
  if (type === 'drop') add(16, 'bodemval geeft diepte- en stroomovergang');

  const status = String(props?.statusobject || '').toLowerCase();
  if (status.includes('gerealiseerd')) add(3, 'object staat als gerealiseerd geregistreerd');

  const peil = String(props?.indpeilregulpeilscheidend || '').toLowerCase();
  if (peil === 'ja') add(4, 'peilscheidend object kan waterbeweging concentreren');

  const opening = Number(props?.breedteopening || props?.hoogteopening || 0);
  if (Number.isFinite(opening) && opening > 0) add(2, 'opening/doorstroming is geregistreerd');

  return {
    score: Math.round(clamp(score, 0, 100)),
    reasons: reasons.slice(0, 4)
  };
}

async function fetchJson(url: string): Promise<any> {
  const response = await fetch(url, {
    headers: {
      'User-Agent': 'snoek-ai-scout/1.0'
    }
  });

  if (!response.ok) {
    throw new Error(`Snoek structures request failed: ${response.status}`);
  }

  return response.json();
}

async function fetchPdokLayer(layer: typeof PDOK_TYPES[number], bbox: SnoekStructuresBbox): Promise<SnoekStructure[]> {
  const query = new URLSearchParams({
    service: 'WFS',
    version: '2.0.0',
    request: 'GetFeature',
    typeNames: `waterschappen-kunstwerken-imwa:${layer.layer}`,
    // WFS 2.0 EPSG:4326 expects axis order lat,lon here; output coordinates are CRS84 lon,lat.
    bbox: `${bbox.south},${bbox.west},${bbox.north},${bbox.east},EPSG:4326`,
    srsName: 'EPSG:4326',
    count: '1200',
    outputFormat: 'application/json'
  });
  const payload = await fetchJson(`https://service.pdok.nl/hwh/waterschappen-kunstwerken-imwa/wfs/v2_0?${query}`);
  const features = Array.isArray(payload?.features) ? payload.features : [];

  return features.map((feature: any, index: number) => {
    const center = centroid(feature.geometry);
    if (!center) return undefined;
    const props = feature.properties || {};
    const score = scoreStructure(layer.type, props);
    const point = projectToMap(center.lon, center.lat, bbox);
    const code = firstText(props.naam, props.code, props.nen3610id, feature.id);
    const name = code ? `${layer.label} ${code}` : layer.label;

    return {
      id: `pdok-${layer.layer}-${feature.id || index}`,
      type: layer.type,
      source: 'pdok-imwa',
      sourceLayer: layer.layer,
      name,
      label: layer.label,
      lat: center.lat,
      lon: center.lon,
      x: point.x,
      y: point.y,
      score: score.score,
      reasons: score.reasons
    };
  }).filter(Boolean) as SnoekStructure[];
}

async function fetchRwsLayer(layer: typeof RWS_LAYERS[number], bbox: SnoekStructuresBbox): Promise<SnoekStructure[]> {
  const query = new URLSearchParams({
    f: 'geojson',
    where: '1=1',
    outFields: 'naam,complex_naam,beheerobjectsoort,aard',
    geometry: `${bbox.west},${bbox.south},${bbox.east},${bbox.north}`,
    geometryType: 'esriGeometryEnvelope',
    inSR: '4326',
    outSR: '4326',
    spatialRel: 'esriSpatialRelIntersects',
    returnGeometry: 'true',
    resultRecordCount: '2000'
  });
  const payload = await fetchJson(`https://geo.rijkswaterstaat.nl/arcgis/rest/services/GDR/disk_beheerobjecten/FeatureServer/${layer.id}/query?${query}`);
  const features = Array.isArray(payload?.features) ? payload.features : [];

  return features.map((feature: any, index: number) => {
    const center = centroid(feature.geometry);
    if (!center) return undefined;
    const props = feature.properties || {};
    const score = scoreStructure(layer.type, props);
    const point = projectToMap(center.lon, center.lat, bbox);
    const code = firstText(props.naam, props.complex_naam, props.beheerobjectsoort);
    const name = code ? `${layer.label} ${code}` : layer.label;

    return {
      id: `rws-${layer.layer}-${index}-${center.lon.toFixed(5)}-${center.lat.toFixed(5)}`,
      type: layer.type,
      source: 'rws-arcgis',
      sourceLayer: layer.layer,
      name,
      label: layer.label,
      lat: center.lat,
      lon: center.lon,
      x: point.x,
      y: point.y,
      score: score.score,
      reasons: score.reasons
    };
  }).filter(Boolean) as SnoekStructure[];
}

function dedupeStructures(structures: SnoekStructure[]): SnoekStructure[] {
  const seen = new Set<string>();
  return structures.filter((structure) => {
    const key = `${structure.type}:${structure.lon.toFixed(4)}:${structure.lat.toFixed(4)}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function countByType(structures: SnoekStructure[]): Record<string, number> {
  return structures.reduce((counts, structure) => {
    counts[structure.type] = (counts[structure.type] || 0) + 1;
    return counts;
  }, {} as Record<string, number>);
}

export async function getSnoekStructures(query: any = {}): Promise<SnoekStructuresResult> {
  const bbox = normalizeBbox(query);
  const limit = Math.round(clamp(toNumber(query.limit, 3200), 100, 5000));
  const results = await Promise.all([
    ...PDOK_TYPES.map((layer) => fetchPdokLayer(layer, bbox).catch(() => [])),
    ...RWS_LAYERS.map((layer) => fetchRwsLayer(layer, bbox).catch(() => []))
  ]);
  const structures = dedupeStructures(results.reduce((all, layerResults) => all.concat(layerResults), [] as SnoekStructure[]))
    .sort((a, b) => b.score - a.score || a.label.localeCompare(b.label));

  return {
    ok: true,
    bbox,
    total: structures.length,
    rendered: Math.min(structures.length, limit),
    structures: structures.slice(0, limit),
    counts: countByType(structures),
    sources: [
      {
        id: 'pdok-imwa',
        label: 'PDOK Waterschappen Kunstwerken IMWA',
        attribution: 'Waterschappen Kunstwerken IMWA via PDOK, CC0.'
      },
      {
        id: 'rws-arcgis',
        label: 'Rijkswaterstaat ArcGIS beheerobjecten',
        attribution: 'Rijkswaterstaat GDR beheerobjecten FeatureServer.'
      }
    ]
  };
}
