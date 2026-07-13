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
  rawTotal: number;
  total: number;
  rendered: number;
  structures: SnoekStructure[];
  counts: Record<string, number>;
  rawCounts: Record<string, number>;
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

const COMMUNITY_SIGNALS = [
  {
    name: 'De Ven / Spaarnwoude',
    x: 52,
    y: 63,
    boost: 9,
    species: ['snoek'],
    tactics: ['shad', 'spinnerbait', 'korte worpen langs riet'],
    note: 'community/eigen scout seed: riet, korte sessies en roofvis langs obstakels'
  },
  {
    name: 'Buitenhuizerplas windkant',
    x: 61,
    y: 50,
    boost: 8,
    species: ['snoek'],
    tactics: ['shallow shad', 'jerkbait', 'windkant afvissen'],
    note: 'community seed: windkant en randen interessanter dan open water'
  },
  {
    name: 'Noordzeekanaal / Zijkanaal B',
    x: 38,
    y: 36,
    boost: 10,
    species: ['snoek', 'snoekbaars'],
    tactics: ['shad op talud', 'dropshot bij kade', 'stromingsnaad afvissen'],
    note: 'community seed: kanaal, talud, kade en stromingsnaden'
  },
  {
    name: 'Spaarndam / Mooie Nel',
    x: 38,
    y: 72,
    boost: 7,
    species: ['snoek'],
    tactics: ['softbait langs riet', 'brugschaduw', 'overgangen afvissen'],
    note: 'community seed: bruggen, riet en overgangen bij kleiner water'
  },
  {
    name: 'Zijkanaal C',
    x: 37.9,
    y: 57,
    boost: 16,
    species: ['snoek', 'snoekbaars'],
    tactics: ['softbait', 'dropshot', 'talud en kade langzaam afvissen'],
    note: 'Engelhart hengelsport advies: genoemd als relevante roofvisstek'
  },
  {
    name: 'Brug over de A9 / Zijkanaal C',
    x: 33.3,
    y: 60,
    boost: 14,
    species: ['snoek', 'snoekbaars'],
    tactics: ['dropshot onder brugschaduw', 'shad langs pijlers', 'langzaam tegen bodem'],
    note: 'Engelhart hengelsport advies: brugstructuur bij A9 als roofvisstek'
  },
  {
    name: 'Sluis Spaarndam',
    x: 44.2,
    y: 63.1,
    boost: 18,
    species: ['snoek', 'snoekbaars'],
    tactics: ['dropshot bij stroming', 'shad langs harde rand', 'werpend langs sluisdeuren'],
    note: 'Engelhart hengelsport advies: sluis/stroming genoemd als interessante roofvisplek'
  },
  {
    name: 'Pontje Velsen-Zuid',
    x: 20.8,
    y: 22.3,
    boost: 22,
    species: ['snoekbaars'],
    tactics: ['dropshot', 'shad op bodem', 'stromingsnaad van pontje afvissen'],
    note: 'Engelhart hengelsport advies: positief voor roofvissen; veel snoekbaars rond pontstroming'
  },
  {
    name: 'Steiger Oud Velsen',
    x: 23.7,
    y: 27.7,
    boost: 18,
    species: ['snoekbaars'],
    tactics: ['dropshot langs steiger', 'kleine shad', 'schemer/avond'],
    note: 'Engelhart hengelsport advies: vanaf pontje richting steiger bij Oud Velsen interessant'
  },
  {
    name: 'Pontje Buitenhuizen',
    x: 44.6,
    y: 33.1,
    boost: 18,
    species: ['snoekbaars', 'snoek'],
    tactics: ['dropshot bij stroming', 'shad langs talud', 'korte drift langs kade'],
    note: 'Engelhart hengelsport advies: pontstroming genoemd als roofvisstek'
  },
  {
    name: 'Sluizen IJmuiden richting zee',
    x: 11.3,
    y: 25.4,
    boost: 15,
    species: ['snoekbaars', 'zeebaars'],
    tactics: ['shad bij stroming', 'dropshot luwte', 'zeebaars meer richting sluizen/zee'],
    note: 'Engelhart hengelsport advies: meer richting sluizen wordt zeebaars interessanter'
  }
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

  if (type === 'pumping_station') add(46, 'actieve stroming, zuurstof en aasvis rond gemaal/pomp');
  if (type === 'culvert') add(14, 'versmalling en hinderlaag bij duiker');
  if (type === 'weir') add(34, 'stroming en hoogteverschil bij stuw');
  if (type === 'lock') add(30, 'stroming, harde randen en schaduw bij sluis');
  if (type === 'bridge') add(18, 'schaduw en harde structuur bij brug');
  if (type === 'fish_passage') add(24, 'vismigratie en stroming bij vispassage');
  if (type === 'siphon') add(10, 'onderdoorgang/vernauwing bij sifon');
  if (type === 'trash_rack') add(13, 'vuilvang verzamelt stroming en klein aas');
  if (type === 'gate' || type === 'water_control') add(22, 'waterregeling geeft stromingskans');
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

function distanceToSegment(px: number, py: number, ax: number, ay: number, bx: number, by: number): number {
  const dx = bx - ax;
  const dy = by - ay;
  if (dx === 0 && dy === 0) return Math.hypot(px - ax, py - ay);
  const t = clamp(((px - ax) * dx + (py - ay) * dy) / ((dx * dx) + (dy * dy)), 0, 1);
  return Math.hypot(px - (ax + (t * dx)), py - (ay + (t * dy)));
}

function distanceToWaterLayout(x: number, y: number): number {
  const waterLines = [
    [[0, 24], [25, 26], [45, 35], [66, 46], [100, 51]],
    [[44, 42], [42, 56], [39, 72], [35, 100]],
    [[66, 46], [74, 52], [80, 60], [92, 72]],
    [[20, 76], [34, 70], [47, 66], [58, 66]]
  ];
  let best = Infinity;
  waterLines.forEach((line) => {
    for (let i = 1; i < line.length; i += 1) {
      best = Math.min(best, distanceToSegment(x, y, line[i - 1][0], line[i - 1][1], line[i][0], line[i][1]));
    }
  });
  return best;
}

function buildScoutHotspots(structures: SnoekStructure[], limit: number): SnoekStructure[] {
  const gridSize = 2.8;
  const clusters = new Map<string, SnoekStructure[]>();

  structures
    .filter((structure) => structure.score >= 58 || structure.type === 'culvert' || structure.type === 'bridge')
    .forEach((structure) => {
      const key = `${Math.floor(structure.x / gridSize)}:${Math.floor(structure.y / gridSize)}`;
      const bucket = clusters.get(key) || [];
      bucket.push(structure);
      clusters.set(key, bucket);
    });

  const hotspots: SnoekStructure[] = [];
  clusters.forEach((items, key) => {
    const types = Array.from(new Set(items.map((item) => item.type)));
    const hasPrimeStructure = types.some((type) => (
      type === 'pumping_station' ||
      type === 'weir' ||
      type === 'lock' ||
      type === 'fish_passage' ||
      type === 'water_control'
    ));
    const hasCurrentMaker = types.some((type) => (
      type === 'pumping_station' ||
      type === 'weir' ||
      type === 'lock' ||
      type === 'water_control'
    ));
    const hasAmbushCombo = types.includes('culvert') && (types.includes('bridge') || types.includes('gate') || types.includes('weir'));
    const x = items.reduce((sum, item) => sum + item.x, 0) / items.length;
    const y = items.reduce((sum, item) => sum + item.y, 0) / items.length;
    const waterDistance = distanceToWaterLayout(x, y);
    const nearMainWater = waterDistance <= 5.5;
    const community = nearestCommunitySignal(x, y);
    const keep = hasCurrentMaker || (hasAmbushCombo && nearMainWater) || (hasPrimeStructure && nearMainWater) || (items.length >= 6 && types.length >= 3 && nearMainWater);

    if (!keep) return;

    const best = items.slice().sort((a, b) => b.score - a.score)[0];
    const densityBoost = Math.min(18, items.length * 2);
    const typeBoost = Math.min(20, types.length * 7);
    const waterBoost = waterDistance <= 2.5 ? 14 : waterDistance <= 5.5 ? 8 : 0;
    const comboBoost = hasCurrentMaker ? 18 : hasAmbushCombo ? 8 : hasPrimeStructure ? 10 : 0;
    const communityBoost = community ? community.boost : 0;
    const score = Math.round(clamp(best.score + densityBoost + typeBoost + waterBoost + comboBoost + communityBoost - 18, 0, 100));

    if (score < 68) return;

    const typeLabels = types.map((type) => {
      if (type === 'pumping_station') return 'gemaal';
      if (type === 'culvert') return 'duiker';
      if (type === 'bridge') return 'brug';
      if (type === 'weir') return 'stuw';
      if (type === 'lock') return 'sluis';
      return type;
    });
    const reasons = [
      `GIS: ${items.length} objecten in een klein vak (${typeLabels.slice(0, 4).join(', ')})`,
      nearMainWater ? 'Waterlayout: nabij hoofdwater, kruising of overgang' : '',
      hasAmbushCombo ? 'Snoeklogica: duiker telt mee als hinderlaag, niet als hoofdreden' : '',
      hasCurrentMaker ? 'Snoeklogica: stroming/waterregeling kan aasvis concentreren' : '',
      community ? `Lokale praktijk: ${community.note}` : 'Community: nog geen sterke lokale bevestiging',
      community ? `Soort/techniek: ${community.species.join(', ')} - ${community.tactics.join(', ')}` : ''
    ].filter(Boolean);

    hotspots.push({
      id: `hotspot-${key}`,
      type: best.type,
      source: best.source,
      sourceLayer: 'scout-hotspot',
      name: community ? `Snoekspot ${community.name} - ${best.label}` : `Snoekspot ${best.label}`,
      label: 'Scout hotspot',
      lat: items.reduce((sum, item) => sum + item.lat, 0) / items.length,
      lon: items.reduce((sum, item) => sum + item.lon, 0) / items.length,
      x,
      y,
      score,
      reasons
    });
  });

  return hotspots
    .sort((a, b) => b.score - a.score)
    .slice(0, limit);
}

function nearestCommunitySignal(x: number, y: number): typeof COMMUNITY_SIGNALS[number] | undefined {
  let best: { signal: typeof COMMUNITY_SIGNALS[number]; distance: number } | undefined;
  COMMUNITY_SIGNALS.forEach((signal) => {
    const distance = Math.hypot(x - signal.x, y - signal.y);
    if (distance <= 12 && (!best || distance < best.distance)) {
      best = { signal, distance };
    }
  });
  return best?.signal;
}

export async function getSnoekStructures(query: any = {}): Promise<SnoekStructuresResult> {
  const bbox = normalizeBbox(query);
  const limit = Math.round(clamp(toNumber(query.limit, 120), 30, 300));
  const results = await Promise.all([
    ...PDOK_TYPES.map((layer) => fetchPdokLayer(layer, bbox).catch(() => [])),
    ...RWS_LAYERS.map((layer) => fetchRwsLayer(layer, bbox).catch(() => []))
  ]);
  const structures = dedupeStructures(results.reduce((all, layerResults) => all.concat(layerResults), [] as SnoekStructure[]))
    .sort((a, b) => b.score - a.score || a.label.localeCompare(b.label));
  const hotspots = buildScoutHotspots(structures, limit);

  return {
    ok: true,
    bbox,
    rawTotal: structures.length,
    total: hotspots.length,
    rendered: hotspots.length,
    structures: hotspots,
    counts: countByType(hotspots),
    rawCounts: countByType(structures),
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
