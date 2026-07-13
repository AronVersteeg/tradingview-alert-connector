export type SnoekStructureType =
  | 'pumping_station'
  | 'bridge'
  | 'weir'
  | 'lock'
  | 'fish_passage'
  | 'culvert'
  | 'siphon'
  | 'well'
  | 'coupure'
  | 'treatment_plant'
  | 'ford'
  | 'fixed_dam'
  | 'sediment_trap'
  | 'aqueduct'
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
  source: 'pdok-imwa';
  sourceLayer: string;
  name: string;
  label: string;
  lat: number;
  lon: number;
  geometry: any;
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
  hotspots: SnoekStructure[];
  counts: Record<string, number>;
  rawCounts: Record<string, number>;
  sources: Array<{
    id: string;
    label: string;
    attribution: string;
  }>;
};

const DEFAULT_BBOX: SnoekStructuresBbox = {
  west: 4.54,
  south: 52.355,
  east: 4.82,
  north: 52.505
};

const PDOK_TYPES: Array<{ layer: string; type: SnoekStructureType; label: string }> = [
  { layer: 'brug', type: 'bridge', label: 'Brug' },
  { layer: 'coupure', type: 'coupure', label: 'Coupure' },
  { layer: 'sluis', type: 'lock', label: 'Sluis' },
  { layer: 'verbeterinstallatie', type: 'treatment_plant', label: 'Verbeterinstallatie' },
  { layer: 'hevel', type: 'siphon', label: 'Hevel' },
  { layer: 'afsluitmiddel', type: 'gate', label: 'Afsluitmiddel' },
  { layer: 'stuw', type: 'weir', label: 'Stuw' },
  { layer: 'voorde', type: 'ford', label: 'Voorde' },
  { layer: 'sifon', type: 'siphon', label: 'Sifon' },
  { layer: 'bodemval', type: 'drop', label: 'Bodemval' },
  { layer: 'put', type: 'well', label: 'Put' },
  { layer: 'duiker', type: 'culvert', label: 'Duiker' },
  { layer: 'vispassage', type: 'fish_passage', label: 'Vispassage' },
  { layer: 'vastedam', type: 'fixed_dam', label: 'VasteDam' },
  { layer: 'zandvang', type: 'sediment_trap', label: 'Zandvang' },
  { layer: 'aquaduct', type: 'aqueduct', label: 'Aquaduct' },
  { layer: 'gemaal', type: 'pumping_station', label: 'Gemaal' },
  { layer: 'vuilvang', type: 'trash_rack', label: 'Vuilvang' }
];

const DEFAULT_PDOK_LAYERS = new Set(['brug', 'sluis', 'stuw', 'vispassage', 'gemaal']);
const PDOK_PAGE_SIZE = 1000;

const COMMUNITY_SIGNALS = [
  {
    name: 'De Ven / Velsen-Zuid',
    lat: 52.4549,
    lon: 4.6642,
    boost: 9,
    species: ['snoek'],
    tactics: ['shad', 'spinnerbait', 'korte worpen langs riet'],
    note: 'community/eigen scout seed: modelbouwplas, riet, korte sessies en roofvis langs obstakels'
  },
  {
    name: 'Buitenhuizerplas windkant',
    lat: 52.42914,
    lon: 4.70786,
    boost: 8,
    species: ['snoek'],
    tactics: ['shallow shad', 'jerkbait', 'windkant afvissen'],
    note: 'community seed: windkant en randen interessanter dan open water'
  },
  {
    name: 'Noordzeekanaal / Zijkanaal B',
    lat: 52.439069,
    lon: 4.6818906,
    boost: 10,
    species: ['snoek', 'snoekbaars'],
    tactics: ['shad op talud', 'dropshot bij kade', 'stromingsnaad afvissen'],
    note: 'community seed: kanaal, talud, kade en stromingsnaden'
  },
  {
    name: 'Spaarndam / Mooie Nel',
    lat: 52.4129566,
    lon: 4.6814088,
    boost: 7,
    species: ['snoek'],
    tactics: ['softbait langs riet', 'brugschaduw', 'overgangen afvissen'],
    note: 'community seed: bruggen, riet en overgangen bij kleiner water'
  },
  {
    name: 'Zijkanaal C',
    lat: 52.4212,
    lon: 4.693,
    boost: 16,
    species: ['snoek', 'snoekbaars'],
    tactics: ['softbait', 'dropshot', 'talud en kade langzaam afvissen'],
    note: 'Engelhart hengelsport advies: genoemd als relevante roofvisstek'
  },
  {
    name: 'Brug over de A9 / Zijkanaal C',
    lat: 52.4212,
    lon: 4.693,
    boost: 14,
    species: ['snoek', 'snoekbaars'],
    tactics: ['dropshot onder brugschaduw', 'shad langs pijlers', 'langzaam tegen bodem'],
    note: 'Engelhart hengelsport advies: brugstructuur bij A9 als roofvisstek'
  },
  {
    name: 'Sluis Spaarndam',
    lat: 52.4129566,
    lon: 4.6814088,
    boost: 18,
    species: ['snoek', 'snoekbaars'],
    tactics: ['dropshot bij stroming', 'shad langs harde rand', 'werpend langs sluisdeuren'],
    note: 'Engelhart hengelsport advies: sluis/stroming genoemd als interessante roofvisplek'
  },
  {
    name: 'Pontje Velsen-Zuid',
    lat: 52.4626581,
    lon: 4.6323097,
    boost: 22,
    species: ['snoekbaars'],
    tactics: ['dropshot', 'shad op bodem', 'stromingsnaad van pontje afvissen'],
    note: 'Engelhart hengelsport advies: positief voor roofvissen; veel snoekbaars rond pontstroming'
  },
  {
    name: 'Steiger Oud Velsen',
    lat: 52.4605,
    lon: 4.631,
    boost: 18,
    species: ['snoekbaars'],
    tactics: ['dropshot langs steiger', 'kleine shad', 'schemer/avond'],
    note: 'Engelhart hengelsport advies: vanaf pontje richting steiger bij Oud Velsen interessant'
  },
  {
    name: 'Pontje Buitenhuizen',
    lat: 52.433,
    lon: 4.7255,
    boost: 18,
    species: ['snoekbaars', 'snoek'],
    tactics: ['dropshot bij stroming', 'shad langs talud', 'korte drift langs kade'],
    note: 'Engelhart hengelsport advies: pontstroming genoemd als roofvisstek'
  },
  {
    name: 'Sluizen IJmuiden richting zee',
    lat: 52.464,
    lon: 4.601,
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
  if (type === 'weir') add(34, 'stroming en hoogteverschil bij stuw');
  if (type === 'lock') add(30, 'stroming, harde randen en schaduw bij sluis');
  if (type === 'bridge') add(18, 'schaduw en harde structuur bij brug');
  if (type === 'fish_passage') add(24, 'vismigratie en stroming bij vispassage');
  if (type === 'culvert') add(14, 'vernauwing en mogelijke lokale waterbeweging bij duiker');
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
  const features: any[] = [];
  let startIndex = 0;

  while (true) {
    const query = new URLSearchParams({
      service: 'WFS',
      version: '2.0.0',
      request: 'GetFeature',
      typeNames: `waterschappen-kunstwerken-imwa:${layer.layer}`,
      // WFS 2.0 EPSG:4326 expects axis order lat,lon here; output coordinates are CRS84 lon,lat.
      bbox: `${bbox.south},${bbox.west},${bbox.north},${bbox.east},EPSG:4326`,
      srsName: 'EPSG:4326',
      count: String(PDOK_PAGE_SIZE),
      startIndex: String(startIndex),
      outputFormat: 'application/json'
    });
    const payload = await fetchJson(`https://service.pdok.nl/hwh/waterschappen-kunstwerken-imwa/wfs/v2_0?${query}`);
    const page = Array.isArray(payload?.features) ? payload.features : [];
    if (!page.length) break;
    features.push(...page);
    // PDOK can return fewer than `count` before the final page, so advance by
    // the requested WFS offset until an actually empty page is reached.
    startIndex += PDOK_PAGE_SIZE;
  }

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
      geometry: feature.geometry,
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
    const key = structure.id;
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

const HOTSPOT_TYPES: SnoekStructureType[] = ['pumping_station', 'weir', 'lock', 'bridge', 'fish_passage', 'culvert'];

function hotspotTypeLabel(type: SnoekStructureType): string {
  if (type === 'pumping_station') return 'gemaal/pomp';
  if (type === 'weir') return 'stuw';
  if (type === 'lock') return 'sluis';
  if (type === 'bridge') return 'brug';
  if (type === 'fish_passage') return 'vispassage';
  if (type === 'culvert') return 'duiker';
  return type;
}

function requestedPdokTypes(query: any): typeof PDOK_TYPES {
  const requested = String(query?.layers || '').trim().toLowerCase();
  if (requested === 'none') return [];
  if (!requested) return PDOK_TYPES.filter((layer) => DEFAULT_PDOK_LAYERS.has(layer.layer));
  const names = new Set(requested.split(',').map((name) => name.trim()).filter(Boolean));
  return PDOK_TYPES.filter((layer) => names.has(layer.layer));
}

function countByLayer(structures: SnoekStructure[]): Record<string, number> {
  return structures.reduce((counts, structure) => {
    counts[structure.sourceLayer] = (counts[structure.sourceLayer] || 0) + 1;
    return counts;
  }, {} as Record<string, number>);
}

function selectBalancedHotspots(hotspots: SnoekStructure[], limit: number): SnoekStructure[] {
  const quotas: Record<string, number> = {
    pumping_station: Math.floor(limit * 0.3),
    weir: Math.floor(limit * 0.25),
    lock: Math.floor(limit * 0.15),
    bridge: Math.floor(limit * 0.15),
    fish_passage: Math.floor(limit * 0.1),
    culvert: limit
  };
  quotas.culvert -= quotas.pumping_station + quotas.weir + quotas.lock + quotas.bridge + quotas.fish_passage;

  const selected: SnoekStructure[] = [];
  HOTSPOT_TYPES.forEach((type) => {
    selected.push(...hotspots
      .filter((hotspot) => hotspot.type === type)
      .sort((a, b) => b.score - a.score)
      .slice(0, quotas[type] || 0));
  });

  if (selected.length < limit) {
    const selectedIds = new Set(selected.map((hotspot) => hotspot.id));
    selected.push(...hotspots
      .filter((hotspot) => !selectedIds.has(hotspot.id))
      .sort((a, b) => b.score - a.score)
      .slice(0, limit - selected.length));
  }

  return selected.sort((a, b) => b.score - a.score).slice(0, limit);
}

export function buildScoutHotspots(structures: SnoekStructure[], limit: number): SnoekStructure[] {
  const gridSize = 1.2;
  const clusters = new Map<string, SnoekStructure[]>();

  structures
    .filter((structure) => HOTSPOT_TYPES.includes(structure.type))
    .forEach((structure) => {
      const key = `${Math.floor(structure.x / gridSize)}:${Math.floor(structure.y / gridSize)}`;
      const bucket = clusters.get(key) || [];
      bucket.push(structure);
      clusters.set(key, bucket);
    });

  const hotspots: SnoekStructure[] = [];
  clusters.forEach((items, key) => {
    const types = Array.from(new Set(items.map((item) => item.type)));
    const hasCurrentMaker = types.some((type) => (
      type === 'pumping_station' ||
      type === 'weir' ||
      type === 'lock' ||
      type === 'fish_passage'
    ));
    const lat = items.reduce((sum, item) => sum + item.lat, 0) / items.length;
    const lon = items.reduce((sum, item) => sum + item.lon, 0) / items.length;
    const community = nearestCommunitySignal(lat, lon, 1.2);

    HOTSPOT_TYPES.forEach((type) => {
      const typedItems = items.filter((item) => item.type === type);
      if (!typedItems.length) return;

      // A bridge is useful context, but only becomes a hotspot near flow or strong local evidence.
      if (type === 'bridge' && !hasCurrentMaker && !community) return;
      // Culverts stay a low-priority context layer unless another signal supports them.
      if (type === 'culvert' && !hasCurrentMaker && !community) return;

      const best = typedItems.slice().sort((a, b) => b.score - a.score)[0];
      const densityBoost = Math.min(10, Math.max(0, typedItems.length - 1) * 2);
      const contextBoost = type === 'bridge'
        ? hasCurrentMaker ? 14 : 7
        : type === 'culvert'
          ? hasCurrentMaker ? 12 : 6
        : types.length > 1 ? 6 : 2;
      const communityBoost = community ? Math.min(10, community.boost) : 0;
      const score = Math.round(clamp(best.score + densityBoost + contextBoost + communityBoost - 10, 0, 100));
      const minimumScore = type === 'bridge'
        ? 66
        : type === 'culvert'
          ? 60
          : type === 'fish_passage'
            ? 62
            : type === 'lock' ? 70 : 72;
      if (score < minimumScore) return;

      const otherTypes = types
        .filter((itemType) => itemType !== type)
        .map(hotspotTypeLabel)
        .slice(0, 3);
      const reasons = [
        `GIS: ${typedItems.length} ${hotspotTypeLabel(type)}-objecten in dit kaartvak`,
        type === 'pumping_station' ? 'Stromingslogica: gemaal/pomp kan zuurstof en aasvis concentreren' : '',
        type === 'weir' ? 'Stromingslogica: verval en waterbeweging bij de stuw' : '',
        type === 'lock' ? 'Stromingslogica: harde randen, luwte en schut- of spuibeweging' : '',
        type === 'bridge' ? 'Structuurlogica: schaduw en harde randen, met stroming of lokale bevestiging dichtbij' : '',
        type === 'fish_passage' ? 'Migratielogica: vispassage bundelt visbeweging en vaak ook lokale stroming' : '',
        type === 'culvert' ? 'Contextlogica: duiker telt alleen mee met stroming of lokale praktijkindicatie dichtbij' : '',
        otherTypes.length ? `Nabije GIS-context: ${otherTypes.join(', ')}` : '',
        community ? `Lokale praktijk: ${community.note}` : 'Community: nog geen sterke lokale bevestiging'
      ].filter(Boolean);

      hotspots.push({
        id: `hotspot-${type}-${key}`,
        type,
        source: best.source,
        sourceLayer: best.sourceLayer,
        name: community ? `${community.name} - ${best.label}` : `Snoekspot ${best.name}`,
        label: best.label,
        lat: best.lat,
        lon: best.lon,
        geometry: best.geometry,
        x: best.x,
        y: best.y,
        score,
        reasons
      });
    });
  });

  return selectBalancedHotspots(hotspots, limit);
}

function distanceKm(latA: number, lonA: number, latB: number, lonB: number): number {
  const toRadians = (value: number) => value * Math.PI / 180;
  const earthRadiusKm = 6371;
  const dLat = toRadians(latB - latA);
  const dLon = toRadians(lonB - lonA);
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(toRadians(latA)) * Math.cos(toRadians(latB)) * Math.sin(dLon / 2) ** 2;
  return earthRadiusKm * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function nearestCommunitySignal(
  lat: number,
  lon: number,
  maxDistanceKm = 3.2
): typeof COMMUNITY_SIGNALS[number] | undefined {
  let best: { signal: typeof COMMUNITY_SIGNALS[number]; distance: number } | undefined;
  COMMUNITY_SIGNALS.forEach((signal) => {
    const distance = distanceKm(lat, lon, signal.lat, signal.lon);
    if (distance <= maxDistanceKm && (!best || distance < best.distance)) {
      best = { signal, distance };
    }
  });
  return best?.signal;
}

export async function getSnoekStructures(query: any = {}): Promise<SnoekStructuresResult> {
  const bbox = normalizeBbox(query);
  const limit = Math.round(clamp(toNumber(query.limit, 120), 30, 300));
  const requestedLayers = requestedPdokTypes(query);
  const results = await Promise.all([
    ...requestedLayers.map((layer) => fetchPdokLayer(layer, bbox))
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
    structures,
    hotspots,
    counts: countByType(hotspots),
    rawCounts: countByLayer(structures),
    sources: [
      {
        id: 'pdok-imwa',
        label: 'PDOK Waterschappen Kunstwerken IMWA',
        attribution: 'Waterschappen Kunstwerken IMWA via PDOK, CC0.'
      }
    ]
  };
}
