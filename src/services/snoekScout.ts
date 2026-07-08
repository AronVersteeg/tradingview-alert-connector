export type SnoekTarget = 'snoek' | 'method_feeder' | 'witvis';
export type PressureTrend = 'falling' | 'steady' | 'rising';
export type RainState = 'none' | 'light' | 'heavy';
export type TimeOfDay = 'morning' | 'midday' | 'evening' | 'night';

export type SnoekScoutInput = {
  target?: SnoekTarget;
  location?: string;
  temperatureC?: number | string;
  windBft?: number | string;
  cloudCoverPct?: number | string;
  pressureTrend?: PressureTrend;
  rain?: RainState;
  timeOfDay?: TimeOfDay;
};

export type SnoekSpot = {
  id: string;
  name: string;
  area: string;
  lat: number;
  lon: number;
  x: number;
  y: number;
  targets: SnoekTarget[];
  waterType: string;
  depthClass: string;
  structures: string[];
  note: string;
};

export type SnoekMapFeature = {
  id: string;
  layer: string;
  spotId?: string;
  name: string;
  type: 'depth' | 'bridge' | 'reed' | 'inlet' | 'parking' | 'review' | 'route';
  x: number;
  y: number;
  scoreBoost: number;
  description: string;
};

export type SnoekDataSource = {
  id: string;
  label: string;
  status: 'live' | 'seeded' | 'manual' | 'planned';
  use: string;
};

export type SnoekCommunityReview = {
  spotId: string;
  rating: number;
  source: string;
  note: string;
  baits: string[];
  confidence: 'laag' | 'middel' | 'hoog';
};

export type SnoekActivityWindow = {
  label: string;
  times: string[];
  note: string;
};

export type SnoekScoutResult = {
  ok: true;
  target: SnoekTarget;
  score: number;
  label: string;
  action: string;
  summary: string;
  reasons: string[];
  warnings: string[];
  tactics: string[];
  spots: SnoekSpot[];
  activityWindows: SnoekActivityWindow[];
  mapFeatures: SnoekMapFeature[];
  dataSources: SnoekDataSource[];
  communityReviews: SnoekCommunityReview[];
  input: Required<Omit<SnoekScoutInput, 'location'>> & { location: string };
};

const SPOTS: SnoekSpot[] = [
  {
    id: 'noordzeekanaal-velsen',
    name: 'Noordzeekanaal Velsen',
    area: 'Velsen',
    lat: 52.4635,
    lon: 4.6326,
    x: 27,
    y: 63,
    targets: ['snoek', 'witvis'],
    waterType: 'kanaal',
    depthClass: '1-6 m, talud naar hoofdgeul',
    structures: ['talud', 'kade', 'stromingsnaad'],
    note: 'Zoek windkant, stromingsnaden, steigers en harde overgangen.'
  },
  {
    id: 'buitenhuizerplas',
    name: 'Buitenhuizerplas',
    area: 'Spaarnwoude',
    lat: 52.4358,
    lon: 4.6831,
    x: 67,
    y: 51,
    targets: ['snoek', 'method_feeder', 'witvis'],
    waterType: 'plas',
    depthClass: 'ondiep tot middel, randen interessanter dan open water',
    structures: ['windkant', 'wier/rand', 'flauwe oever'],
    note: 'Interessant bij bewolking en wind op de kant; vis randen en wiergaten af.'
  },
  {
    id: 'de-ven',
    name: 'De Ven',
    area: 'Spaarnwoude',
    lat: 52.4329,
    lon: 4.6684,
    x: 55,
    y: 70,
    targets: ['method_feeder', 'witvis', 'snoek'],
    waterType: 'parkplas',
    depthClass: 'geschat 0,8-2 m',
    structures: ['riet', 'parkbrug', 'kommetje'],
    note: 'Rustig water voor method feeder; bij roofvis vooral zoeken bij riet en obstakels.'
  },
  {
    id: 'park-schoonenberg',
    name: 'Park Schoonenberg',
    area: 'Velsen-Zuid',
    lat: 52.4598,
    lon: 4.6478,
    x: 40,
    y: 42,
    targets: ['witvis', 'method_feeder', 'snoek'],
    waterType: 'parkvijver',
    depthClass: 'geschat ondiep tot 1,5 m',
    structures: ['parkvijver', 'schaduw', 'kleine stek'],
    note: 'Kleinschalig water: stil benaderen en compact voeren werkt vaak beter.'
  }
];

const MAP_FEATURES: SnoekMapFeature[] = [
  {
    id: 'zijkanaal-b-talud',
    layer: 'Esri/RWS bathymetrie seed',
    spotId: 'noordzeekanaal-velsen',
    name: 'Zijkanaal B talud',
    type: 'depth',
    x: 31,
    y: 60,
    scoreBoost: 14,
    description: 'Seedlaag uit de chat: oever 1-2 m, hoofdgeul mogelijk 3-6 m. Later vervangen door echte RWS/Esri bathymetrie.'
  },
  {
    id: 'buitenhuizerplas-windkant',
    layer: 'AI hotspot seed',
    spotId: 'buitenhuizerplas',
    name: 'Windkant Buitenhuizerplas',
    type: 'reed',
    x: 70,
    y: 49,
    scoreBoost: 10,
    description: 'Wind op de kant kan aasvis verzamelen; interessant bij bewolking en 2-4 Bft.'
  },
  {
    id: 'de-ven-riet',
    layer: 'Structuur seed',
    spotId: 'de-ven',
    name: 'Rietrand De Ven',
    type: 'reed',
    x: 57,
    y: 67,
    scoreBoost: 8,
    description: 'Riet, luwte en korte werpafstand; geschikt om method feeder en roofvis rustig te testen.'
  },
  {
    id: 'schoonenberg-schaduw',
    layer: 'Structuur seed',
    spotId: 'park-schoonenberg',
    name: 'Schaduwstek Schoonenberg',
    type: 'bridge',
    x: 42,
    y: 39,
    scoreBoost: 5,
    description: 'Kleiner water: schaduw en obstakels zijn belangrijker dan grote afstanden afvissen.'
  },
  {
    id: 'looproute-test',
    layer: 'Looproute seed',
    name: 'Korte scout-loop',
    type: 'route',
    x: 48,
    y: 56,
    scoreBoost: 0,
    description: 'Eerste routeconcept: begin bij structuur, loop daarna windkant en eindig bij rustiger water.'
  }
];

const DATA_SOURCES: SnoekDataSource[] = [
  {
    id: 'open-meteo',
    label: 'Open-Meteo weer en geocoding',
    status: 'live',
    use: 'Automatisch locatie, temperatuur, wind, bewolking, regen en luchtdruktrend invullen zonder API-key.'
  },
  {
    id: 'esri-rws-bathymetry',
    label: 'Esri / Rijkswaterstaat bathymetrie',
    status: 'planned',
    use: 'Echte dieptekaart voor Noordzeekanaal, zijkanalen en taluds.'
  },
  {
    id: 'ahn',
    label: 'AHN hoogte/oeverdata',
    status: 'planned',
    use: 'Oevertaluds, steile randen, bruggen en duikers herkennen waar bathymetrie ontbreekt.'
  },
  {
    id: 'osm',
    label: 'OpenStreetMap',
    status: 'seeded',
    use: 'Waterlijnen, paden, bruggen, parkeerplekken en bereikbaarheid.'
  },
  {
    id: 'visplanner',
    label: 'VISplanner',
    status: 'manual',
    use: 'Controleren waar je met VISpas mag vissen en welke regels gelden.'
  },
  {
    id: 'fishbrain-community',
    label: 'Fishbrain / FishAngler community reviews',
    status: 'manual',
    use: 'Vangstmeldingen, aaskeuze en lokale hints als inspiratie, niet als absolute waarheid.'
  },
  {
    id: 'windy-fishingpoints',
    label: 'Windy / Fishing Points stijl input',
    status: 'seeded',
    use: 'Wind, luchtdruk, bewolking, zon/maan en activiteitsscore.'
  },
  {
    id: 'satellite-water-reading',
    label: 'Satellietkaart / handmatig water lezen',
    status: 'manual',
    use: 'Riet, bruggen, schaduw, inhammen, duikers, donkere waterstukken en looproutes herkennen.'
  }
];

const COMMUNITY_REVIEWS: SnoekCommunityReview[] = [
  {
    spotId: 'noordzeekanaal-velsen',
    rating: 4,
    source: 'Community seed',
    note: 'Goed leerwater voor roofvis, vooral bij kade, bruggen, stroming en talud.',
    baits: ['shad 7-12 cm', 'jigkop', 'spinner'],
    confidence: 'middel'
  },
  {
    spotId: 'buitenhuizerplas',
    rating: 3,
    source: 'Community seed',
    note: 'Interessant als wind op de kant staat; grote open stukken overslaan zonder structuur.',
    baits: ['spinnerbait', 'shallow shad', 'jerkbait'],
    confidence: 'middel'
  },
  {
    spotId: 'de-ven',
    rating: 3,
    source: 'Eigen scout seed',
    note: 'Fijne testplek voor korte sessies en method feeder; roofvis vooral langs riet/obstakels.',
    baits: ['method feeder', 'kleine spinner', 'softbait'],
    confidence: 'laag'
  }
];

function toNumber(value: number | string | undefined, fallback: number): number {
  if (value === undefined || value === '') return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function normalizeEnum<T extends string>(value: unknown, allowed: readonly T[], fallback: T): T {
  return allowed.includes(value as T) ? value as T : fallback;
}

function normalizeInput(input: SnoekScoutInput) {
  return {
    target: normalizeEnum(input.target, ['snoek', 'method_feeder', 'witvis'], 'snoek'),
    location: String(input.location || 'Velsen / Spaarnwoude').trim() || 'Velsen / Spaarnwoude',
    temperatureC: toNumber(input.temperatureC, 20),
    windBft: toNumber(input.windBft, 3),
    cloudCoverPct: clamp(toNumber(input.cloudCoverPct, 70), 0, 100),
    pressureTrend: normalizeEnum(input.pressureTrend, ['falling', 'steady', 'rising'], 'steady'),
    rain: normalizeEnum(input.rain, ['none', 'light', 'heavy'], 'none'),
    timeOfDay: normalizeEnum(input.timeOfDay, ['morning', 'midday', 'evening', 'night'], 'evening')
  };
}

function add(reasons: string[], text: string, points: number): number {
  reasons.push(`${points > 0 ? '+' : ''}${points}: ${text}`);
  return points;
}

function scoreSnoek(input: ReturnType<typeof normalizeInput>, reasons: string[], warnings: string[]): number {
  let score = 50;
  if (input.windBft >= 2 && input.windBft <= 4) score += add(reasons, 'wind 2-4 Bft geeft beweging en dekking', 14);
  if (input.windBft === 0) score += add(reasons, 'windstil maakt roofvis vaak schuwer', -10);
  if (input.windBft >= 6) score += add(reasons, 'harde wind maakt werpen en stekcontrole lastig', -12);

  if (input.cloudCoverPct >= 60) score += add(reasons, 'bewolking haalt de felle zon van het water', 12);
  if (input.cloudCoverPct <= 20 && input.timeOfDay === 'midday') score += add(reasons, 'felle middagzon is vaak moeizaam', -14);

  if (input.pressureTrend === 'falling') score += add(reasons, 'dalende luchtdruk kan aasvis en roofvis activeren', 12);
  if (input.pressureTrend === 'rising') score += add(reasons, 'stijgende druk is minder overtuigend', -5);

  if (input.rain === 'light') score += add(reasons, 'lichte regen geeft extra dekking', 7);
  if (input.rain === 'heavy') score += add(reasons, 'zware regen maakt het onrustig en oncomfortabel', -8);

  if (input.temperatureC >= 8 && input.temperatureC <= 18) score += add(reasons, 'temperatuur zit mooi voor snoekactiviteit', 12);
  else if (input.temperatureC >= 19 && input.temperatureC <= 23) score += add(reasons, 'warm, maar nog bruikbaar met schaduw en wind', 4);
  else if (input.temperatureC > 24) {
    score += add(reasons, 'erg warm; vis korter en behandel vangsten extra voorzichtig', -14);
    warnings.push('Bij hoge watertemperatuur: kort drillen, snel onthaken en geen lange fotosessies.');
  }

  if (input.timeOfDay === 'morning' || input.timeOfDay === 'evening') {
    score += add(reasons, 'ochtend/avond geeft vaak meer activiteit', 8);
  }
  return score;
}

function scoreMethod(input: ReturnType<typeof normalizeInput>, reasons: string[]): number {
  let score = 50;
  if (input.temperatureC >= 15 && input.temperatureC <= 25) score += add(reasons, 'temperatuur is goed voor witvis/karperachtigen', 14);
  if (input.windBft >= 1 && input.windBft <= 4) score += add(reasons, 'lichte tot matige wind helpt tegen schuw gedrag', 8);
  if (input.cloudCoverPct >= 35 && input.cloudCoverPct <= 90) score += add(reasons, 'bewolking geeft een prettiger aasmoment', 8);
  if (input.pressureTrend === 'falling') score += add(reasons, 'dalende druk voor een front is vaak gunstig', 6);
  if (input.rain === 'heavy') score += add(reasons, 'zware regen bemoeilijkt voeren en beetregistratie', -10);
  if (input.timeOfDay === 'midday' && input.cloudCoverPct < 30) score += add(reasons, 'felle middagzon is minder ideaal', -8);
  if (input.timeOfDay === 'morning' || input.timeOfDay === 'evening') score += add(reasons, 'ochtend/avond is vaak rustiger aan de waterkant', 6);
  return score;
}

function buildTactics(target: SnoekTarget, score: number): string[] {
  if (target === 'snoek') {
    return score >= 70
      ? ['Start met spinnerbait, jerkbait of shad langs riet en windkant.', 'Vis actief: 5-10 worpen per hoek en dan verkassen.', 'Pak donkere of opvallende kleuren bij troebel water.']
      : ['Zoek schaduw, bruggen, steigers en diepere randen.', 'Vis trager met shad of suspending jerkbait.', 'Maak korte sessies en wissel pas als je dekking hebt afgevist.'];
  }

  return [
    'Begin compact: kleine method korf, weinig voer, scherp op beetmomenten.',
    'Vis ochtend of avond als het helder en warm is.',
    'Bij wind op de kant: probeer de rand waar natuurlijk voer samenkomt.'
  ];
}

function selectSpots(target: SnoekTarget, score: number): SnoekSpot[] {
  return SPOTS
    .filter((spot) => spot.targets.includes(target))
    .sort((a, b) => {
      const aBoost = a.waterType === 'kanaal' && target === 'snoek' && score >= 65 ? 1 : 0;
      const bBoost = b.waterType === 'kanaal' && target === 'snoek' && score >= 65 ? 1 : 0;
      return bBoost - aBoost || a.name.localeCompare(b.name);
    })
    .slice(0, 3);
}

function buildActivityWindows(input: ReturnType<typeof normalizeInput>): SnoekActivityWindow[] {
  const eveningNote = input.target === 'snoek'
    ? 'Schemer met bewolking/wind is vaak het beste roofvisblok.'
    : 'Rustiger licht en minder drukte aan de waterkant.';
  return [
    {
      label: 'Grote tijden',
      times: ['05:45 - 08:15', '18:10 - 20:40'],
      note: eveningNote
    },
    {
      label: 'Kleine tijden',
      times: ['13:00 - 14:45', '23:40 - 01:20'],
      note: 'Solunar-seed uit dashboard; later koppelen aan echte maan/zondata.'
    }
  ];
}

function relevantMapFeatures(spots: SnoekSpot[]): SnoekMapFeature[] {
  const spotIds = spots.map((spot) => spot.id);
  return MAP_FEATURES.filter((feature) => !feature.spotId || spotIds.includes(feature.spotId));
}

function relevantReviews(spots: SnoekSpot[]): SnoekCommunityReview[] {
  const spotIds = spots.map((spot) => spot.id);
  return COMMUNITY_REVIEWS.filter((review) => spotIds.includes(review.spotId));
}

export function buildSnoekScout(input: SnoekScoutInput = {}): SnoekScoutResult {
  const normalized = normalizeInput(input);
  const reasons: string[] = [];
  const warnings: string[] = [];
  const rawScore = normalized.target === 'snoek'
    ? scoreSnoek(normalized, reasons, warnings)
    : scoreMethod(normalized, reasons);
  const score = Math.round(clamp(rawScore, 0, 100));

  const label = score >= 75 ? 'Sterke kans' : score >= 58 ? 'Prima proberen' : score >= 42 ? 'Selectief vissen' : 'Liever plannen';
  const action = score >= 75
    ? 'Ga vissen'
    : score >= 58
      ? 'Ga als je een goede stek kiest'
      : score >= 42
        ? 'Alleen gericht en kort'
        : 'Bewaar je beste stekken voor beter weer';

  const spots = selectSpots(normalized.target, score);

  return {
    ok: true,
    target: normalized.target,
    score,
    label,
    action,
    summary: `${label}: ${action}.`,
    reasons,
    warnings,
    tactics: buildTactics(normalized.target, score),
    spots,
    activityWindows: buildActivityWindows(normalized),
    mapFeatures: relevantMapFeatures(spots),
    dataSources: DATA_SOURCES,
    communityReviews: relevantReviews(spots),
    input: normalized
  };
}
