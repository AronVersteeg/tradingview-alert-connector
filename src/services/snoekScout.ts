export type SnoekTarget = 'snoek' | 'snoekbaars' | 'method_feeder' | 'witvis';
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
    targets: ['snoek', 'snoekbaars', 'witvis'],
    waterType: 'kanaal',
    depthClass: '1-6 m, talud naar hoofdgeul',
    structures: ['talud', 'kade', 'stromingsnaad'],
    note: 'Zoek windkant, stromingsnaden, steigers en harde overgangen.'
  },
  {
    id: 'pontje-velsen-zuid',
    name: 'Pontje Velsen-Zuid',
    area: 'Velsen-Zuid',
    lat: 52.4626581,
    lon: 4.6323097,
    targets: ['snoekbaars'],
    waterType: 'kanaalstroming',
    depthClass: 'kanaal/talud rond pontstroming',
    structures: ['pontstroming', 'kade', 'talud', 'steiger richting Oud Velsen'],
    note: 'Engelhart advies: veel snoekbaars rond stroming van het pontje; ook richting steiger Oud Velsen proberen.'
  },
  {
    id: 'zijkanaal-c-a9',
    name: 'Zijkanaal C / Brug A9',
    area: 'Spaarndam',
    lat: 52.4212,
    lon: 4.693,
    targets: ['snoek', 'snoekbaars'],
    waterType: 'zijkanaal',
    depthClass: 'kanaalrand, brugschaduw en talud',
    structures: ['brug', 'zijkanaal', 'harde rand', 'talud'],
    note: 'Engelhart advies: Zijkanaal C en brug over de A9 genoemd als roofvisstek.'
  },
  {
    id: 'sluis-spaarndam',
    name: 'Sluis Spaarndam',
    area: 'Spaarndam',
    lat: 52.4129566,
    lon: 4.6814088,
    targets: ['snoek', 'snoekbaars'],
    waterType: 'sluis/stroming',
    depthClass: 'harde sluisranden, stroming en luwtes',
    structures: ['sluis', 'stroming', 'harde kant', 'aasvis'],
    note: 'Engelhart advies: sluis Spaarndam genoemd; dropshot en softbait langs stroming/luwte.'
  },
  {
    id: 'pontje-buitenhuizen',
    name: 'Pontje Buitenhuizen',
    area: 'Buitenhuizen',
    lat: 52.433,
    lon: 4.7255,
    targets: ['snoekbaars', 'snoek'],
    waterType: 'pontstroming',
    depthClass: 'kanaalrand met stromingsnaad',
    structures: ['pontstroming', 'kade', 'talud'],
    note: 'Engelhart advies: pontje Buitenhuizen genoemd als roofvisplek; stroming is de hoofdreden.'
  },
  {
    id: 'buitenhuizerplas',
    name: 'Buitenhuizerplas',
    area: 'Buitenhuizen, Velsen-Zuid',
    lat: 52.42914,
    lon: 4.70786,
    targets: ['snoek', 'method_feeder', 'witvis'],
    waterType: 'plas',
    depthClass: 'ondiep tot middel, randen interessanter dan open water',
    structures: ['windkant', 'wier/rand', 'flauwe oever'],
    note: 'Voormalige zwemplas bij Buitenhuizen; bij bewolking en wind vooral randen en wiergaten afvissen.'
  },
  {
    id: 'de-ven',
    name: 'De Ven',
    area: 'De Ven, Velsen-Zuid',
    lat: 52.4549,
    lon: 4.6642,
    targets: ['method_feeder', 'witvis', 'snoek'],
    waterType: 'parkplas',
    depthClass: 'geschat 0,8-2 m',
    structures: ['riet', 'parkbrug', 'kommetje'],
    note: 'Modelbouw-/recreatieplas bij De Ven; bij roofvis vooral zoeken bij riet en obstakels.'
  },
  {
    id: 'park-schoonenberg',
    name: 'Park Schoonenberg',
    area: 'Driehuis',
    lat: 52.4523246,
    lon: 4.6356894,
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
    id: 'kadaster-brt-top10nl',
    label: 'Kadaster BRT + TOP10NL via PDOK',
    status: 'live',
    use: 'Leidende kaartgeometrie voor water, wegen, gebouwen, spoor en plaatslabels in dezelfde projectie als de luchtfoto.'
  },
  {
    id: 'esri-rws-bathymetry',
    label: 'Rijkswaterstaat bodemhoogte 1 m WNN_n_NAP',
    status: 'live',
    use: 'Officiële bodemhoogteklassen in meters t.o.v. NAP voor IJmuiden, Noordzeekanaal en gedekte zijkanalen; bedoeld om geulen en taluds te lezen.'
  },
  {
    id: 'rws-waterwebservices-current',
    label: 'Rijkswaterstaat stroming en richting',
    status: 'live',
    use: 'Actuele ADCP-puntmetingen van stroomsnelheid en stroomrichting; oude of onbetrouwbare reeksen worden niet als live getoond.'
  },
  {
    id: 'pdok-imwa-kunstwerken',
    label: 'PDOK Waterschappen Kunstwerken IMWA',
    status: 'live',
    use: 'Alle 18 officiële IMWA-lagen met hun exacte PDOK-titel en geometrie; de snoekselectie blijft als aparte afgeleide laag herkenbaar.'
  },
  {
    id: 'ahn',
    label: 'AHN hoogte/oeverdata',
    status: 'planned',
    use: 'Oevertaluds, steile randen en brugstructuren herkennen waar bathymetrie ontbreekt.'
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
    id: 'engelhart-hengelsport-advies',
    label: 'Engelhart Hengelsport advies',
    status: 'manual',
    use: 'Lokale praktijkinput voor Zijkanaal C, brug A9, Sluis Spaarndam, pontjes, snoekbaars, softbaits en dropshot.'
  },
  {
    id: 'windy-fishingpoints',
    label: 'Windy / Fishing Points stijl input',
    status: 'seeded',
    use: 'Wind, luchtdruk, bewolking, zon/maan en activiteitsscore.'
  },
  {
    id: 'pdok-aerial-context',
    label: 'PDOK actuele luchtfoto',
    status: 'live',
    use: 'Alleen als visuele inkleuring onder de leidende Kadaster BRT/TOP10NL-geometrie.'
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
    spotId: 'pontje-velsen-zuid',
    rating: 5,
    source: 'Engelhart Hengelsport advies',
    note: 'Veel snoekbaars rond pontstroming; ook richting steiger bij Oud Velsen proberen.',
    baits: ['dropshot', 'kleine shad', 'jigkop'],
    confidence: 'hoog'
  },
  {
    spotId: 'zijkanaal-c-a9',
    rating: 4,
    source: 'Engelhart Hengelsport advies',
    note: 'Zijkanaal C en brug over de A9 genoemd als relevante roofvisstek.',
    baits: ['softbait', 'dropshot', 'shad'],
    confidence: 'hoog'
  },
  {
    spotId: 'sluis-spaarndam',
    rating: 4,
    source: 'Engelhart Hengelsport advies',
    note: 'Sluis/stroming genoemd; interessant voor roofvis als aasvis door waterbeweging wordt geconcentreerd.',
    baits: ['dropshot', 'shad', 'softbait'],
    confidence: 'hoog'
  },
  {
    spotId: 'pontje-buitenhuizen',
    rating: 4,
    source: 'Engelhart Hengelsport advies',
    note: 'Pontstroming genoemd als reden om hier gericht te zoeken.',
    baits: ['dropshot', 'shad', 'jigkop'],
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
    target: normalizeEnum(input.target, ['snoek', 'snoekbaars', 'method_feeder', 'witvis'], 'snoek'),
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

function scoreSnoekbaars(input: ReturnType<typeof normalizeInput>, reasons: string[]): number {
  let score = 50;
  if (input.windBft >= 1 && input.windBft <= 4) score += add(reasons, 'lichte tot matige wind helpt stromingsnaden en kades afvissen', 8);
  if (input.windBft >= 6) score += add(reasons, 'harde wind maakt dropshot en bodemcontact lastig', -10);

  if (input.cloudCoverPct >= 50) score += add(reasons, 'bewolking past bij snoekbaars op ondiepere randen', 8);
  if (input.cloudCoverPct <= 20 && input.timeOfDay === 'midday') score += add(reasons, 'fel middaglicht drukt snoekbaars vaak dieper of strakker tegen structuur', -10);

  if (input.pressureTrend === 'falling') score += add(reasons, 'dalende luchtdruk kan aasvis en snoekbaars activeren', 8);
  if (input.pressureTrend === 'rising') score += add(reasons, 'stijgende druk is minder overtuigend voor actief zoeken', -4);

  if (input.rain === 'light') score += add(reasons, 'lichte regen geeft dekking en minder drukte', 5);
  if (input.rain === 'heavy') score += add(reasons, 'zware regen maakt langzaam bodemvissen onnauwkeurig', -8);

  if (input.temperatureC >= 7 && input.temperatureC <= 22) score += add(reasons, 'temperatuur is bruikbaar voor snoekbaars met shad/dropshot', 10);
  else if (input.temperatureC > 24) score += add(reasons, 'erg warm; vis kort en kies zuurstofrijke stroming', -8);

  if (input.timeOfDay === 'evening' || input.timeOfDay === 'night') {
    score += add(reasons, 'schemer/nacht is sterk voor snoekbaars rond kade, pont en sluis', 14);
  } else if (input.timeOfDay === 'morning') {
    score += add(reasons, 'ochtend kan goed zijn als er stroming of schaduw staat', 6);
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

  if (target === 'snoekbaars') {
    return score >= 70
      ? ['Begin met dropshot rond pontstroming, steigers en harde kanaalranden.', 'Vis shads traag tegen de bodem op talud en stromingsnaad.', 'Pak schemer of avond; blijf compact op plekken met stroming/aasvis.']
      : ['Zoek eerst stroming: pontje, sluis, gemaal of harde kade.', 'Dropshot of kleine shad werkt beter dan snel kunstaas.', 'Vermijd grote open stukken zonder talud, kade of stromingsnaad.'];
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
      const boost = (spot: SnoekSpot) => {
        if (target === 'snoekbaars') {
          let value = 0;
          if (spot.waterType.includes('pontstroming') || spot.waterType.includes('kanaalstroming')) value += 5;
          if (spot.waterType.includes('sluis')) value += 4;
          if (spot.waterType.includes('zijkanaal') || spot.waterType.includes('kanaal')) value += 2;
          if (spot.structures.some((structure) => ['kade', 'talud', 'stroming', 'pontstroming'].includes(structure))) value += 2;
          return value;
        }
        return spot.waterType === 'kanaal' && target === 'snoek' && score >= 65 ? 1 : 0;
      };
      const aBoost = boost(a);
      const bBoost = boost(b);
      return bBoost - aBoost || a.name.localeCompare(b.name);
    })
    .slice(0, 3);
}

function buildActivityWindows(input: ReturnType<typeof normalizeInput>): SnoekActivityWindow[] {
  const eveningNote = input.target === 'snoek'
    ? 'Schemer met bewolking/wind is vaak het beste roofvisblok.'
    : input.target === 'snoekbaars'
      ? 'Schemer/nacht plus stroming bij pont, sluis of kade is het sterkste blok.'
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
    : normalized.target === 'snoekbaars'
      ? scoreSnoekbaars(normalized, reasons)
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
