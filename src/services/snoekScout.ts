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
  targets: SnoekTarget[];
  waterType: string;
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
  input: Required<Omit<SnoekScoutInput, 'location'>> & { location: string };
};

const SPOTS: SnoekSpot[] = [
  {
    id: 'noordzeekanaal-velsen',
    name: 'Noordzeekanaal Velsen',
    area: 'Velsen',
    lat: 52.4635,
    lon: 4.6326,
    targets: ['snoek', 'witvis'],
    waterType: 'kanaal',
    note: 'Zoek windkant, stromingsnaden, steigers en harde overgangen.'
  },
  {
    id: 'buitenhuizerplas',
    name: 'Buitenhuizerplas',
    area: 'Spaarnwoude',
    lat: 52.4358,
    lon: 4.6831,
    targets: ['snoek', 'method_feeder', 'witvis'],
    waterType: 'plas',
    note: 'Interessant bij bewolking en wind op de kant; vis randen en wiergaten af.'
  },
  {
    id: 'de-ven',
    name: 'De Ven',
    area: 'Spaarnwoude',
    lat: 52.4329,
    lon: 4.6684,
    targets: ['method_feeder', 'witvis', 'snoek'],
    waterType: 'parkplas',
    note: 'Rustig water voor method feeder; bij roofvis vooral zoeken bij riet en obstakels.'
  },
  {
    id: 'park-schoonenberg',
    name: 'Park Schoonenberg',
    area: 'Velsen-Zuid',
    lat: 52.4598,
    lon: 4.6478,
    targets: ['witvis', 'method_feeder', 'snoek'],
    waterType: 'parkvijver',
    note: 'Kleinschalig water: stil benaderen en compact voeren werkt vaak beter.'
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
    spots: selectSpots(normalized.target, score),
    input: normalized
  };
}
