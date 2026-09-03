export type RoofvisSpecies = 'snoek' | 'snoekbaars' | 'baars';

export type FishAdviceReading = {
  depthM: number | null;
  temperatureC: number;
  chlorideMgL?: number | null;
  conductivityUsCm?: number | null;
};

export type FishDepthWindow = {
  minM: number;
  maxM: number;
  label: string;
};

export type RoofvisAdvice = {
  species: RoofvisSpecies;
  label: string;
  confidence: 'laag' | 'middel';
  activityLabel: string;
  dayDepth: FishDepthWindow | null;
  lowLightDepth: FishDepthWindow | null;
  seasonalPreyCm: { min: number; max: number };
  lureLengthCm: { min: number; max: number };
  lureTypes: string[];
  presentation: string;
  reasons: string[];
  limitation: string;
};

export type FishAdviceSource = {
  id: string;
  label: string;
  url: string;
  use: string;
};

export const FISH_ADVICE_SOURCES: FishAdviceSource[] = [
  {
    id: 'pierce-pike-depth',
    label: 'Pierce et al. (2013), Northern pike depth and thermal habitat',
    url: 'https://doi.org/10.1080/00028487.2013.822422',
    use: 'Grote snoek kan in de zomer koeler water volgen; zuurstof kan de ondergrens van die zone bepalen.'
  },
  {
    id: 'casselman-pike-habitat',
    label: 'Casselman & Lewis (1996), Northern pike habitat requirements',
    url: 'https://doi.org/10.1139/f96-019',
    use: 'Dekking en waterplanten zijn belangrijke snoekhabitat; lage zuurstof beperkt bruikbare diepte.'
  },
  {
    id: 'horky-zander-telemetry',
    label: 'Horky et al. (2008), diel pikeperch telemetry',
    url: 'https://vuzv.cz/publikace/a-telemetry-study-on-the-diurnal-distribution-and-activity-of-adult-pikeperch-sander-lucioperca-l-in-a-riverine-environment/',
    use: 'Snoekbaars zat overdag dieper en verplaatste zich rond schemer naar ondiepere zones.'
  },
  {
    id: 'frisk-zander-temperature',
    label: 'Frisk et al. (2012), thermal optimum of pikeperch',
    url: 'https://doi.org/10.1016/j.aquaculture.2011.10.024',
    use: 'Volwassen snoekbaars heeft een brede thermische gebruikszone; temperatuur alleen discrimineert weinig.'
  },
  {
    id: 'perch-review',
    label: 'European perch ecology review (2025)',
    url: 'https://doi.org/10.1007/s11160-025-09924-z',
    use: 'Seizoen, licht, prooiaanbod en habitat sturen diepte en activiteit van baars.'
  },
  {
    id: 'gaeta-prey-size',
    label: 'Gaeta et al. (2018), predator and prey size in piscivores',
    url: 'https://pmc.ncbi.nlm.nih.gov/articles/PMC5854328/',
    use: 'Prooigrootte schaalt met roofvislengte en heeft een brede verdeling; een exact aasformaat is geen meetuitkomst.'
  },
  {
    id: 'tien-yoy-growth',
    label: 'Tien et al. (2024), temperature-related growth of Dutch lake fish',
    url: 'https://doi.org/10.1111/jfb.15745',
    use: 'Groei van jonge baars, blankvoorn en brasem hangt samen met opgehoopte warmtesom; maandbanden blijven indicatief.'
  }
];

function round(value: number, digits = 1): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function depthWindow(minM: number, maxM: number, label: string): FishDepthWindow {
  return { minM: round(minM, 2), maxM: round(Math.max(minM, maxM), 2), label };
}

function seasonalPreyBand(month: number): { min: number; max: number } {
  if (month <= 2 || month === 12) return { min: 7, max: 13 };
  if (month === 3) return { min: 6, max: 11 };
  if (month === 4) return { min: 4, max: 8 };
  if (month === 5) return { min: 3, max: 7 };
  if (month === 6) return { min: 4, max: 8 };
  if (month === 7) return { min: 5, max: 9 };
  if (month === 8) return { min: 6, max: 10 };
  if (month === 9) return { min: 7, max: 11 };
  if (month === 10) return { min: 8, max: 12 };
  return { min: 8, max: 13 };
}

function speciesLureBand(species: RoofvisSpecies, prey: { min: number; max: number }): { min: number; max: number } {
  if (species === 'snoek') return { min: Math.max(7, prey.min + 1), max: prey.max + 3 };
  if (species === 'snoekbaars') return { min: Math.max(6, prey.min), max: prey.max };
  return { min: Math.max(4, prey.min - 3), max: Math.max(7, prey.max - 2) };
}

function closestDepth(readings: Array<FishAdviceReading & { depthM: number }>, targetC: number): number {
  return readings.reduce((best, reading) => (
    Math.abs(reading.temperatureC - targetC) < Math.abs(best.temperatureC - targetC) ? reading : best
  )).depthM;
}

function windowAround(depthM: number, minDepth: number, maxDepth: number, label: string): FishDepthWindow {
  const spread = Math.max(0.35, (maxDepth - minDepth) / 5);
  return depthWindow(Math.max(minDepth, depthM - spread), Math.min(maxDepth, depthM + spread), label);
}

function activityFor(species: RoofvisSpecies, meanC: number): string {
  if (species === 'snoek') {
    if (meanC > 24) return 'Warmtestress mogelijk: kort drillen en koelere/dekkingsrijke zones zoeken';
    if (meanC >= 16 && meanC <= 22) return 'Thermisch gunstig bereik voor actieve snoek';
    return meanC < 8 ? 'Koud water: traag en spaarzaam aanbieden' : 'Bruikbaar temperatuurbereik';
  }
  if (species === 'snoekbaars') {
    if (meanC >= 11 && meanC <= 26) return 'Binnen de brede thermische gebruikszone';
    return meanC < 8 ? 'Koud water: langzaam dicht bij bodem/structuur' : 'Temperatuur kan activiteit beperken';
  }
  if (meanC >= 15 && meanC <= 24) return 'Gunstig bereik; licht en prooivis bepalen de plek mede';
  return meanC < 8 ? 'Koud water: scholen vaak compacter en dieper' : 'Bruikbaar, maar niet thermisch optimaal';
}

export function buildRoofvisAdvice(readings: FishAdviceReading[], month: number): RoofvisAdvice[] {
  const known = readings
    .filter((reading): reading is FishAdviceReading & { depthM: number } => reading.depthM !== null)
    .sort((a, b) => a.depthM - b.depthM);
  const temperatures = readings.map((reading) => reading.temperatureC).filter(Number.isFinite);
  if (!temperatures.length) return [];

  const meanC = temperatures.reduce((sum, value) => sum + value, 0) / temperatures.length;
  const tempRange = Math.max(...temperatures) - Math.min(...temperatures);
  const prey = seasonalPreyBand(month);
  const minDepth = known[0]?.depthM ?? 0;
  const maxDepth = known[known.length - 1]?.depthM ?? 0;
  const depthSpan = Math.max(0, maxDepth - minDepth);
  const hasProfile = known.length >= 2 && depthSpan > 0;
  const homogeneous = tempRange < 0.6;
  const chlorideValues = known.map((reading) => reading.chlorideMgL).filter((value): value is number => Number.isFinite(value));
  const chlorideRange = chlorideValues.length >= 2 ? Math.max(...chlorideValues) - Math.min(...chlorideValues) : 0;
  const stronglyLayeredChemistry = chlorideRange >= 500;

  const pikeThermalDepth = hasProfile ? closestDepth(known, 19.5) : null;
  const pikeFreshest = stronglyLayeredChemistry
    ? known.filter((reading) => Number.isFinite(reading.chlorideMgL)).reduce((best, reading) => (
      Number(reading.chlorideMgL) < Number(best.chlorideMgL) ? reading : best
    )).depthM
    : null;
  const pikeDepth = pikeFreshest ?? pikeThermalDepth;
  const pikeWindow = hasProfile && !homogeneous && pikeDepth !== null
    ? windowAround(pikeDepth, minDepth, maxDepth, 'thermische zoekzone')
    : hasProfile
      ? depthWindow(minDepth, maxDepth, 'hele gemeten kolom; zoek dekking en randen')
      : null;

  const deepStart = minDepth + depthSpan * 0.5;
  const midEnd = minDepth + depthSpan * 0.65;
  const shallowEnd = minDepth + depthSpan * 0.45;
  const zanderDay = hasProfile ? depthWindow(deepStart, maxDepth, 'overdag midden-diep tot diep') : null;
  const zanderLowLight = hasProfile ? depthWindow(minDepth, midEnd, 'rond schemer ondieper zoeken') : null;
  const perchDay = hasProfile
    ? depthWindow(minDepth + depthSpan * (month >= 9 || month <= 3 ? 0.4 : 0.2), maxDepth, 'middenlaag tot diepere rand')
    : null;
  const perchLowLight = hasProfile ? depthWindow(minDepth, shallowEnd, 'schemer: rand en ondiepere prooizone') : null;

  const commonLimitation = 'Zuurstof, troebelheid, vegetatie en lokale prooivis zijn hier niet gemeten; de zone is een zoekhypothese, geen visdetectie.';
  return [
    {
      species: 'snoek',
      label: 'Snoek',
      confidence: hasProfile && (!homogeneous || stronglyLayeredChemistry) ? 'middel' : 'laag',
      activityLabel: activityFor('snoek', meanC),
      dayDepth: pikeWindow,
      lowLightDepth: pikeWindow,
      seasonalPreyCm: prey,
      lureLengthCm: speciesLureBand('snoek', prey),
      lureTypes: hasProfile && maxDepth >= 2.5 ? ['shad', 'suspending crankbait', 'spinnerbait langs dekking'] : ['jerkbait', 'ondiep lopende crankbait', 'spinnerbait'],
      presentation: homogeneous
        ? 'Vis de waterkolom systematisch; geef voorrang aan talud, planten, schaduw en luwte naast stroming.'
        : 'Begin net boven de gekozen zone en laat het kunstaas niet langdurig onder de vermoedelijke vislaag lopen.',
      reasons: [
        homogeneous ? `Temperatuurverschil is slechts ${round(tempRange)} C; temperatuur selecteert geen smalle laag.` : `Het profiel verschilt ${round(tempRange)} C; de zone ligt nabij het voor snoek gunstige thermische bereik.`,
        stronglyLayeredChemistry ? `Chloride verschilt ${Math.round(chlorideRange)} mg/L over de kolom; de zoetere laag krijgt een bescheiden voorkeur, met lokale aanpassing als voorbehoud.` : 'Dekking en prooivis blijven waarschijnlijk belangrijker dan kleine temperatuurverschillen.'
      ],
      limitation: commonLimitation
    },
    {
      species: 'snoekbaars',
      label: 'Snoekbaars',
      confidence: hasProfile ? 'middel' : 'laag',
      activityLabel: activityFor('snoekbaars', meanC),
      dayDepth: zanderDay,
      lowLightDepth: zanderLowLight,
      seasonalPreyCm: prey,
      lureLengthCm: speciesLureBand('snoekbaars', prey),
      lureTypes: ['shad op jigkop', 'dropshot', 'slanke crankbait bij schemer'],
      presentation: 'Overdag langzaam langs bodem, talud en harde structuur; rond schemer eerst de ondiepere rand en stromingsnaad afvissen.',
      reasons: [
        'De thermische gebruikszone van volwassen snoekbaars is breed; licht, diepte en structuur wegen daarom zwaarder.',
        'Telemetrieonderzoek ondersteunt dieper gebruik overdag en verplaatsing naar ondiepere zones rond schemer.'
      ],
      limitation: commonLimitation
    },
    {
      species: 'baars',
      label: 'Baars',
      confidence: hasProfile ? 'middel' : 'laag',
      activityLabel: activityFor('baars', meanC),
      dayDepth: perchDay,
      lowLightDepth: perchLowLight,
      seasonalPreyCm: prey,
      lureLengthCm: speciesLureBand('baars', prey),
      lureTypes: ['kleine shad', 'dropshot', 'kleine crankbait'],
      presentation: 'Zoek actief langs talud, havenrand en aasvis; verklein het kunstaas wanneer alleen kleine prooivis of voorzichtige aanbeten zichtbaar zijn.',
      reasons: [
        month >= 9 || month <= 3 ? 'In herfst en winter gebruikt baars vaker midden-diepe en diepere zones.' : 'In het warme seizoen kan baars ondiepere, prooirijke zones gebruiken.',
        'Baars is een zichtjager; licht en troebelheid kunnen de effectieve zoekzone sterk veranderen.'
      ],
      limitation: commonLimitation
    }
  ];
}
