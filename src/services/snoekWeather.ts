import { RainState, SnoekScoutInput, TimeOfDay, PressureTrend } from './snoekScout';

export type SnoekWeatherResult = {
  ok: true;
  source: 'open-meteo';
  attribution: string;
  location: {
    name: string;
    country?: string;
    latitude: number;
    longitude: number;
    timezone?: string;
  };
  weather: {
    temperatureC: number;
    windKmh: number;
    windBft: number;
    cloudCoverPct: number;
    pressureHpa: number | null;
    pressureTrend: PressureTrend;
    precipitationMm: number;
    rain: RainState;
    timeOfDay: TimeOfDay;
    observedAt?: string;
  };
  scoutInput: SnoekScoutInput;
};

type OpenMeteoGeoResult = {
  name: string;
  country?: string;
  latitude: number;
  longitude: number;
  timezone?: string;
};

function numberOrNull(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function round(value: number, digits = 1): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function windKmhToBft(windKmh: number): number {
  const thresholds = [1, 6, 12, 20, 29, 39, 50, 62, 75, 89, 103, 118];
  const index = thresholds.findIndex((threshold) => windKmh < threshold);
  return index === -1 ? 12 : index;
}

function rainFromPrecipitation(precipitationMm: number): RainState {
  if (precipitationMm >= 2.5) return 'heavy';
  if (precipitationMm > 0) return 'light';
  return 'none';
}

function timeOfDayFromIso(value?: string): TimeOfDay {
  const date = value ? new Date(value) : new Date();
  const hour = Number.isFinite(date.getHours()) ? date.getHours() : new Date().getHours();
  if (hour >= 5 && hour < 11) return 'morning';
  if (hour >= 11 && hour < 17) return 'midday';
  if (hour >= 17 && hour < 23) return 'evening';
  return 'night';
}

function pressureTrendFromHourly(hourly: any, currentPressure: number | null): PressureTrend {
  if (!hourly?.pressure_msl?.length || currentPressure === null) return 'steady';
  const futurePressure = numberOrNull(hourly.pressure_msl[Math.min(3, hourly.pressure_msl.length - 1)]);
  if (futurePressure === null) return 'steady';

  const delta = futurePressure - currentPressure;
  if (delta <= -0.8) return 'falling';
  if (delta >= 0.8) return 'rising';
  return 'steady';
}

async function fetchJson(url: string): Promise<any> {
  const response = await fetch(url, {
    headers: {
      'User-Agent': 'snoek-ai-scout/1.0'
    }
  });

  if (!response.ok) {
    throw new Error(`Open-Meteo request failed: ${response.status}`);
  }

  return response.json();
}

async function geocodeLocation(location: string): Promise<OpenMeteoGeoResult> {
  const query = new URLSearchParams({
    name: location,
    count: '1',
    language: 'nl',
    format: 'json'
  });
  const payload = await fetchJson(`https://geocoding-api.open-meteo.com/v1/search?${query}`);
  const first = payload?.results?.[0];

  if (!first) {
    throw new Error(`Geen locatie gevonden voor "${location}".`);
  }

  return {
    name: first.name,
    country: first.country,
    latitude: Number(first.latitude),
    longitude: Number(first.longitude),
    timezone: first.timezone
  };
}

export async function getSnoekWeather(location: string): Promise<SnoekWeatherResult> {
  const resolvedLocation = await geocodeLocation(location || 'Velsen-Zuid');
  const query = new URLSearchParams({
    latitude: String(resolvedLocation.latitude),
    longitude: String(resolvedLocation.longitude),
    current: [
      'temperature_2m',
      'precipitation',
      'rain',
      'cloud_cover',
      'pressure_msl',
      'wind_speed_10m',
      'wind_direction_10m',
      'weather_code'
    ].join(','),
    hourly: 'pressure_msl',
    forecast_hours: '4',
    timezone: 'auto',
    wind_speed_unit: 'kmh'
  });
  const payload = await fetchJson(`https://api.open-meteo.com/v1/forecast?${query}`);
  const current = payload.current || {};

  const temperatureC = numberOrNull(current.temperature_2m) ?? 20;
  const windKmh = numberOrNull(current.wind_speed_10m) ?? 0;
  const cloudCoverPct = numberOrNull(current.cloud_cover) ?? 50;
  const pressureHpa = numberOrNull(current.pressure_msl);
  const precipitationMm = numberOrNull(current.precipitation) ?? numberOrNull(current.rain) ?? 0;
  const pressureTrend = pressureTrendFromHourly(payload.hourly, pressureHpa);
  const timeOfDay = timeOfDayFromIso(current.time);
  const rain = rainFromPrecipitation(precipitationMm);

  return {
    ok: true,
    source: 'open-meteo',
    attribution: 'Weather and geocoding data from Open-Meteo.com (CC BY 4.0).',
    location: {
      name: resolvedLocation.name,
      country: resolvedLocation.country,
      latitude: resolvedLocation.latitude,
      longitude: resolvedLocation.longitude,
      timezone: resolvedLocation.timezone || payload.timezone
    },
    weather: {
      temperatureC: round(temperatureC),
      windKmh: round(windKmh),
      windBft: windKmhToBft(windKmh),
      cloudCoverPct: Math.round(cloudCoverPct),
      pressureHpa: pressureHpa === null ? null : round(pressureHpa),
      pressureTrend,
      precipitationMm: round(precipitationMm),
      rain,
      timeOfDay,
      observedAt: current.time
    },
    scoutInput: {
      location: resolvedLocation.name,
      temperatureC: round(temperatureC),
      windBft: windKmhToBft(windKmh),
      cloudCoverPct: Math.round(cloudCoverPct),
      pressureTrend,
      rain,
      timeOfDay
    }
  };
}
