import fs from 'node:fs';
import path from 'node:path';
import { atrHistoryPath, decodeAtrHistory, decodeGreenHistory, greenHistoryPath, type GreenHistoryArchive } from '../src/research/greenRetestHistory';
import { priorSpotAtr, type SpotAtrRange } from '../src/services/atrEntry';
import { PURPLE_LINE_SETUPS, type PurpleLineMarket } from '../src/services/purpleLineSetup';

const dir = process.argv[2];
if (!dir) throw new Error('Provide the Binance Spot input directory.');
const requested = process.argv[3]?.toUpperCase();
if (requested && !Object.hasOwn(PURPLE_LINE_SETUPS, requested)) throw new Error('Expected a supported Spot chart market.');
for (const [market, setup] of Object.entries(PURPLE_LINE_SETUPS) as [PurpleLineMarket, typeof PURPLE_LINE_SETUPS[PurpleLineMarket]][]) {
  if (requested && market !== requested) continue;
  const green: GreenHistoryArchive = JSON.parse(fs.readFileSync(greenHistoryPath(market), 'utf8'));
  const raw = JSON.parse(fs.readFileSync(path.join(dir, `${setup.asset}.json`), 'utf8'));
  if (raw.symbol !== setup.symbol || raw.ticker !== market) throw new Error(`Wrong input for ${market}.`);
  const byHour = new Map<number, SpotAtrRange>(raw.spot.filter(([at, end]: number[]) => end === at + 3_600_000 - 1)
    .map(([startedAtMs, , , high, low, close]: number[]) => [startedAtMs, { startedAtMs, high, low, close }]));
  const ranges = green.greens.map(([closedAt, , , close]): [number, number] => {
    const signalAt = closedAt - 3_600_000;
    const atr = priorSpotAtr(byHour, signalAt);
    if (atr === undefined || byHour.get(signalAt)?.close !== close) {
      throw new Error(`Unverified ${market} Spot data at ${new Date(signalAt).toISOString()}.`);
    }
    return [closedAt, atr];
  });
  const archive = { version: 1 as const, market, model: setup.modelVersion, from: green.from, through: green.through, ranges };
  const checked = decodeAtrHistory(decodeGreenHistory(market, green), archive);
  fs.writeFileSync(atrHistoryPath(market), JSON.stringify(archive) + '\n');
  console.log(market, ranges.length, 'verified reference ranges;', checked.atrMarkers?.length, 'ATR signals');
}
