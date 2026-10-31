import fs from 'fs';
import os from 'os';
import path from 'path';
import { jsonlLines, lastJsonlLine } from '../src/services/jsonlFile';
import { DecentralizedDomCollector } from '../src/services/decentralizedDomCollector';

describe('bounded JSONL history reads', () => {
  let directory: string, file: string;
  beforeEach(() => { directory = fs.mkdtempSync(path.join(os.tmpdir(), 'jsonl-read-')); file = path.join(directory, 'history.jsonl'); });
  afterEach(() => { jest.restoreAllMocks(); jest.useRealTimers(); fs.rmSync(directory, { recursive: true, force: true }); });
  test.each(['\n', '\r\n'])('retains complete UTF-8 rows across chunk boundaries with %j separators', newline => {
    const rows = [JSON.stringify({ text: 'x'.repeat(65_525) + '\u20ac\u00e9' }), JSON.stringify({ text: 'y'.repeat(131_100) })];
    fs.writeFileSync(file, newline + rows.join(newline + newline) + newline + newline);
    expect([...jsonlLines(file)]).toEqual(rows); expect(lastJsonlLine(file)).toBe(rows[1]);
  });
  test.each(['', '\n\r\n', '{"x":1}', '{"x":1}\n'])('handles empty files and a final row without a newline: %j', content => {
    fs.writeFileSync(file, content);
    const expected = content.split(/\r?\n/).filter(Boolean);
    expect([...jsonlLines(file)]).toEqual(expected); expect(lastJsonlLine(file)).toBe(expected.at(-1));
  });
  test('closes descriptors when the consumer takes only the first row or parsing fails', () => {
    fs.writeFileSync(file, 'first\nsecond\n');
    const close = jest.spyOn(fs, 'closeSync');
    for (const _line of jsonlLines(file)) break;
    expect(close).toHaveBeenCalledTimes(1);
    expect(() => { for (const line of jsonlLines(file)) JSON.parse(line); }).toThrow();
    expect(close).toHaveBeenCalledTimes(2);
  });
  test('oversized single rows fail explicitly without rewriting the file or leaking descriptors', () => {
    fs.writeFileSync(file, 'x'.repeat(4 * 1024 * 1024 + 1));
    const size = fs.statSync(file).size, close = jest.spyOn(fs, 'closeSync');
    expect(() => [...jsonlLines(file)]).toThrow('4 MiB');
    expect(() => lastJsonlLine(file)).toThrow('4 MiB');
    expect(close).toHaveBeenCalledTimes(2); expect(fs.statSync(file).size).toBe(size);
  });
  test('DOM range sampling is identical to the former full-array algorithm, retaining only selected rows', () => {
    jest.useFakeTimers(); jest.setSystemTime(Date.parse('2026-10-31T13:00:00Z'));
    const start = Date.parse('2026-10-29T01:00:00Z');
    const rows = Array.from({ length: 2401 }, (_, i) => ({ bucketStart: new Date(start + i * 60_000).toISOString(),
      bucketEnd: new Date(start + (i + 1) * 60_000).toISOString(), market: 'SOL-USD', marker: i, payload: 'x'.repeat(2048) }));
    const groups = new Map<string, any[]>();
    for (const row of rows) {
      const name = `dom-${row.bucketStart.slice(0, 10)}.ndjson`;
      groups.set(name, [...(groups.get(name) || []), row]);
    }
    for (const [name, records] of groups) fs.writeFileSync(path.join(directory, name), records.map(row => JSON.stringify(row)).join('\n') + '\n');
    const collector = new DecentralizedDomCollector({ market: 'SOL-USD' });
    jest.spyOn(collector, 'historyDirectory').mockReturnValue(directory);
    const wholeFile = jest.spyOn(fs, 'readFileSync').mockImplementation(() => { throw new Error('Whole-file history read is forbidden'); });
    const result = collector.getHistory({ from: rows[0].bucketStart, to: rows.at(-1)!.bucketEnd, maxPoints: 10 });
    const stride = Math.ceil(rows.length / 10);
    expect(result.records).toEqual(rows.filter((_, i) => i % stride === 0 || i === rows.length - 1));
    expect(result.records.length).toBeLessThanOrEqual(11); expect(result.market).toBe('SOL-USD');
    const read = jest.spyOn(fs, 'readSync');
    expect(collector.getCoverage()).toMatchObject({ from: rows[0].bucketStart, to: rows.at(-1)!.bucketEnd, sourceFiles: 2 });
    expect(read.mock.calls.every(call => Number((call as any[])[3]) <= 64 * 1024)).toBe(true);
    expect(read).toHaveBeenCalledTimes(2); expect(wholeFile).not.toHaveBeenCalled();
  });
  test('an invalid DOM tail preserves earlier records and source bytes', () => {
    jest.useFakeTimers(); jest.setSystemTime(Date.parse('2026-10-31T13:00:00Z'));
    file = path.join(directory, 'dom-2026-10-31.ndjson');
    const row = { bucketStart: '2026-10-31T10:00:00Z', bucketEnd: '2026-10-31T10:01:00Z', market: 'ETH-USD' };
    const content = JSON.stringify(row) + '\n{"partial'; fs.writeFileSync(file, content);
    const collector = new DecentralizedDomCollector({ market: 'ETH-USD' });
    jest.spyOn(collector, 'historyDirectory').mockReturnValue(directory);
    jest.spyOn(console, 'warn').mockImplementation(() => {});
    expect(collector.getHistory({}).records).toEqual([row]);
    expect(fs.readFileSync(file, 'utf8')).toBe(content);
  });
});
