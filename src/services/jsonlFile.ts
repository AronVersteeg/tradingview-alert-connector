import fs from 'fs';

const CHUNK = 64 * 1024;
const MAX_LINE = 4 * 1024 * 1024;

// Never materialize a complete history file just to inspect or sample its rows.
export function* jsonlLines(file: string): Generator<string> {
  const fd = fs.openSync(file, 'r'), buffer = Buffer.alloc(CHUNK);
  let parts: Buffer[] = [], length = 0, position = 0;
  try {
    const size = fs.fstatSync(fd).size;
    while (position < size) {
      const count = fs.readSync(fd, buffer, 0, Math.min(CHUNK, size - position), position);
      if (!count) break;
      position += count;
      let start = 0;
      for (let index = 0; index < count; index++) {
        if (buffer[index] !== 10) continue;
        const piece = buffer.subarray(start, index);
        if (length + piece.length > MAX_LINE) throw new Error('JSONL row exceeds the 4 MiB read limit.');
        const line = Buffer.concat([...parts, piece], length + piece.length).toString('utf8').replace(/\r$/, '');
        parts = []; length = 0; start = index + 1;
        if (line) yield line;
      }
      if (start < count) {
        const piece = Buffer.from(buffer.subarray(start, count));
        length += piece.length;
        if (length > MAX_LINE) throw new Error('JSONL row exceeds the 4 MiB read limit.');
        parts.push(piece);
      }
    }
    if (length) {
      const line = Buffer.concat(parts, length).toString('utf8').replace(/\r$/, '');
      if (line) yield line;
    }
  } finally { fs.closeSync(fd); }
}

export function lastJsonlLine(file: string): string | undefined {
  const fd = fs.openSync(file, 'r');
  let tail = Buffer.alloc(0);
  try {
    let position = fs.fstatSync(fd).size;
    while (position > 0) {
      const size = Math.min(CHUNK, position), chunk = Buffer.alloc(size);
      position -= size;
      const count = fs.readSync(fd, chunk, 0, size, position);
      tail = Buffer.concat([chunk.subarray(0, count), tail]);
      let end = tail.length;
      while (end && (tail[end - 1] === 10 || tail[end - 1] === 13)) end--;
      if (!end) { tail = Buffer.alloc(0); continue; }
      const newline = tail.lastIndexOf(10, end - 1), start = newline + 1;
      if (end - start > MAX_LINE) throw new Error('JSONL row exceeds the 4 MiB read limit.');
      if (newline >= 0 || position === 0) return tail.subarray(start, end).toString('utf8');
      tail = tail.subarray(0, end);
    }
    return undefined;
  } finally { fs.closeSync(fd); }
}
