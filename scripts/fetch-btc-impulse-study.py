"""Public GET-only research capture. No trading bootstrap or credentials."""
import datetime as dt
import hashlib
import io
import json
import pathlib
import sys
import time
import urllib.error
import urllib.request
import zipfile

base = pathlib.Path(sys.argv[1])
out = pathlib.Path(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)
capture = json.loads((base / "inputs.json").read_text())
replay = json.loads((base / "replay.json").read_text())
signals = replay["signals"]
manifest = []


def get(url, name, binary=False):
    file = out / name
    if file.exists():
        data = file.read_bytes()
    else:
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "BTC-offline-research/1.0"})
            with urllib.request.urlopen(request, timeout=60) as response:
                data = response.read()
            file.write_bytes(data)
            time.sleep(.5)
        except urllib.error.HTTPError as error:
            manifest.append({"url": url, "httpStatus": error.code, "name": name})
            if error.code in [418, 429]:
                raise RuntimeError("Rate limited: stopping, no retries or host bypass") from error
            print(f"Unavailable: {name} ({error.code})", flush=True)
            return None
    manifest.append({"url": url, "name": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    return data if binary else json.loads(data)


def klines(symbol, venue, interval, start, end, name):
    saved = out / name
    if saved.exists():
        return json.loads(saved.read_text())
    host = "https://fapi.binance.com/fapi/v1/klines" if venue == "futures" else "https://api.binance.com/api/v3/klines"
    duration = 3600000 if interval == "1h" else 300000
    rows = []
    cursor = start
    while cursor < end:
        url = f"{host}?symbol={symbol}&interval={interval}&startTime={cursor}&endTime={end - 1}&limit=1000"
        page = get(url, f"page-{symbol}-{venue}-{interval}-{cursor}.json")
        if not isinstance(page, list) or not page:
            return None
        rows.extend(page)
        next_cursor = int(page[-1][0]) + duration
        if next_cursor <= cursor:
            raise ValueError("Non-advancing klines")
        cursor = next_cursor
    saved.write_text(json.dumps(rows))
    print(f"{name}: {len(rows)}", flush=True)
    return rows


start = int(capture["hourly"][0][0])
end = capture["cutoff"]
klines("BTCUSDT", "futures", "1h", start, end, "futures-1h.json")
klines("ETHUSDT", "spot", "1h", start, end, "eth-1h.json")
get(f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1M&endTime={end-1}&limit=1000", "monthly.json")
get(f"https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&startTime={(signals[0]['time']-7*86400)*1000}&endTime={signals[-1]['closedAt']*1000-1}&limit=1000", "funding.json")
days = set()
for signal in signals:
    cutoff = signal["closedAt"] * 1000
    tag = dt.datetime.fromtimestamp(signal["time"], dt.timezone.utc).strftime("%Y-%m-%d-%H")
    for venue in ["spot", "futures"]:
        klines("BTCUSDT", venue, "5m", cutoff - 6 * 3600000, cutoff, f"{venue}-5m-{tag}.json")
    date = dt.datetime.fromtimestamp(signal["closedAt"], dt.timezone.utc).date()
    days.update(date - dt.timedelta(days=n) for n in range(3))
for day in sorted(days):
    name = f"BTCUSDT-metrics-{day}.zip"
    url = "https://data.binance.vision/data/futures/um/daily/metrics/BTCUSDT/" + name
    data = get(url, name, True)
    if data:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            if archive.testzip():
                raise ValueError("Corrupt metrics archive")
            csv = archive.read(archive.namelist()[0])
            (out / name.replace(".zip", ".csv")).write_bytes(csv)
        print(name, flush=True)
raw = get("https://arons-tradingview-alert-connector.onrender.com/decentrader/liquidity-timelapse", "saved-context-full.json")
if raw:
    times = {signal["time"] for signal in signals}
    rows = []
    for row in raw.get("intrusionDomStudy", {}).get("history", []):
        timestamp = row.get("timestamp", "").replace(" ", "T") + "Z"
        try:
            stamp = int(dt.datetime.fromisoformat(timestamp).timestamp())
        except ValueError:
            continue
        if stamp in times:
            rows.append(row)
    (out / "saved-context.json").write_text(json.dumps({"source": raw.get("source"), "records": rows}))
(out / "manifest.json").write_text(json.dumps({"retrievedAt": dt.datetime.now(dt.timezone.utc).isoformat(),
    "baseInputSha256": hashlib.sha256((base / "inputs.json").read_bytes()).hexdigest(), "files": manifest}, indent=2))
print("Capture finished", flush=True)
