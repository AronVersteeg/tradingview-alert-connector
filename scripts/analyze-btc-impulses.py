"""Exploratory, post-selection study. Never a live filter or validated predictor."""
import datetime as dt
import html
import itertools
import json
import math
import pathlib
import sys
import numpy as np
import pandas as pd

base, out = map(pathlib.Path, sys.argv[1:3])
capture = json.loads((base / "inputs.json").read_text())
replay = json.loads((base / "replay.json").read_text())
structure = json.loads((out / "structure.json").read_text())
targets = {"2026-06-01T11:00:00.000Z|short", "2026-08-19T15:00:00.000Z|long"}
definitions = {}


def frame(raw, interval_ms=3600000):
    columns = ["time", "open", "high", "low", "close", "volume", "end", "quote", "trades", "buy", "buy_quote", "ignore"]
    data = pd.DataFrame(raw, columns=columns).apply(pd.to_numeric).sort_values("time").reset_index(drop=True)
    data = data[data.end < capture["cutoff"]].copy()
    if len(data) > 1 and not (data.time.diff().dropna() == interval_ms).all():
        raise ValueError("Non-contiguous candle input")
    if ((data.buy > data.volume + 1e-6) | (data.buy < 0) | (data.buy_quote > data.quote + 1)).any():
        raise ValueError("Invalid taker-volume accounting")
    data["delta"] = 2 * data.buy - data.volume
    data["delta_quote"] = 2 * data.buy_quote - data.quote
    data["date"] = pd.to_datetime(data.time, unit="ms", utc=True)
    return data


def wilder(series, n=14):
    result = np.full(len(series), np.nan)
    first = series.first_valid_index()
    if first is None or first + n > len(series):
        return pd.Series(result, index=series.index)
    at = first + n - 1
    result[at] = series.iloc[first:at+1].mean()
    for i in range(at + 1, len(series)):
        result[i] = (result[i - 1] * (n - 1) + series.iloc[i]) / n
    return pd.Series(result, index=series.index)


def indicators(data):
    data = data.reset_index(drop=True).copy()
    close, high, low = data.close, data.high, data.low
    tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    data["atr"] = wilder(tr)
    change = close.diff()
    gain, loss = wilder(change.clip(lower=0)), wilder((-change).clip(lower=0))
    data["rsi"] = 100 - 100 / (1 + gain / loss.replace(0, np.nan))
    up, down = high.diff(), -low.diff()
    plus = wilder(up.where((up > down) & (up > 0), 0)) / data.atr * 100
    minus = wilder(down.where((down > up) & (down > 0), 0)) / data.atr * 100
    data["di_gap"] = plus - minus
    data["adx"] = wilder((plus - minus).abs() / (plus + minus).replace(0, np.nan) * 100)
    for n in [9, 21, 50, 200]:
        data[f"ema{n}"] = close.ewm(span=n, adjust=False, min_periods=n).mean()
    macd = close.ewm(span=12, adjust=False).mean() - close.ewm(span=26, adjust=False).mean()
    data["macd_hist"] = macd - macd.ewm(span=9, adjust=False).mean()
    mean = close.rolling(20).mean()
    data["bb_width"] = 4 * close.rolling(20).std(ddof=0) / mean * 100
    return data


def resampled(data, hours):
    groups = data.set_index("date").resample(f"{hours}h", label="right", closed="left")
    result = groups.agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum", "quote": "sum", "delta": "sum", "delta_quote": "sum"})
    counts = groups.close.count()
    result = result[counts == hours].reset_index()
    end_ms = result.date.astype("datetime64[ns, UTC]").astype("int64") // 1000000
    result["time"] = end_ms - hours * 3600000
    result["end"] = end_ms - 1
    if result.end.max() > data.end.max() or result.time.min() < data.time.min():
        raise ValueError("Resample timestamp units or coverage invalid")
    return indicators(result)


spot = indicators(frame(capture["hourly"]))
futures = indicators(frame(json.loads((out / "futures-1h.json").read_text())))
eth = frame(json.loads((out / "eth-1h.json").read_text()))
h4 = resampled(spot, 4)
day = resampled(spot, 24)
funding = pd.DataFrame(json.loads((out / "funding.json").read_text()))
if not funding.empty:
    funding["fundingRate"] = pd.to_numeric(funding.fundingRate)
    funding = funding.sort_values("fundingTime")
metrics = pd.concat([pd.read_csv(file) for file in sorted(out.glob("BTCUSDT-metrics-*.csv"))], ignore_index=True)
metrics["stamp"] = pd.to_datetime(metrics.create_time, utc=True).astype("datetime64[ns, UTC]").astype("int64") // 1000000
if not metrics.stamp.between(1700000000000, capture["cutoff"]).any():
    raise ValueError("Metrics timestamp units invalid")
duplicate_conflicts = int(metrics.groupby("stamp").sum_open_interest.nunique().gt(1).sum())
metrics = metrics.sort_values("stamp").drop_duplicates("stamp", keep="last")
contexts = json.loads((out / "saved-context.json").read_text()).get("records", [])
structure_by_time = {row["time"]: row for row in structure["records"]}
rows = []
context_audit = []


def snapshot(cutoff, lag_minutes=5):
    eligible = metrics[metrics.stamp <= cutoff - lag_minutes * 60000]
    if eligible.empty:
        return None
    row = eligible.iloc[-1]
    if cutoff - lag_minutes * 60000 - row.stamp > 600000:
        return None
    return row


for signal in replay["signals"]:
    stamp, cutoff = signal["time"] * 1000, signal["closedAt"] * 1000
    sign = 1 if signal["direction"] == "long" else -1
    index = int(spot.index[spot.time == stamp][0])
    trade = next(t for t in replay["independent"]["trades"] if t["signalTime"] == signal["time"])
    features = dict(structure_by_time[signal["time"]]["features"])
    current = spot.iloc[index]
    prior = spot.iloc[:index]
    features["initial_stop_distance_pct"] = abs(trade["entryPrice"] - trade["initialStop"]) / trade["entryPrice"] * 100
    for prefix, data in [("spot", spot), ("futures", futures)]:
        j = int(data.index[data.time == stamp][0])
        c, p = data.iloc[j], data.iloc[:j]
        features[f"{prefix}_body_pct"] = sign * (c.close / c.open - 1) * 100
        features[f"{prefix}_body_ATR"] = sign * (c.close - c.open) / p.atr.iloc[-1]
        features[f"{prefix}_range_ATR"] = (c.high - c.low) / p.atr.iloc[-1]
        features[f"{prefix}_close_location_pct"] = ((c.close-c.low) if sign == 1 else (c.high-c.close)) / (c.high-c.low) * 100
        features[f"{prefix}_body_fraction_pct"] = abs(c.close-c.open) / (c.high-c.low) * 100
        features[f"{prefix}_delta_quote_usd"] = sign * c.delta_quote
        features[f"{prefix}_delta_pct"] = sign * c.delta / c.volume * 100
        features[f"{prefix}_adx14"] = c.adx
        features[f"{prefix}_directional_DI_gap"] = sign * c.di_gap
        features[f"{prefix}_directional_RSI14"] = sign * (c.rsi - 50)
        features[f"{prefix}_directional_MACD_ATR"] = sign * c.macd_hist / c.atr
        features[f"{prefix}_ATR_pct"] = c.atr / c.close * 100
        features[f"{prefix}_BB_width20_pct"] = c.bb_width
        features[f"{prefix}_pre_BB_width20_pct"] = p.bb_width.iloc[-1]
        for n in [9, 21, 50, 200]:
            features[f"{prefix}_directional_distance_EMA{n}_ATR"] = sign * (c.close-c[f"ema{n}"]) / c.atr
        for n in [6, 24, 72, 168]:
            previous = p.tail(n)
            features[f"{prefix}_volume_ratio_{n}"] = c.volume / previous.volume.mean()
            features[f"{prefix}_volume_median_ratio_{n}"] = c.volume / previous.volume.median()
            features[f"{prefix}_volume_percentile_{n}"] = (previous.volume < c.volume).mean() * 100
        for n in [1, 3, 6, 12, 24, 72, 168]:
            window = data.iloc[max(0, j-n+1):j+1]
            features[f"{prefix}_directional_return_{n}h_pct"] = sign * (c.close / data.iloc[j-n].close-1) * 100
            features[f"{prefix}_directional_delta_{n}h_pct"] = sign * window.delta.sum() / window.volume.sum() * 100
        for n in [20, 72, 168]:
            prior_n = p.tail(n)
            features[f"{prefix}_prior_range_{n}_pct"] = (prior_n.high.max()-prior_n.low.min()) / c.close * 100
            features[f"{prefix}_donchian_break_{n}_ATR"] = (c.close-prior_n.high.max() if sign == 1 else prior_n.low.min()-c.close) / p.atr.iloc[-1]
        if prefix == "spot":
            definitions["spot_delta_pct"] = "100 * direction * (2*takerBuyBaseVolume-totalBaseVolume) / totalBaseVolume"
    for label, data in [("4h", h4), ("1d", day)]:
        closed = data[data.end < cutoff]
        c = closed.iloc[-1]
        if not 0 < cutoff-c.end <= int(label[:-1])*(3600000 if label.endswith('h') else 86400000):
            raise ValueError("Higher-timeframe data is not the latest completed period")
        features[f"{label}_directional_RSI14"] = sign * (c.rsi-50)
        features[f"{label}_adx14"] = c.adx
        features[f"{label}_directional_DI_gap"] = sign * c.di_gap
        features[f"{label}_directional_MACD_ATR"] = sign * c.macd_hist / c.atr
        features[f"{label}_body_ATR"] = sign * (c.close-c.open) / closed.atr.iloc[-2]
        for n in [21, 50, 200]:
            features[f"{label}_directional_distance_EMA{n}_ATR"] = sign * (current.close-c[f"ema{n}"]) / c.atr
    midnight = current.date.floor("D")
    monday = midnight - pd.Timedelta(days=midnight.dayofweek)
    for label, since in [("day", midnight), ("week", monday)]:
        included = spot[(spot.date >= since) & (spot.time <= stamp)]
        vwap = included.quote.sum() / included.volume.sum()
        features[f"spot_directional_distance_{label}_VWAP_ATR"] = sign * (current.close-vwap) / current.atr
    fut = futures[futures.time == stamp].iloc[0]
    features["futures_spot_basis_bps"] = (fut.close/current.close-1)*10000
    features["futures_to_spot_volume_ratio"] = fut.volume/current.volume
    e = eth[eth.time == stamp].iloc[0]
    for n in [1, 6, 24]:
        ep = eth[eth.time == stamp-n*3600000].iloc[0]
        bp = spot.iloc[index-n]
        features[f"eth_directional_return_{n}h_pct"] = sign * (e.close/ep.close-1)*100
        features[f"ethbtc_directional_return_{n}h_pct"] = sign * ((e.close/current.close)/(ep.close/bp.close)-1)*100
    if not funding.empty:
        known = funding[funding.fundingTime < cutoff]
        features["funding_last_pct"] = float(known.iloc[-1].fundingRate)*100 if len(known) else None
        features["funding_directional_last_pct"] = sign*features["funding_last_pct"] if len(known) else None
    for lag in [0, 5, 10]:
        end = snapshot(cutoff, lag)
        for n in [1, 3, 6, 24]:
            begin = snapshot(cutoff-n*3600000, lag)
            features[f"OI_change_{n}h_pct_lag{lag}"] = (end.sum_open_interest/begin.sum_open_interest-1)*100 if end is not None and begin is not None else None
        if end is not None:
            for col in ["count_toptrader_long_short_ratio", "sum_toptrader_long_short_ratio", "count_long_short_ratio"]:
                features[f"{col}_lag{lag}"] = float(end[col])
            features[f"OI_snapshot_age_minutes_lag{lag}"] = (cutoff-end.stamp)/60000
    tag = dt.datetime.fromtimestamp(signal["time"], dt.timezone.utc).strftime("%Y-%m-%d-%H")
    for venue in ["spot", "futures"]:
        bars = frame(json.loads((out/f"{venue}-5m-{tag}.json").read_text()), 300000)
        event = bars[(bars.time >= stamp) & (bars.end < cutoff)]
        if len(event) != 12:
            raise ValueError("Signal 5m sub-bars incomplete")
        reference = (spot if venue == 'spot' else futures).query('time == @stamp').iloc[0]
        combined = {"open": event.iloc[0].open, "close": event.iloc[-1].close,
            "high": event.high.max(), "low": event.low.min(), "volume": event.volume.sum(), "buy": event.buy.sum()}
        for key, value in combined.items():
            if not math.isclose(value, float(reference[key]), rel_tol=1e-8, abs_tol=1e-5):
                raise ValueError(f"5m/1h mismatch: {tag} {venue} {key}")
        last = event.iloc[-1]
        for minutes in [5, 15, 30]:
            tail = event.tail(minutes//5)
            features[f"{venue}_last{minutes}m_directional_return_pct"] = sign*(last.close/tail.iloc[0].open-1)*100
            features[f"{venue}_last{minutes}m_delta_pct"] = sign*tail.delta.sum()/tail.volume.sum()*100
        favorable_extreme = event.high.max() if sign == 1 else event.low.min()
        features[f"{venue}_retrace_from_event_extreme_pct"] = abs(last.close/favorable_extreme-1)*100
        features[f"{venue}_last15m_volume_share_pct"] = event.tail(3).volume.sum()/event.volume.sum()*100
        features[f"{venue}_last5m_volume_BTC"] = float(last.volume)
        features[f"{venue}_last5m_volume_share_pct"] = last.volume/event.volume.sum()*100
        features[f"{venue}_last5m_delta_quote_usd"] = sign*last.delta_quote
        features[f"{venue}_last5m_trade_count"] = float(last.trades)
    for feature in ["D_next_intact_distance_R", "OB_nearest_ahead_distance_R"]:
        definitions[feature] = "Distance in trade direction at signal close / exact initial SL distance; only already confirmed, wick-intact levels"
    context = next((r for r in contexts if r.get("timestamp") == signal["signalOpenUtc"][:19].replace("T", " ") and r.get("direction") == signal["direction"]), {})
    cg = context.get("coinGlass", {})
    event_time = pd.to_datetime(cg.get("eventObservedAt"), utc=True, errors="coerce")
    entry_time = pd.to_datetime(cutoff, unit="ms", utc=True)
    cg_available = bool(pd.notna(event_time) and event_time <= entry_time)
    cg_age = (entry_time-event_time).total_seconds()/60 if pd.notna(event_time) else None
    support, friction = cg.get("eventSupportUsd"), cg.get("eventFrictionUsd")
    levels = cg.get("eventRelevantLevels", [])
    ahead = [level for level in levels if level.get('side') == ('sell' if sign == 1 else 'buy') and sign*(level['price']-current.close) > 0]
    nearest = min(ahead, key=lambda level: abs(level['price']-current.close)) if ahead else None
    context_audit.append({"signalTime": signal["time"], "coinGlassEventObservedAt": cg.get("eventObservedAt"),
        "coinGlassReviewObservedAt": cg.get("reviewObservedAt"),
        "coinGlassEventAvailableByEntry": cg_available, "coinGlassEventAgeMinutes": cg_age,
        "coinGlassEventFresh15m": bool(cg_available and cg_age <= 15),
        "eventSupportUsd": support, "eventFrictionUsd": friction,
        "eventSupportFrictionRatio": support/friction if support is not None and friction else None,
        "nearestOpposingLevelInSavedSnapshot": nearest,
        "eventLevelCount": cg.get("eventLevelCount"), "sourceAudit": context.get("sourceAudit"),
        "savedIqOi": context.get("impulseQuality", {}).get("openInterest"),
        "pre1h": context.get("pre1h"), "event": context.get("event")})
    rows.append({"signalTime": signal["time"], "signalOpenAmsterdam": signal["signalOpenAmsterdam"],
        "date": signal["signalOpenUtc"][:10], "direction": signal["direction"], "target": f'{signal["signalOpenUtc"]}|{signal["direction"]}' in targets,
        "pnl": trade["netPnl"], "R": trade["rMultiple"], **features})

data = pd.DataFrame(rows)
if int(data.target.sum()) != 2 or len(data) != 11:
    raise ValueError("This study is defined for the two exact target signals within the original 11-signal replay")
metadata = {"signalTime", "signalOpenAmsterdam", "date", "direction", "target", "pnl", "R"}
feature_cols = [c for c in data.columns if c not in metadata and "snapshot_age" not in c and not c.endswith("lag0") and not c.endswith("lag10")]
complete = [c for c in feature_cols if data[c].notna().all() and np.isfinite(data[c]).all() and data[c].nunique() > 1]
target_idx = set(data.index[data.target])
ranked = []
for feature in feature_cols:
    values = data[feature]
    wins, other = values[data.target].dropna(), values[~data.target].dropna()
    if len(wins) != 2 or not len(other):
        continue
    auc_up = float(np.mean([float(a > b)+.5*float(a == b) for a in wins for b in other]))
    high = auc_up >= .5
    threshold = float(wins.min() if high else wins.max())
    selected = values >= threshold if high else values <= threshold
    ranked.append({"feature": feature, "orientation": ">=" if high else "<=", "targetBoundary": threshold,
        "pairwiseAUC": max(auc_up, 1-auc_up), "otherSelected": int((selected & ~data.target).sum()),
        "otherObserved": len(other), "missingOthers": 9-len(other), "complete": feature in complete,
        "targetValues": list(map(float, wins)), "otherMin": float(other.min()), "otherMax": float(other.max())})
ranked.sort(key=lambda r: (not r["complete"], r["otherSelected"], -r["pairwiseAUC"]))


def best_separation(pair):
    positive = data.index.isin(pair)
    best = 9
    for feature in complete:
        a = data.loc[positive, feature]
        b = data.loc[~positive, feature]
        best = min(best, int((b >= a.min()).sum()), int((b <= a.max()).sum()))
    return best


permutations = [best_separation(set(pair)) for pair in itertools.combinations(data.index, 2)]
observed = best_separation(target_idx)
calibration = {"completeVariableCount": len(complete), "candidateVariableCount": len(feature_cols), "allTargetPairs": len(permutations),
    "observedBestOtherSelected": observed,
    "fractionOfArbitraryPairsAtLeastAsSeparable": sum(v <= observed for v in permutations)/len(permutations),
    "warning": "Descriptive multiple-search calibration only: outcomes chosen retrospectively; events overlap; not an IID significance test or predictive validation"}
fixed_rules = {
    "Spot volume >= 2x prior24 mean": data.spot_volume_ratio_24 >= 2,
    "Spot volume >= 3x prior24 mean": data.spot_volume_ratio_24 >= 3,
    "Directional spot delta > 0": data.spot_delta_pct > 0,
    "Directional futures delta > 0": data.futures_delta_pct > 0,
    "Directional body >= 1 prior ATR": data.spot_body_ATR >= 1,
    "1H ADX >= 25": data.spot_adx14 >= 25,
    "OI 1h decreasing (5m lag)": data.OI_change_1h_pct_lag5 < 0,
    "Room to next intact Daily >= 1R": data.D_next_intact_distance_R >= 1,
    "Room to nearest intact OB >= 1R": data.OB_nearest_ahead_distance_R >= 1,
    "Fresh Weekly break": data.W_fresh_wick_breaks > 0,
    "Fresh Monthly break": data.M_fresh_wick_breaks > 0,
    "Break confirmed opposing OB": data.OB_broken_count > 0,
    "Break OB intact >= 7 days": data.OB_oldest_break_age_hours >= 168,
    "Last15m spot delta still aligned": data.spot_last15m_delta_pct > 0,
    "Exploratory: futures last5m directional delta >= 25%": data.futures_last5m_delta_pct >= 25,
    "Exploratory: pre-signal BB width20 <= 2.5%": data.spot_pre_BB_width20_pct <= 2.5,
}
rules = [{"rule": name, "targetsRetained": int((mask & data.target).sum()), "signals": int(mask.sum()),
    "otherSignals": int((mask & ~data.target).sum()), "losses": int((mask & (data.pnl < 0)).sum()),
    "sumIndependentPnl": float(data.loc[mask, "pnl"].sum()), "dates": data.loc[mask, "date"].tolist()} for name, mask in fixed_rules.items()]
data.to_csv(out/"features.csv", index=False)
summary = {"generatedAt": dt.datetime.now(dt.timezone.utc).isoformat(), "rows": json.loads(data.to_json(orient="records")),
    "rankedFeatures": ranked, "searchCalibration": calibration, "fixedRules": rules, "contextAudit": context_audit,
    "definitions": definitions, "metricsConflictingTimestamps": duplicate_conflicts,
    "limitations": ["11 retrospective signals, not all market events; two winners chosen from their outcomes.",
        "No out-of-sample validation. Thresholds at target boundaries are fitted descriptions, not recommendations.",
        "Archive metrics time labeling is uncertain; main OI features use a conservative 5m label lag, sensitivity 0m/10m retained.",
        "Raw delta is executed taker buy minus taker sell, not new positions or resting orderbook demand.",
        "OB pivot-volume ratios are contextual proxies, not actual remaining liquidity or required absorption.",
        "CoinGlass observations after the entry or without provenance are excluded from causal features."]}
(out/"study.json").write_text(json.dumps(summary, indent=2, allow_nan=False))
print(json.dumps({"features": len(feature_cols), "complete": len(complete), "calibration": calibration, "top": ranked[:15], "rules": rules}, indent=2))
