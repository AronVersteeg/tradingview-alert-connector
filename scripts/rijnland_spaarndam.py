#!/usr/bin/env python3
"""Read near-realtime Rijnland water data for Spaarndam.

The script discovers the current FeatureServer URLs from Rijnland's public
ArcGIS WebMaps and then queries exact, known feature identifiers. It uses only
the Python standard library and never substitutes guessed values.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


ARCGIS_ITEM_DATA_URL = (
    "https://www.arcgis.com/sharing/rest/content/items/{item_id}/data"
)
DEFAULT_TIMEOUT_SECONDS = 30.0
USER_AGENT = "snoek-ai-rijnland-reader/1.0"


@dataclass(frozen=True)
class SourceSpec:
    key: str
    webmap_id: str
    layer_title: str
    feature_identifier: str
    unit: str


SOURCES = (
    SourceSpec(
        key="boezem_level",
        webmap_id="97b578c6d86241499d0668b9c4ea0bcf",
        layer_title="WaterpeilenBoezemLaag",
        feature_identifier="464-036-00021_boezem",
        unit="m NAP",
    ),
    SourceSpec(
        key="flow",
        webmap_id="7ef7e2cbd14e4233bd8c4ee4ea7d1ac5",
        layer_title="Aan- en afvoer (debiet)",
        feature_identifier="464-036-00021",
        unit="m3/s",
    ),
    SourceSpec(
        key="pump_status",
        webmap_id="87d0c15958424e0ba136760d10832289",
        layer_title="Gemalen Actief/Niet Actief",
        feature_identifier="464-036-00021",
        unit="status",
    ),
    SourceSpec(
        key="temperature",
        webmap_id="5246de169d3b4a7db28490a934fa3959",
        layer_title="Temperatuur metingen",
        feature_identifier="462-105-00029_kwaliteit",
        unit="deg C",
    ),
    SourceSpec(
        key="chloride",
        webmap_id="37f8f499e2994bfd92331dc63d281142",
        layer_title="Chloride metingen",
        feature_identifier="462-105-00029_kwaliteit",
        unit="mg/l",
    ),
    SourceSpec(
        key="conductivity",
        webmap_id="7ba3721cdd024638a02ff559928b18a4",
        layer_title="EGV metingen",
        feature_identifier="464-036-00021_kwaliteit1",
        unit="uS/cm",
    ),
    SourceSpec(
        key="precipitation",
        webmap_id="80b1c6d1f4794813bff0ae5a7fc73edf",
        layer_title="Neerslag",
        feature_identifier="462-105-00026_meteo",
        unit="mm/day",
    ),
)


class SourceError(RuntimeError):
    pass


def local_timezone():
    try:
        return ZoneInfo("Europe/Amsterdam")
    except ZoneInfoNotFoundError:
        return datetime.now().astimezone().tzinfo or timezone.utc


def iso_from_milliseconds(value: Any) -> str | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    timestamp = datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    return timestamp.astimezone(local_timezone()).isoformat(timespec="seconds")


def fetch_json(
    url: str,
    params: dict[str, Any] | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    query = dict(params or {})
    query.setdefault("f", "json")
    query.setdefault("_", int(time.time() * 1000))
    request_url = f"{url}?{urlencode(query)}"
    request = Request(
        request_url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise SourceError(f"Request failed for {url}: {exc}") from exc

    if not isinstance(payload, dict):
        raise SourceError(f"Unexpected response for {url}")
    if payload.get("error"):
        raise SourceError(f"ArcGIS error for {url}: {payload['error']}")
    return payload


def normalized_title(value: Any) -> str:
    return " ".join(str(value or "").split()).casefold()


def nested_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from nested_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from nested_dicts(child)


def discover_layer_url(spec: SourceSpec, timeout: float) -> str:
    webmap_url = ARCGIS_ITEM_DATA_URL.format(item_id=spec.webmap_id)
    webmap = fetch_json(webmap_url, timeout=timeout)
    target = normalized_title(spec.layer_title)
    matches = [
        item.get("url")
        for item in nested_dicts(webmap.get("operationalLayers", []))
        if normalized_title(item.get("title")) == target and item.get("url")
    ]
    unique_matches = list(dict.fromkeys(matches))
    if len(unique_matches) != 1:
        raise SourceError(
            f"Expected one layer named {spec.layer_title!r} in WebMap "
            f"{spec.webmap_id}, found {len(unique_matches)}"
        )
    return str(unique_matches[0]).rstrip("/")


def numeric_value(value: Any) -> float | int:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise SourceError(f"Measurement is not numeric: {value!r}")
    return value


def fetch_measurement(spec: SourceSpec, timeout: float) -> dict[str, Any]:
    layer_url = discover_layer_url(spec, timeout)
    layer_info = fetch_json(layer_url, timeout=timeout)
    escaped_identifier = spec.feature_identifier.replace("'", "''")
    query = fetch_json(
        f"{layer_url}/query",
        params={
            "where": f"featureIdentifier='{escaped_identifier}'",
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": "4326",
        },
        timeout=timeout,
    )
    features = query.get("features") or []
    if len(features) != 1:
        raise SourceError(
            f"Expected one {spec.key} record for {spec.feature_identifier}, "
            f"found {len(features)}"
        )

    feature = features[0]
    attributes = feature.get("attributes") or {}
    geometry = feature.get("geometry") or {}
    data_last_edit = (layer_info.get("editingInfo") or {}).get("dataLastEditDate")
    return {
        "key": spec.key,
        "name": attributes.get("name"),
        "value": numeric_value(attributes.get("value")),
        "classification": attributes.get("classification"),
        "unit": spec.unit,
        "feature_identifier": attributes.get("featureIdentifier"),
        "coordinates": {
            "lat": geometry.get("y"),
            "lon": geometry.get("x"),
        },
        "source_updated_at": iso_from_milliseconds(data_last_edit),
        "feature_service": layer_url,
        "chart_url": attributes.get("chartUrl"),
        "webmap_id": spec.webmap_id,
    }


def read_all(timeout: float) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    measurements: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=len(SOURCES)) as executor:
        futures = {
            executor.submit(fetch_measurement, spec, timeout): spec
            for spec in SOURCES
        }
        for future in as_completed(futures):
            spec = futures[future]
            try:
                measurements[spec.key] = future.result()
            except Exception as exc:  # Keep partial official output usable.
                errors[spec.key] = str(exc)
    return measurements, errors


def pump_active(measurement: dict[str, Any]) -> bool | None:
    classification = str(measurement.get("classification") or "").strip().casefold()
    if classification == "aan":
        return True
    if classification == "uit":
        return False
    return None


def flow_direction(signed_flow: float | int) -> str:
    if signed_flow < 0:
        return "afvoer"
    if signed_flow > 0:
        return "aanvoer"
    return "geen"


def compact_source(measurement: dict[str, Any]) -> dict[str, Any]:
    return {
        "feature_identifier": measurement["feature_identifier"],
        "measurement_point": measurement["name"],
        "unit": measurement["unit"],
        "source_updated_at": measurement["source_updated_at"],
        "feature_service": measurement["feature_service"],
        "chart_url": measurement["chart_url"],
        "webmap_id": measurement["webmap_id"],
    }


def build_output(
    measurements: dict[str, dict[str, Any]], errors: dict[str, str]
) -> dict[str, Any]:
    output: dict[str, Any] = {
        "location": "Spaarndam",
        "retrieved_at": datetime.now(local_timezone()).isoformat(timespec="seconds"),
        "gemaal": {"name": "Boezemgemaal Spaarndam"},
        "water": {},
        "weather": {},
        "timestamp_note": (
            "Rijnland exposes the FeatureServer refresh time, not a verified "
            "timestamp for each individual sensor observation."
        ),
    }

    status = measurements.get("pump_status")
    if status:
        active = pump_active(status)
        if active is not None:
            output["gemaal"]["active"] = active
        output["gemaal"]["status"] = status["classification"]
        output["gemaal"]["coordinates"] = status["coordinates"]
        output["gemaal"]["status_source_updated_at"] = status["source_updated_at"]

    flow = measurements.get("flow")
    if flow:
        signed_flow = flow["value"]
        output["gemaal"]["flow_m3_s"] = abs(signed_flow)
        output["gemaal"]["flow_signed_m3_s"] = signed_flow
        output["gemaal"]["flow_direction"] = flow_direction(signed_flow)
        output["gemaal"]["flow_source_updated_at"] = flow["source_updated_at"]

    water_fields = {
        "boezem_level": "boezem_level_m_nap",
        "temperature": "temperature_c",
        "chloride": "chloride_mg_l",
        "conductivity": "conductivity_us_cm",
    }
    for source_key, output_key in water_fields.items():
        measurement = measurements.get(source_key)
        if measurement:
            output["water"][output_key] = measurement["value"]

    precipitation = measurements.get("precipitation")
    if precipitation:
        output["weather"]["precipitation_mm_day"] = precipitation["value"]
        output["weather"]["measurement_point"] = precipitation["name"]

    output["sources"] = {
        key: compact_source(measurements[key]) for key in sorted(measurements)
    }
    if errors:
        output["errors"] = errors
    return output


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read public near-realtime Rijnland data for Spaarndam."
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout per request in seconds (default: 30)",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Print compact JSON instead of indented JSON",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    measurements, errors = read_all(args.timeout)
    output = build_output(measurements, errors)
    print(
        json.dumps(
            output,
            ensure_ascii=False,
            indent=None if args.compact else 2,
            sort_keys=False,
        )
    )
    if not measurements:
        return 1
    return 2 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
