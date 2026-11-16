import os
import sys
import json
import hashlib
import uuid
import sqlite3
import logging
from datetime import datetime, timezone, timedelta
from typing import Tuple, List, Dict, Any

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DQ_PASS_THRESHOLD = 95.0
DEFAULT_DB_PATH = os.path.join("warehouse", "building_telemetry.db")
SCHEMA_PATH = os.path.join("sql", "schema.sql")

ZONES = [
    {"zone_id": "ZONE-A1", "zone_name": "Data Center & Server Room", "floor_level": 1, "target_temp_c": 19.5},
    {"zone_id": "ZONE-B1", "zone_name": "North Engineering Wing", "floor_level": 2, "target_temp_c": 22.5},
    {"zone_id": "ZONE-B2", "zone_name": "South Operations Wing", "floor_level": 2, "target_temp_c": 22.5},
    {"zone_id": "ZONE-C1", "zone_name": "Central HVAC Chiller Plant", "floor_level": 0, "target_temp_c": 18.0},
    {"zone_id": "ZONE-D1", "zone_name": "Executive & Conference Atrium", "floor_level": 3, "target_temp_c": 23.0},
]


class DataQualityThresholdError(Exception):
    """Raised when batch data quality drops below the configured SLA threshold."""


def categorize_weather(code: int) -> str:
    if code == 0:
        return "Clear Sky"
    if code in (1, 2, 3):
        return "Partly Cloudy"
    if code in (45, 48):
        return "Fog"
    if code in (51, 53, 55, 61, 63, 65, 80, 81, 82):
        return "Rain"
    if code in (95, 96, 99):
        return "Thunderstorm"
    return "Other"


def extract_weather_data(lat: float = 12.9716, lon: float = 77.5946) -> pd.DataFrame:
    """
    Extract hourly weather telemetry from the Open-Meteo REST API (Bengaluru coordinates).
    Includes a deterministic fallback if the external API is unreachable in restricted runners.
    """
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}&hourly=temperature_2m,relative_humidity_2m,weather_code"
        f"&past_days=2&forecast_days=1&timezone=UTC"
    )
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        payload = response.json()["hourly"]
        df_weather = pd.DataFrame({
            "timestamp_utc": pd.to_datetime(payload["time"], utc=True).strftime("%Y-%m-%dT%H:00:00Z"),
            "outdoor_temp_c": payload["temperature_2m"],
            "humidity_pct": payload["relative_humidity_2m"],
            "weather_code": payload["weather_code"],
        })
        df_weather["weather_category"] = df_weather["weather_code"].apply(categorize_weather)
        logger.info("Extracted %d hourly weather records from Open-Meteo API.", len(df_weather))
        return df_weather
    except Exception as exc:
        logger.warning("Open-Meteo API unavailable (%s); generating deterministic weather fallback.", exc)
        now_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        rows = []
        for h in range(-48, 24):
            ts = (now_hour + timedelta(hours=h)).strftime("%Y-%m-%dT%H:00:00Z")
            hour_of_day = (now_hour + timedelta(hours=h)).hour
            temp = round(21.0 + 6.5 * ((12 - abs(14 - hour_of_day)) / 12.0), 1)
            rows.append({
                "timestamp_utc": ts,
                "outdoor_temp_c": temp,
                "humidity_pct": 62.0,
                "weather_code": 1,
                "weather_category": "Partly Cloudy",
            })
        return pd.DataFrame(rows)


def extract_sensor_telemetry(hours: int = 48) -> pd.DataFrame:
    """
    Simulate Smart Building HVAC & Energy IoT sensor logs across 5 zones,
    including realistic dirty telemetry anomalies to test the Data Quality Gate.
    """
    end_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    records: List[Dict[str, Any]] = []

    for h in range(hours):
        ts_dt = end_hour - timedelta(hours=(hours - 1 - h))
        ts_str = ts_dt.strftime("%Y-%m-%dT%H:00:00Z")
        is_business_hours = 8 <= ts_dt.hour <= 19

        for idx, z in enumerate(ZONES):
            base_kwh = 42.0 if z["zone_id"] == "ZONE-C1" else (28.0 if z["zone_id"] == "ZONE-A1" else 16.5)
            load_factor = 1.35 if is_business_hours else 0.75
            energy_kwh = round(base_kwh * load_factor + ((h + idx) % 5) * 0.85, 2)
            indoor_temp = round(z["target_temp_c"] + (((h * 3 + idx) % 4) - 1) * 0.3, 2)
            hvac_load = round(min(94.0, 48.0 * load_factor + ((h + idx) % 7) * 2.1), 1)

            records.append({
                "zone_id": z["zone_id"],
                "timestamp_utc": ts_str,
                "energy_kwh": energy_kwh,
                "indoor_temp_c": indoor_temp,
                "hvac_load_pct": hvac_load,
            })

    # Inject 8 realistic data quality anomalies (~3.2% of 248 total rows -> 96.77% pass rate)
    anomalies = [
        # 2 exact duplicate sensor events
        records[10].copy(),
        records[45].copy(),
        # 2 physically impossible negative energy readings
        {"zone_id": "ZONE-B1", "timestamp_utc": records[20]["timestamp_utc"], "energy_kwh": -14.8, "indoor_temp_c": 22.4, "hvac_load_pct": 55.0},
        {"zone_id": "ZONE-C1", "timestamp_utc": records[80]["timestamp_utc"], "energy_kwh": -3.2, "indoor_temp_c": 18.2, "hvac_load_pct": 61.0},
        # 2 faulty thermal sensor spikes (>100°C)
        {"zone_id": "ZONE-A1", "timestamp_utc": records[35]["timestamp_utc"], "energy_kwh": 31.2, "indoor_temp_c": 118.4, "hvac_load_pct": 88.0},
        {"zone_id": "ZONE-D1", "timestamp_utc": records[95]["timestamp_utc"], "energy_kwh": 19.0, "indoor_temp_c": 104.9, "hvac_load_pct": 49.0},
        # 1 missing foreign key (null zone_id)
        {"zone_id": None, "timestamp_utc": records[50]["timestamp_utc"], "energy_kwh": 15.4, "indoor_temp_c": 22.1, "hvac_load_pct": 50.0},
        # 1 out-of-bounds HVAC load percentage (>100%)
        {"zone_id": "ZONE-B2", "timestamp_utc": records[110]["timestamp_utc"], "energy_kwh": 21.5, "indoor_temp_c": 23.0, "hvac_load_pct": 142.0},
    ]

    raw_df = pd.DataFrame(records + anomalies)
    logger.info("Extracted %d raw IoT sensor telemetry records.", len(raw_df))
    return raw_df


def transform_and_validate(
    raw_sensor_df: pd.DataFrame,
    weather_df: pd.DataFrame,
    run_id: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, float]:
    """
    Standardize schemas, deduplicate sensor events, enforce physical validation rules,
    route invalid records to quarantine, and join with outdoor weather telemetry.
    """
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    clean_rows: List[Dict[str, Any]] = []
    quarantine_rows: List[Dict[str, Any]] = []
    seen_keys = set()

    for _, row in raw_sensor_df.iterrows():
        raw_dict = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        zone_id = raw_dict.get("zone_id")
        ts_raw = raw_dict.get("timestamp_utc")
        energy = raw_dict.get("energy_kwh")
        temp = raw_dict.get("indoor_temp_c")
        hvac = raw_dict.get("hvac_load_pct")

        reason = None
        if not zone_id or not ts_raw or energy is None or temp is None or hvac is None:
            reason = "MISSING_REQUIRED_ATTRIBUTE"
        else:
            ts_norm = pd.to_datetime(ts_raw, utc=True).strftime("%Y-%m-%dT%H:00:00Z")
            dedup_key = (zone_id, ts_norm)
            if dedup_key in seen_keys:
                reason = "DUPLICATE_SENSOR_EVENT"
            elif not (0.0 <= float(energy) <= 500.0):
                reason = f"OUT_OF_RANGE_ENERGY_KWH ({energy})"
            elif not (10.0 <= float(temp) <= 45.0):
                reason = f"THERMAL_SPIKE_ANOMALY ({temp}C)"
            elif not (0.0 <= float(hvac) <= 100.0):
                reason = f"INVALID_HVAC_LOAD_PCT ({hvac}%)"
            else:
                seen_keys.add(dedup_key)
                telemetry_hash = hashlib.sha256(f"{zone_id}|{ts_norm}".encode("utf-8")).hexdigest()[:24]
                clean_rows.append({
                    "telemetry_id": telemetry_hash,
                    "zone_id": zone_id,
                    "timestamp_utc": ts_norm,
                    "energy_kwh": round(float(energy), 2),
                    "indoor_temp_c": round(float(temp), 2),
                    "hvac_load_pct": round(float(hvac), 1),
                })

        if reason:
            quarantine_rows.append({
                "run_id": run_id,
                "zone_id": zone_id,
                "timestamp_utc": ts_raw,
                "raw_payload": json.dumps(raw_dict),
                "failure_reason": reason,
                "quarantined_at_utc": now_iso,
            })

    total_raw = len(raw_sensor_df)
    dq_pass_rate = round((len(clean_rows) / total_raw) * 100.0, 2) if total_raw > 0 else 0.0

    clean_df = pd.DataFrame(clean_rows)
    quarantine_df = pd.DataFrame(quarantine_rows)

    if not clean_df.empty:
        clean_df = clean_df.merge(
            weather_df[["timestamp_utc", "outdoor_temp_c"]],
            on="timestamp_utc",
            how="left",
        )
        clean_df["thermal_delta_c"] = (clean_df["outdoor_temp_c"] - clean_df["indoor_temp_c"]).round(2)

    logger.info(
        "Validation complete | Raw: %d | Clean: %d | Quarantined: %d | Pass Rate: %.2f%%",
        total_raw,
        len(clean_df),
        len(quarantine_df),
        dq_pass_rate,
    )
    return clean_df, quarantine_df, dq_pass_rate


def load_to_warehouse(
    db_path: str,
    schema_path: str,
    weather_df: pd.DataFrame,
    clean_df: pd.DataFrame,
    quarantine_df: pd.DataFrame,
    run_id: str,
    raw_count: int,
    dq_pass_rate: float,
    status: str,
) -> None:
    """Idempotently load dimensions, facts, quarantine records, and run audit into SQLite."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    with sqlite3.connect(db_path) as conn:
        with open(schema_path, "r", encoding="utf-8") as f:
            conn.executescript(f.read())

        # 1. Upsert dim_building_zone
        for z in ZONES:
            conn.execute(
                """
                INSERT INTO dim_building_zone (zone_id, zone_name, floor_level, target_temp_c)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(zone_id) DO UPDATE SET
                    zone_name = excluded.zone_name,
                    floor_level = excluded.floor_level,
                    target_temp_c = excluded.target_temp_c
                """,
                (z["zone_id"], z["zone_name"], z["floor_level"], z["target_temp_c"]),
            )

        # 2. Upsert dim_weather_condition
        for _, w in weather_df.iterrows():
            conn.execute(
                """
                INSERT INTO dim_weather_condition (timestamp_utc, outdoor_temp_c, humidity_pct, weather_code, weather_category)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(timestamp_utc) DO UPDATE SET
                    outdoor_temp_c = excluded.outdoor_temp_c,
                    humidity_pct = excluded.humidity_pct,
                    weather_code = excluded.weather_code,
                    weather_category = excluded.weather_category
                """,
                (
                    w["timestamp_utc"],
                    float(w["outdoor_temp_c"]),
                    float(w["humidity_pct"]),
                    int(w["weather_code"]),
                    w["weather_category"],
                ),
            )

        # Lookup foreign keys
        zone_map = dict(conn.execute("SELECT zone_id, zone_key FROM dim_building_zone").fetchall())
        weather_map = dict(conn.execute("SELECT timestamp_utc, weather_key FROM dim_weather_condition").fetchall())

        # 3. Upsert fact_energy_telemetry (Idempotent load)
        for _, row in clean_df.iterrows():
            z_key = zone_map[row["zone_id"]]
            w_key = weather_map.get(row["timestamp_utc"])
            thermal_delta = None if pd.isna(row.get("thermal_delta_c")) else float(row["thermal_delta_c"])
            conn.execute(
                """
                INSERT INTO fact_energy_telemetry (
                    telemetry_id, zone_key, weather_key, timestamp_utc,
                    energy_kwh, indoor_temp_c, hvac_load_pct, thermal_delta_c, loaded_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(telemetry_id) DO UPDATE SET
                    weather_key = excluded.weather_key,
                    energy_kwh = excluded.energy_kwh,
                    indoor_temp_c = excluded.indoor_temp_c,
                    hvac_load_pct = excluded.hvac_load_pct,
                    thermal_delta_c = excluded.thermal_delta_c,
                    loaded_at_utc = excluded.loaded_at_utc
                """,
                (
                    row["telemetry_id"],
                    z_key,
                    w_key,
                    row["timestamp_utc"],
                    float(row["energy_kwh"]),
                    float(row["indoor_temp_c"]),
                    float(row["hvac_load_pct"]),
                    thermal_delta,
                    now_iso,
                ),
            )

        # 4. Append quarantine logs
        for _, q in quarantine_df.iterrows():
            conn.execute(
                """
                INSERT INTO quarantine_logs (run_id, zone_id, timestamp_utc, raw_payload, failure_reason, quarantined_at_utc)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (q["run_id"], q["zone_id"], q["timestamp_utc"], q["raw_payload"], q["failure_reason"], q["quarantined_at_utc"]),
            )

        # 5. Record ETL run audit
        conn.execute(
            """
            INSERT INTO etl_run_audit (run_id, run_timestamp_utc, raw_records, clean_records, quarantined_records, dq_pass_rate, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, now_iso, raw_count, len(clean_df), len(quarantine_df), dq_pass_rate, status),
        )
        conn.commit()


def run_pipeline(db_path: str = DEFAULT_DB_PATH, schema_path: str = SCHEMA_PATH) -> Dict[str, Any]:
    run_id = f"{datetime.now(timezone.utc).strftime('RUN-%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"
    logger.info("Starting Smart Building ETL Pipeline [%s]", run_id)

    weather_df = extract_weather_data()
    raw_sensor_df = extract_sensor_telemetry(hours=48)
    clean_df, quarantine_df, dq_pass_rate = transform_and_validate(raw_sensor_df, weather_df, run_id)

    status = "SUCCESS" if dq_pass_rate >= DQ_PASS_THRESHOLD else "FAILED_DQ_GATE"
    load_to_warehouse(
        db_path=db_path,
        schema_path=schema_path,
        weather_df=weather_df,
        clean_df=clean_df,
        quarantine_df=quarantine_df,
        run_id=run_id,
        raw_count=len(raw_sensor_df),
        dq_pass_rate=dq_pass_rate,
        status=status,
    )

    if dq_pass_rate < DQ_PASS_THRESHOLD:
        raise DataQualityThresholdError(
            f"Data quality pass rate {dq_pass_rate}% fell below required {DQ_PASS_THRESHOLD}% SLA."
        )

    return {
        "run_id": run_id,
        "raw_records": len(raw_sensor_df),
        "clean_records": len(clean_df),
        "quarantined_records": len(quarantine_df),
        "dq_pass_rate": dq_pass_rate,
        "status": status,
    }


if __name__ == "__main__":
    summary = run_pipeline()
    logger.info("Pipeline completed successfully: %s", json.dumps(summary))
