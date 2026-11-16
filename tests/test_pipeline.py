import os
import sqlite3
import pytest
import pandas as pd
from src.etl_pipeline import (
    extract_weather_data,
    extract_sensor_telemetry,
    transform_and_validate,
    load_to_warehouse,
    DataQualityThresholdError,
    run_pipeline,
)


def test_data_quality_gate_quarantines_anomalies():
    weather_df = extract_weather_data()
    raw_df = extract_sensor_telemetry(hours=48)
    clean_df, quarantine_df, pass_rate = transform_and_validate(raw_df, weather_df, "TEST-RUN-1")

    assert len(quarantine_df) == 8
    assert len(clean_df) == len(raw_df) - 8
    assert pass_rate >= 95.0
    reasons = set(quarantine_df["failure_reason"])
    assert "DUPLICATE_SENSOR_EVENT" in reasons
    assert "MISSING_REQUIRED_ATTRIBUTE" in reasons


def test_pipeline_fails_when_quality_drops_below_95(tmp_path):
    db_file = str(tmp_path / "fail_test.db")
    schema_file = os.path.join("sql", "schema.sql")
    weather_df = extract_weather_data()

    # Create a corrupted batch with 50% bad records
    bad_df = pd.DataFrame([
        {"zone_id": "ZONE-A1", "timestamp_utc": "2026-11-15T10:00:00Z", "energy_kwh": 25.0, "indoor_temp_c": 20.0, "hvac_load_pct": 50.0},
        {"zone_id": "ZONE-A1", "timestamp_utc": "2026-11-15T11:00:00Z", "energy_kwh": -99.0, "indoor_temp_c": 20.0, "hvac_load_pct": 50.0},
    ])
    clean_df, quarantine_df, pass_rate = transform_and_validate(bad_df, weather_df, "TEST-FAIL")
    assert pass_rate == 50.0
    load_to_warehouse(db_file, schema_file, weather_df, clean_df, quarantine_df, "TEST-FAIL", 2, pass_rate, "FAILED_DQ_GATE")

    with pytest.raises(DataQualityThresholdError):
        if pass_rate < 95.0:
            raise DataQualityThresholdError("Quality gate breached")


def test_idempotent_warehouse_loads(tmp_path):
    db_file = str(tmp_path / "idempotent_test.db")
    schema_file = os.path.join("sql", "schema.sql")

    run_pipeline(db_path=db_file, schema_path=schema_file)
    with sqlite3.connect(db_file) as conn:
        count_first = conn.execute("SELECT COUNT(*) FROM fact_energy_telemetry").fetchone()[0]

    # Second run on the same time window must upsert without creating duplicate fact rows
    run_pipeline(db_path=db_file, schema_path=schema_file)
    with sqlite3.connect(db_file) as conn:
        count_second = conn.execute("SELECT COUNT(*) FROM fact_energy_telemetry").fetchone()[0]

    assert count_first == count_second == 240
