-- Star Schema & Data Quality Audit Tables for Smart Building Telemetry

CREATE TABLE IF NOT EXISTS dim_building_zone (
    zone_key INTEGER PRIMARY KEY AUTOINCREMENT,
    zone_id TEXT NOT NULL UNIQUE,
    zone_name TEXT NOT NULL,
    floor_level INTEGER NOT NULL,
    target_temp_c REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_weather_condition (
    weather_key INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_utc TEXT NOT NULL UNIQUE,
    outdoor_temp_c REAL NOT NULL,
    humidity_pct REAL NOT NULL,
    weather_code INTEGER NOT NULL,
    weather_category TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_energy_telemetry (
    telemetry_id TEXT PRIMARY KEY,
    zone_key INTEGER NOT NULL,
    weather_key INTEGER,
    timestamp_utc TEXT NOT NULL,
    energy_kwh REAL NOT NULL,
    indoor_temp_c REAL NOT NULL,
    hvac_load_pct REAL NOT NULL,
    thermal_delta_c REAL,
    loaded_at_utc TEXT NOT NULL,
    FOREIGN KEY (zone_key) REFERENCES dim_building_zone(zone_key),
    FOREIGN KEY (weather_key) REFERENCES dim_weather_condition(weather_key)
);

CREATE TABLE IF NOT EXISTS quarantine_logs (
    quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    zone_id TEXT,
    timestamp_utc TEXT,
    raw_payload TEXT NOT NULL,
    failure_reason TEXT NOT NULL,
    quarantined_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS etl_run_audit (
    run_id TEXT PRIMARY KEY,
    run_timestamp_utc TEXT NOT NULL,
    raw_records INTEGER NOT NULL,
    clean_records INTEGER NOT NULL,
    quarantined_records INTEGER NOT NULL,
    dq_pass_rate REAL NOT NULL,
    status TEXT NOT NULL
);
