# Smart Building Energy & Weather ETL Pipeline

An automated, idempotent **Data Engineering ETL Pipeline** that integrates Smart Building HVAC & energy sensor telemetry with live outdoor weather data from the **Open-Meteo REST API**, enforces automated **Data Quality (DQ) gates**, and loads validated records into a **Star Schema Data Warehouse**.

Built to demonstrate industrial telemetry integration, dimensional data modeling, quarantine auditing, and CI/CD pipeline orchestration.

---

## System Architecture

```text
+-----------------------------------+       +-----------------------------------+
|  Source 1: Building IoT Sensors   |       |  Source 2: Open-Meteo REST API    |
|  (5 Zones: HVAC Load, kWh, Temp)  |       |  (Hourly Outdoor Temp & Humidity) |
+-----------------+-----------------+       +-----------------+-----------------+
                  |                                           |
                  +---------------------+---------------------+
                                        |
                                        v
                   +-----------------------------------------+
                   |      TRANSFORM & DATA QUALITY GATE      |
                   |  * UTC ISO-8601 Timestamp Normalization |
                   |  * Composite Key Deduplication          |
                   |  * Physical Bounds & Null Validation    |
                   |  * SLA Gate Check (Pass Rate >= 95.0%)  |
                   +--------------------+--------------------+
                                        |
                      +-----------------+-----------------+
                      | (Valid: 96.77%)                   | (Invalid: 3.23%)
                      v                                   v
     +---------------------------------+   +-------------------------------+
     |   STAR SCHEMA DATA WAREHOUSE    |   |       QUARANTINE & AUDIT      |
     |  * fact_energy_telemetry        |   |  * quarantine_logs (JSON)     |
     |  * dim_building_zone            |   |  * etl_run_audit (SLA Log)    |
     |  * dim_weather_condition        |   +-------------------------------+
     +---------------------------------+
                      ^
                      |
     +---------------------------------+
     |   ORCHESTRATION (GitHub Actions)|
     |  * Scheduled Cron (Every 6 Hrs) |
     |  * Pytest Idempotency Checks    |
     +---------------------------------+
```

---

## Dimensional Data Model (Star Schema)

* **`fact_energy_telemetry`**: Grain = 1 hourly reading per building zone. Uses a deterministic SHA-256 `telemetry_id` (`zone_id + timestamp_utc`) for idempotent `UPSERT` loads. Stores `energy_kwh`, `indoor_temp_c`, `hvac_load_pct`, and derived `thermal_delta_c` (`outdoor_temp_c - indoor_temp_c`).
* **`dim_building_zone`**: Building zone metadata (`zone_id`, `zone_name`, `floor_level`, `target_temp_c`).
* **`dim_weather_condition`**: Hourly meteorological conditions (`timestamp_utc`, `outdoor_temp_c`, `humidity_pct`, `weather_code`, `weather_category`).
* **`quarantine_logs`**: Captures rejected sensor payloads alongside the exact `run_id` and `failure_reason` without crashing valid records.
* **`etl_run_audit`**: Tracks batch-level throughput (`raw_records`, `clean_records`, `quarantined_records`, `dq_pass_rate`, `status`).

---

## Data Quality Rules & SLA Enforcement

Every batch is validated against four domain integrity rules before loading:

1. **Completeness Check:** Rejects records missing `zone_id`, `timestamp_utc`, `energy_kwh`, `indoor_temp_c`, or `hvac_load_pct` (`MISSING_REQUIRED_ATTRIBUTE`).
2. **Deduplication:** Drops exact duplicate `(zone_id, timestamp_utc)` sensor events (`DUPLICATE_SENSOR_EVENT`).
3. **Energy Bounds:** Flags physically impossible meter readings outside `[0.0, 500.0] kWh` (`OUT_OF_RANGE_ENERGY_KWH`).
4. **Thermal & HVAC Bounds:** Quarantines faulty thermistor spikes outside `[10.0, 45.0] °C` (`THERMAL_SPIKE_ANOMALY`) or HVAC load outside `[0.0, 100.0]%`.

If the batch **Data Quality Pass Rate drops below 95.0%**, the pipeline raises `DataQualityThresholdError` and fails the GitHub Actions workflow.

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run unit & idempotency tests
PYTHONPATH=. pytest -v

# 3. Execute the ETL pipeline
PYTHONPATH=. python src/etl_pipeline.py
```
