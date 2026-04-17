"""
Oura Ring → PostgreSQL daily ETL pipeline.
Replaces the n8n "Oura daily update" workflow with pure Python.

Usage:
    python oura_etl.py              # run once (manual or called by launchd/cron)
    python oura_etl.py --days 30    # backfill last 30 days
    python oura_etl.py --daemon     # run as persistent scheduler (every day at 11:00)
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ─── Configuration ──────────────────────────────────────────────────────────

load_dotenv(Path(__file__).parent / ".env")

OURA_TOKEN = os.environ["OURA_PERSONAL_ACCESS_TOKEN"]
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "oura")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "")

OURA_BASE = "https://api.ouraring.com/v2/usercollection"
HEADERS = {"Authorization": f"Bearer {OURA_TOKEN}"}

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "oura_etl.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("oura_etl")

# ─── API helpers ────────────────────────────────────────────────────────────

def fetch_oura(endpoint: str, params: dict) -> list[dict]:
    """Fetch paginated data from Oura API v2."""
    url = f"{OURA_BASE}/{endpoint}"
    all_data = []
    while url:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        all_data.extend(body.get("data", []))
        url = body.get("next_token")
        if url:
            # next_token means we pass it as param, not a full URL
            params = {"next_token": url}
            url = f"{OURA_BASE}/{endpoint}"
    return all_data


def fetch_date_endpoints(start_date: str, end_date: str) -> dict[str, list[dict]]:
    """Fetch all date-based endpoints in sequence (safe for local use)."""
    endpoints = {
        "sleep": "sleep",
        "daily_sleep": "daily_sleep",
        "daily_activity": "daily_activity",
        "daily_readiness": "daily_readiness",
        "daily_stress": "daily_stress",
        "workout": "workout",
        "enhanced_tag": "enhanced_tag",
    }
    results = {}
    for key, ep in endpoints.items():
        log.info(f"Fetching {key}...")
        try:
            results[key] = fetch_oura(ep, {"start_date": start_date, "end_date": end_date})
            log.info(f"  → {len(results[key])} records")
        except Exception as e:
            log.error(f"  ✗ {key} failed: {e}")
            results[key] = []
    return results


def fetch_heartrate(start_date: str, end_date: str) -> list[dict]:
    """Heart rate uses datetime params, not date."""
    log.info("Fetching heartrate...")
    params = {
        "start_datetime": f"{start_date}T00:00:00+00:00",
        "end_datetime": f"{end_date}T23:59:59+00:00",
    }
    data = fetch_oura("heartrate", params)
    log.info(f"  → {len(data)} records")
    return data

# ─── Database helpers ───────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
    )


def safe(val):
    """Return None for null-like values, passthrough otherwise."""
    if val is None or val == "":
        return None
    return val


def safe_json(val):
    """Serialize lists/dicts to JSON string, or None."""
    if val is None:
        return None
    if isinstance(val, (list, dict)):
        return json.dumps(val)
    return str(val)

# ─── Upsert functions (mirror n8n SQL exactly) ─────────────────────────────

def upsert_sleep(cur, records: list[dict]):
    if not records:
        return
    sql = """
    INSERT INTO oura_sleep (
        id, day, bedtime_start, bedtime_end, type,
        total_sleep_duration, deep_sleep_duration, rem_sleep_duration,
        light_sleep_duration, awake_time, latency, efficiency,
        average_heart_rate, lowest_heart_rate, average_hrv, average_breath,
        time_in_bed, restless_periods, sleep_phase_5_min,
        heart_rate_5_min, hrv_5_min, movement_30_sec
    ) VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        bedtime_start = EXCLUDED.bedtime_start,
        bedtime_end = EXCLUDED.bedtime_end,
        type = EXCLUDED.type,
        total_sleep_duration = EXCLUDED.total_sleep_duration,
        deep_sleep_duration = EXCLUDED.deep_sleep_duration,
        rem_sleep_duration = EXCLUDED.rem_sleep_duration,
        light_sleep_duration = EXCLUDED.light_sleep_duration,
        awake_time = EXCLUDED.awake_time,
        latency = EXCLUDED.latency,
        efficiency = EXCLUDED.efficiency,
        average_heart_rate = EXCLUDED.average_heart_rate,
        lowest_heart_rate = EXCLUDED.lowest_heart_rate,
        average_hrv = EXCLUDED.average_hrv,
        average_breath = EXCLUDED.average_breath,
        time_in_bed = EXCLUDED.time_in_bed,
        restless_periods = EXCLUDED.restless_periods,
        sleep_phase_5_min = EXCLUDED.sleep_phase_5_min,
        heart_rate_5_min = EXCLUDED.heart_rate_5_min,
        hrv_5_min = EXCLUDED.hrv_5_min,
        movement_30_sec = EXCLUDED.movement_30_sec
    """
    rows = []
    for r in records:
        hr = r.get("heart_rate")
        hrv = r.get("hrv")
        rows.append((
            r["id"], r["day"], r["bedtime_start"], r["bedtime_end"], r.get("type"),
            r.get("total_sleep_duration"), r.get("deep_sleep_duration"),
            r.get("rem_sleep_duration"), r.get("light_sleep_duration"),
            r.get("awake_time"), safe(r.get("latency")), r.get("efficiency"),
            safe(r.get("average_heart_rate")) if r.get("average_heart_rate") != 0 else None,
            safe(r.get("lowest_heart_rate")), safe(r.get("average_hrv")),
            safe(r.get("average_breath")), r.get("time_in_bed"),
            safe(r.get("restless_periods")), safe(r.get("sleep_phase_5_min")),
            safe_json(hr), safe_json(hrv), safe(r.get("movement_30_sec")),
        ))
    execute_values(cur, sql, rows)
    log.info(f"  Upserted {len(rows)} sleep records")


def upsert_daily_sleep(cur, records: list[dict]):
    if not records:
        return
    sql = """
    INSERT INTO oura_daily_sleep (
        day, score, deep_sleep, efficiency, latency,
        rem_sleep, restfulness, timing, total_sleep
    ) VALUES %s
    ON CONFLICT (day) DO UPDATE SET
        score = EXCLUDED.score,
        deep_sleep = EXCLUDED.deep_sleep,
        efficiency = EXCLUDED.efficiency,
        latency = EXCLUDED.latency,
        rem_sleep = EXCLUDED.rem_sleep,
        restfulness = EXCLUDED.restfulness,
        timing = EXCLUDED.timing,
        total_sleep = EXCLUDED.total_sleep
    """
    rows = []
    for r in records:
        c = r.get("contributors") or {}
        rows.append((
            r["day"], safe(r.get("score")),
            safe(c.get("deep_sleep")), safe(c.get("efficiency")),
            safe(c.get("latency")), safe(c.get("rem_sleep")),
            safe(c.get("restfulness")), safe(c.get("timing")),
            safe(c.get("total_sleep")),
        ))
    execute_values(cur, sql, rows)
    log.info(f"  Upserted {len(rows)} daily_sleep records")


def upsert_activity(cur, records: list[dict]):
    if not records:
        return
    sql = """
    INSERT INTO oura_daily_activity (
        day, score, active_calories, average_met_minutes, equivalent_walking_distance,
        high_activity_met_minutes, high_activity_time, inactivity_alerts,
        low_activity_met_minutes, low_activity_time,
        medium_activity_met_minutes, medium_activity_time,
        meters_to_target, non_wear_time, resting_time,
        sedentary_met_minutes, sedentary_time,
        steps, target_calories, target_meters, total_calories,
        meet_daily_targets, move_every_hour, recovery_time,
        stay_active, training_frequency, training_volume
    ) VALUES %s
    ON CONFLICT (day) DO UPDATE SET
        score = EXCLUDED.score,
        active_calories = EXCLUDED.active_calories,
        steps = EXCLUDED.steps,
        total_calories = EXCLUDED.total_calories,
        non_wear_time = EXCLUDED.non_wear_time,
        meet_daily_targets = EXCLUDED.meet_daily_targets,
        move_every_hour = EXCLUDED.move_every_hour,
        recovery_time = EXCLUDED.recovery_time,
        stay_active = EXCLUDED.stay_active,
        training_frequency = EXCLUDED.training_frequency,
        training_volume = EXCLUDED.training_volume
    """
    rows = []
    for r in records:
        c = r.get("contributors") or {}
        rows.append((
            r["day"], safe(r.get("score")),
            safe(r.get("active_calories")), safe(r.get("average_met_minutes")),
            safe(r.get("equivalent_walking_distance")),
            safe(r.get("high_activity_met_minutes")), safe(r.get("high_activity_time")),
            safe(r.get("inactivity_alerts")),
            safe(r.get("low_activity_met_minutes")), safe(r.get("low_activity_time")),
            safe(r.get("medium_activity_met_minutes")), safe(r.get("medium_activity_time")),
            safe(r.get("meters_to_target")), safe(r.get("non_wear_time")),
            safe(r.get("resting_time")),
            safe(r.get("sedentary_met_minutes")), safe(r.get("sedentary_time")),
            safe(r.get("steps")), safe(r.get("target_calories")),
            safe(r.get("target_meters")), safe(r.get("total_calories")),
            safe(c.get("meet_daily_targets")), safe(c.get("move_every_hour")),
            safe(c.get("recovery_time")), safe(c.get("stay_active")),
            safe(c.get("training_frequency")), safe(c.get("training_volume")),
        ))
    execute_values(cur, sql, rows)
    log.info(f"  Upserted {len(rows)} activity records")


def upsert_readiness(cur, records: list[dict]):
    if not records:
        return
    sql = """
    INSERT INTO oura_daily_readiness (
        day, score, activity_balance, body_temperature,
        hrv_balance, previous_day_activity, previous_night,
        recovery_index, resting_heart_rate, sleep_balance,
        temperature_deviation, temperature_trend_deviation
    ) VALUES %s
    ON CONFLICT (day) DO UPDATE SET
        score = EXCLUDED.score,
        activity_balance = EXCLUDED.activity_balance,
        body_temperature = EXCLUDED.body_temperature,
        hrv_balance = EXCLUDED.hrv_balance,
        previous_day_activity = EXCLUDED.previous_day_activity,
        previous_night = EXCLUDED.previous_night,
        recovery_index = EXCLUDED.recovery_index,
        resting_heart_rate = EXCLUDED.resting_heart_rate,
        sleep_balance = EXCLUDED.sleep_balance,
        temperature_deviation = EXCLUDED.temperature_deviation,
        temperature_trend_deviation = EXCLUDED.temperature_trend_deviation
    """
    rows = []
    for r in records:
        c = r.get("contributors") or {}
        rows.append((
            r["day"], safe(r.get("score")),
            safe(c.get("activity_balance")), safe(c.get("body_temperature")),
            safe(c.get("hrv_balance")), safe(c.get("previous_day_activity")),
            safe(c.get("previous_night")), safe(c.get("recovery_index")),
            safe(c.get("resting_heart_rate")), safe(c.get("sleep_balance")),
            safe(r.get("temperature_deviation")),
            safe(r.get("temperature_trend_deviation")),
        ))
    execute_values(cur, sql, rows)
    log.info(f"  Upserted {len(rows)} readiness records")


def upsert_stress(cur, records: list[dict]):
    if not records:
        return
    sql = """
    INSERT INTO oura_daily_stress (
        day, stress_high, recovery_high, day_summary
    ) VALUES %s
    ON CONFLICT (day) DO UPDATE SET
        stress_high = EXCLUDED.stress_high,
        recovery_high = EXCLUDED.recovery_high,
        day_summary = EXCLUDED.day_summary
    """
    rows = [(
        r["day"], safe(r.get("stress_high")),
        safe(r.get("recovery_high")), safe(r.get("day_summary")),
    ) for r in records]
    execute_values(cur, sql, rows)
    log.info(f"  Upserted {len(rows)} stress records")


def upsert_workouts(cur, records: list[dict]):
    if not records:
        return
    sql = """
    INSERT INTO oura_workouts (
        id, day, activity, calories, day_start, day_end,
        distance, intensity, label, source
    ) VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        activity = EXCLUDED.activity,
        calories = EXCLUDED.calories,
        day_start = EXCLUDED.day_start,
        day_end = EXCLUDED.day_end,
        distance = EXCLUDED.distance,
        intensity = EXCLUDED.intensity,
        label = EXCLUDED.label
    """
    rows = [(
        r["id"], r["day"], safe(r.get("activity")), safe(r.get("calories")),
        safe(r.get("start_datetime")), safe(r.get("end_datetime")),
        safe(r.get("distance")), safe(r.get("intensity")),
        safe(r.get("label")), safe(r.get("source")),
    ) for r in records]
    execute_values(cur, sql, rows)
    log.info(f"  Upserted {len(rows)} workout records")


def upsert_events(cur, records: list[dict]):
    if not records:
        return
    sql = """
    INSERT INTO oura_events (
        id, tag_type_code, custom_name,
        start_day, end_day, start_time, end_time, comment
    ) VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        tag_type_code = EXCLUDED.tag_type_code,
        custom_name = EXCLUDED.custom_name,
        start_day = EXCLUDED.start_day,
        end_day = EXCLUDED.end_day,
        start_time = EXCLUDED.start_time,
        end_time = EXCLUDED.end_time,
        comment = EXCLUDED.comment
    """
    rows = [(
        r["id"], safe(r.get("tag_type_code")), safe(r.get("custom_name")),
        safe(r.get("start_day")), safe(r.get("end_day")),
        safe(r.get("start_time")), safe(r.get("end_time")),
        safe(r.get("comment")),
    ) for r in records]
    execute_values(cur, sql, rows)
    log.info(f"  Upserted {len(rows)} event records")


def upsert_heartrate(cur, records: list[dict]):
    if not records:
        return
    sql = """
    INSERT INTO oura_heartrate (timestamp, bpm, source)
    VALUES %s
    ON CONFLICT (timestamp) DO NOTHING
    """
    rows = [(r["timestamp"], safe(r.get("bpm")), safe(r.get("source"))) for r in records]
    # Heart rate can have thousands of rows — batch in chunks
    CHUNK = 5000
    total = 0
    for i in range(0, len(rows), CHUNK):
        execute_values(cur, sql, rows[i : i + CHUNK])
        total += len(rows[i : i + CHUNK])
    log.info(f"  Upserted {total} heartrate records")


def refresh_cycles(cur):
    """Refresh oura_cycles derived table — exact copy of n8n SQL."""
    sql = """
    WITH period_days AS (
        SELECT DISTINCT start_day AS day
        FROM oura_events
        WHERE tag_type_code = 'tag_generic_period'
    ),
    period_starts AS (
        SELECT day AS start_day
        FROM period_days pd
        WHERE NOT EXISTS (
            SELECT 1 FROM period_days pd2
            WHERE pd2.day = pd.day - INTERVAL '1 day'
        )
    ),
    cycle_lengths AS (
        SELECT
            start_day,
            LEAD(start_day) OVER (ORDER BY start_day) AS next_start,
            LEAD(start_day) OVER (ORDER BY start_day) - start_day AS cycle_length
        FROM period_starts
    ),
    day_cycle AS (
        SELECT
            dr.day,
            cl.start_day        AS cycle_start,
            cl.cycle_length,
            (dr.day - cl.start_day + 1) AS cycle_day,
            dr.temperature_deviation    AS temp_deviation
        FROM oura_daily_readiness dr
        CROSS JOIN LATERAL (
            SELECT start_day, cycle_length
            FROM cycle_lengths
            WHERE start_day <= dr.day
            ORDER BY start_day DESC
            LIMIT 1
        ) cl
        WHERE dr.day >= CURRENT_DATE - INTERVAL '5 days'
          AND dr.temperature_deviation IS NOT NULL
    ),
    phase_calc AS (
        SELECT
            dc.*,
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM period_days pd WHERE pd.day = dc.day
                )                                               THEN 'menstrual'
                WHEN dc.cycle_day <= 13                         THEN 'follicular'
                WHEN dc.temp_deviation > 0.15                   THEN 'luteal'
                WHEN dc.cycle_day > 13                          THEN 'luteal'
                ELSE                                                 'unknown'
            END AS phase,
            CASE
                WHEN dc.temp_deviation < -0.1                   THEN 'low'
                WHEN dc.temp_deviation BETWEEN -0.1 AND 0.1     THEN 'stable'
                WHEN dc.temp_deviation BETWEEN 0.1 AND 0.3      THEN 'rising'
                WHEN dc.temp_deviation > 0.3                    THEN 'high'
                ELSE                                                 'unknown'
            END AS temperature_phase
        FROM day_cycle dc
    )
    INSERT INTO oura_cycles (
        day, cycle_start, cycle_day, cycle_length, phase,
        temperature_phase, temp_deviation,
        is_period, has_migraine, has_pms, has_cramps
    )
    SELECT
        pc.day,
        pc.cycle_start,
        pc.cycle_day,
        pc.cycle_length,
        pc.phase,
        pc.temperature_phase,
        pc.temp_deviation,
        EXISTS (SELECT 1 FROM oura_events e
                WHERE e.start_day = pc.day
                  AND e.tag_type_code = 'tag_generic_period')    AS is_period,
        EXISTS (SELECT 1 FROM oura_events e
                WHERE e.start_day = pc.day
                  AND e.tag_type_code = 'tag_generic_migraine')  AS has_migraine,
        EXISTS (SELECT 1 FROM oura_events e
                WHERE e.start_day = pc.day
                  AND e.tag_type_code = 'tag_generic_pms')       AS has_pms,
        EXISTS (SELECT 1 FROM oura_events e
                WHERE e.start_day = pc.day
                  AND e.tag_type_code = 'tag_generic_cramps')    AS has_cramps
    FROM phase_calc pc
    ON CONFLICT (day) DO UPDATE SET
        cycle_start       = EXCLUDED.cycle_start,
        cycle_day         = EXCLUDED.cycle_day,
        cycle_length      = EXCLUDED.cycle_length,
        phase             = EXCLUDED.phase,
        temperature_phase = EXCLUDED.temperature_phase,
        temp_deviation    = EXCLUDED.temp_deviation,
        is_period         = EXCLUDED.is_period,
        has_migraine      = EXCLUDED.has_migraine,
        has_pms           = EXCLUDED.has_pms,
        has_cramps        = EXCLUDED.has_cramps;
    """
    cur.execute(sql)
    log.info(f"  Refreshed oura_cycles ({cur.rowcount} rows)")

# ─── Main pipeline ──────────────────────────────────────────────────────────

def run_pipeline(lookback_days: int = 3):
    """Execute the full ETL pipeline."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    log.info(f"═══ Oura ETL start: {start} → {today} ({lookback_days}d lookback) ═══")

    # 1. Fetch all data from Oura API
    data = fetch_date_endpoints(start, today)
    data["heartrate"] = fetch_heartrate(start, today)

    # 2. Upsert into Postgres
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            upsert_sleep(cur, data["sleep"])
            upsert_daily_sleep(cur, data["daily_sleep"])
            upsert_activity(cur, data["daily_activity"])
            upsert_readiness(cur, data["daily_readiness"])
            upsert_stress(cur, data["daily_stress"])
            upsert_workouts(cur, data["workout"])
            upsert_events(cur, data["enhanced_tag"])
            upsert_heartrate(cur, data["heartrate"])

            # 3. Refresh derived cycles table (after readiness + events)
            refresh_cycles(cur)

        conn.commit()
        log.info("═══ ETL complete ═══")
    except Exception as e:
        conn.rollback()
        log.error(f"ETL failed, rolled back: {e}")
        raise
    finally:
        conn.close()

# ─── CLI ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Oura → Postgres ETL")
    parser.add_argument("--days", type=int, default=3,
                        help="Lookback days (default: 3)")
    parser.add_argument("--daemon", action="store_true",
                        help="Run as persistent scheduler (daily at 11:00)")
    args = parser.parse_args()

    if args.daemon:
        try:
            from apscheduler.schedulers.blocking import BlockingScheduler
            from apscheduler.triggers.cron import CronTrigger
        except ImportError:
            log.error("apscheduler not installed. Run: pip install apscheduler")
            sys.exit(1)

        scheduler = BlockingScheduler(timezone="Europe/Warsaw")
        scheduler.add_job(
            run_pipeline,
            CronTrigger(hour=11, minute=0),
            name="oura_daily_update",
        )
        log.info("Scheduler started — daily at 11:00 Europe/Warsaw. Ctrl+C to stop.")
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            log.info("Scheduler stopped.")
    else:
        run_pipeline(lookback_days=args.days)


if __name__ == "__main__":
    main()
