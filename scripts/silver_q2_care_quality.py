import os
import sys
import argparse
import psycopg2

sys.path.insert(0, os.path.dirname(__file__))
import dq_logger as dq

DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'postgres'),
    'port':     int(os.getenv('DB_PORT', 5432)),
    'dbname':   os.getenv('DB_NAME', 'airflow'),
    'user':     os.getenv('DB_USER', 'airflow'),
    'password': os.getenv('DB_PASSWORD', 'airflow'),
}

def _run(cursor, sql: str, label: str):
    cursor.execute(sql)
    print(f'  {label}: {cursor.rowcount} rows')


# -----------------------------------------------------------------------

def load_patients(cursor, run_id: str):
    cursor.execute('TRUNCATE dw_silver.patients')
    _run(cursor, """
        INSERT INTO dw_silver.patients (
            id, birth_year, age_group, gender, race, ethnicity,
            is_deceased, _dq_status, _dq_notes, _silver_processed_at
        )
        WITH latest AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) AS rn
            FROM dw_bronze.patients
        ),
        cleaned AS (
            SELECT
                id,
                EXTRACT(YEAR FROM birthdate)::INTEGER           AS birth_year,
                CASE
                    WHEN EXTRACT(YEAR FROM NOW()) - EXTRACT(YEAR FROM birthdate) <= 17  THEN '0-17'
                    WHEN EXTRACT(YEAR FROM NOW()) - EXTRACT(YEAR FROM birthdate) <= 34  THEN '18-34'
                    WHEN EXTRACT(YEAR FROM NOW()) - EXTRACT(YEAR FROM birthdate) <= 50  THEN '35-50'
                    WHEN EXTRACT(YEAR FROM NOW()) - EXTRACT(YEAR FROM birthdate) <= 65  THEN '51-65'
                    ELSE '65+'
                END                                             AS age_group,
                CASE
                    WHEN UPPER(TRIM(gender)) IN ('M','MALE')   THEN 'Male'
                    WHEN UPPER(TRIM(gender)) IN ('F','FEMALE') THEN 'Female'
                    ELSE 'Unknown'
                END                                             AS gender,
                TRIM(race)      AS race,
                TRIM(ethnicity) AS ethnicity,
                (deathdate IS NOT NULL)                         AS is_deceased,
                CASE WHEN birthdate IS NULL THEN 'WARN' ELSE 'PASS' END AS _dq_status,
                CASE WHEN birthdate IS NULL THEN 'missing birthdate — age_group unknown' ELSE NULL END AS _dq_notes
            FROM latest
            WHERE rn = 1 AND id IS NOT NULL
        )
        SELECT id, birth_year, age_group, gender, race, ethnicity,
               is_deceased, _dq_status, _dq_notes, NOW()
        FROM cleaned
    """, 'patients')

    t = 'dw_silver.patients'
    dq.log_metrics(cursor, run_id, 'silver_patients', [
        (None,         'completeness', dq.pct_pass_status(cursor, t),                        95.0),
        ('id',         'completeness', dq.pct_not_null(cursor, t, 'id'),                     100.0),
        ('birth_year', 'completeness', dq.pct_not_null(cursor, t, 'birth_year'),             95.0),
        ('birth_year', 'validity',     dq.pct_in_set(cursor, t, 'birth_year::TEXT',
                                           [str(y) for y in range(1900, 2027)]),             99.0),
        ('gender',     'validity',     dq.pct_in_set(cursor, t, 'gender',
                                           ['Male', 'Female', 'Unknown']),                   100.0),
        ('age_group',  'validity',     dq.pct_in_set(cursor, t, 'age_group',
                                           ['0-17','18-34','35-50','51-65','65+']),          100.0),
    ])


def load_encounters(cursor, run_id: str):
    cursor.execute('TRUNCATE dw_silver.encounters')
    _run(cursor, """
        INSERT INTO dw_silver.encounters (
            id, patient_id, payer_id, encounter_class,
            start_ts, stop_ts, duration_minutes,
            base_encounter_cost, total_claim_cost, payer_coverage, patient_oop,
            _dq_status, _dq_notes, _silver_processed_at
        )
        WITH latest AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) AS rn
            FROM dw_bronze.encounters
        ),
        cleaned AS (
            SELECT
                id,
                patient                                        AS patient_id,
                payer                                          AS payer_id,
                LOWER(TRIM(encounterclass))                    AS encounter_class,
                start                                          AS start_ts,
                stop                                           AS stop_ts,
                CASE
                    WHEN start IS NOT NULL AND stop IS NOT NULL
                    THEN EXTRACT(EPOCH FROM (stop - start))::INTEGER / 60
                END                                            AS duration_minutes,
                base_encounter_cost,
                total_claim_cost,
                payer_coverage,
                COALESCE(total_claim_cost, 0) - COALESCE(payer_coverage, 0) AS patient_oop,
                CASE
                    WHEN total_claim_cost < 0              THEN 'WARN'
                    WHEN stop IS NOT NULL AND stop < start THEN 'WARN'
                    ELSE 'PASS'
                END AS _dq_status,
                CASE
                    WHEN total_claim_cost < 0              THEN 'negative total_claim_cost'
                    WHEN stop IS NOT NULL AND stop < start THEN 'stop timestamp before start'
                    ELSE NULL
                END AS _dq_notes
            FROM latest
            WHERE rn = 1 AND id IS NOT NULL
        )
        SELECT id, patient_id, payer_id, encounter_class,
               start_ts, stop_ts, duration_minutes,
               base_encounter_cost, total_claim_cost, payer_coverage, patient_oop,
               _dq_status, _dq_notes, NOW()
        FROM cleaned
    """, 'encounters')

    t = 'dw_silver.encounters'
    dq.log_metrics(cursor, run_id, 'silver_encounters', [
        (None,             'completeness', dq.pct_pass_status(cursor, t),                         95.0),
        ('id',             'completeness', dq.pct_not_null(cursor, t, 'id'),                      100.0),
        ('patient_id',     'completeness', dq.pct_not_null(cursor, t, 'patient_id'),              100.0),
        ('encounter_class','validity',     dq.pct_in_set(cursor, t, 'encounter_class',
                                              ['wellness','ambulatory','outpatient','urgentcare',
                                               'emergency','inpatient','home','snf','hospice','virtual']), 95.0),
        ('total_claim_cost','accuracy',   dq.pct_gte_zero(cursor, t, 'total_claim_cost'),         99.0),
    ])


def load_conditions(cursor, run_id: str):
    cursor.execute('TRUNCATE dw_silver.conditions')
    _run(cursor, """
        INSERT INTO dw_silver.conditions (
            patient_id, encounter_id, code, description,
            start_date, end_date, is_active,
            _dq_status, _dq_notes, _silver_processed_at
        )
        WITH latest AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY patient, encounter, code
                    ORDER BY _ingested_at DESC
                ) AS rn
            FROM dw_bronze.conditions
        ),
        cleaned AS (
            SELECT
                patient             AS patient_id,
                encounter           AS encounter_id,
                TRIM(code)          AS code,
                TRIM(description)   AS description,
                start               AS start_date,
                stop                AS end_date,
                (stop IS NULL)      AS is_active,
                CASE WHEN encounter IS NULL THEN 'WARN' ELSE 'PASS' END AS _dq_status,
                CASE WHEN encounter IS NULL THEN 'missing encounter_id' ELSE NULL END AS _dq_notes
            FROM latest
            WHERE rn = 1 AND patient IS NOT NULL
        )
        SELECT patient_id, encounter_id, code, description,
               start_date, end_date, is_active,
               _dq_status, _dq_notes, NOW()
        FROM cleaned
    """, 'conditions')

    t = 'dw_silver.conditions'
    dq.log_metrics(cursor, run_id, 'silver_conditions', [
        (None,         'completeness', dq.pct_pass_status(cursor, t),                 95.0),
        ('patient_id', 'completeness', dq.pct_not_null(cursor, t, 'patient_id'),      100.0),
        ('code',       'completeness', dq.pct_not_null(cursor, t, 'code'),            100.0),
        ('start_date', 'completeness', dq.pct_not_null(cursor, t, 'start_date'),       95.0),
    ])


def load_careplans(cursor, run_id: str):
    cursor.execute('TRUNCATE dw_silver.careplans')
    _run(cursor, """
        INSERT INTO dw_silver.careplans (
            id, patient_id, encounter_id, code, description,
            start_date, end_date, is_active,
            reason_code, reason_description,
            _dq_status, _dq_notes, _silver_processed_at
        )
        WITH latest AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) AS rn
            FROM dw_bronze.careplans
        ),
        cleaned AS (
            SELECT
                id,
                patient                 AS patient_id,
                encounter               AS encounter_id,
                TRIM(code)              AS code,
                TRIM(description)       AS description,
                start                   AS start_date,
                stop                    AS end_date,
                (stop IS NULL)          AS is_active,
                TRIM(reasoncode)        AS reason_code,
                TRIM(reasondescription) AS reason_description,
                CASE WHEN encounter IS NULL THEN 'WARN' ELSE 'PASS' END AS _dq_status,
                CASE WHEN encounter IS NULL THEN 'missing encounter_id' ELSE NULL END AS _dq_notes
            FROM latest
            WHERE rn = 1 AND patient IS NOT NULL
        )
        SELECT id, patient_id, encounter_id, code, description,
               start_date, end_date, is_active, reason_code, reason_description,
               _dq_status, _dq_notes, NOW()
        FROM cleaned
    """, 'careplans')

    t = 'dw_silver.careplans'
    dq.log_metrics(cursor, run_id, 'silver_careplans', [
        (None,         'completeness', dq.pct_pass_status(cursor, t),                 95.0),
        ('patient_id', 'completeness', dq.pct_not_null(cursor, t, 'patient_id'),      100.0),
        ('code',       'completeness', dq.pct_not_null(cursor, t, 'code'),            100.0),
    ])


# -----------------------------------------------------------------------

STEPS = {
    'load_patients':        load_patients,
    'load_encounters':      load_encounters,
    'load_conditions':      load_conditions,
    'load_careplans':       load_careplans,
}


def main(step: str):
    run_id = dq.generate_run_id('silver_q2')
    print(f'[Silver Q2 Care Quality] step={step}  run_id={run_id}')
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()
    try:
        STEPS[step](cur, run_id)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    print(f'[Silver Q2 Care Quality] done: {step}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Silver Q2 Care Quality loader')
    parser.add_argument('--step', required=True, choices=list(STEPS))
    args = parser.parse_args()
    main(args.step)
