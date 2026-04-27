import os
import argparse
import psycopg2

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

def load_dim_date(cursor):
    cursor.execute('TRUNCATE dw_gold.dim_date CASCADE')
    _run(cursor, """
        INSERT INTO dw_gold.dim_date (
            date_key, full_date, day_of_week, day_of_month,
            week_of_year, month, month_name, quarter, year, is_weekend
        )
        SELECT
            TO_CHAR(d, 'YYYYMMDD')::INT         AS date_key,
            d                                   AS full_date,
            TO_CHAR(d, 'Day')                   AS day_of_week,
            EXTRACT(DAY   FROM d)::INT          AS day_of_month,
            EXTRACT(WEEK  FROM d)::INT          AS week_of_year,
            EXTRACT(MONTH FROM d)::INT          AS month,
            TO_CHAR(d, 'Month')                 AS month_name,
            EXTRACT(QUARTER FROM d)::INT        AS quarter,
            EXTRACT(YEAR  FROM d)::INT          AS year,
            EXTRACT(DOW   FROM d) IN (0, 6)     AS is_weekend
        FROM (
            SELECT generate_series(
                (SELECT MIN(start_ts)::DATE FROM dw_silver.encounters),
                CURRENT_DATE,
                INTERVAL '1 day'
            )::DATE AS d
        ) dates
        ON CONFLICT (date_key) DO NOTHING
    """, 'dim_date')


def load_dim_patients(cursor):
    cursor.execute('TRUNCATE dw_gold.dim_patients CASCADE')
    _run(cursor, """
        INSERT INTO dw_gold.dim_patients (
            patient_id, age_group, gender, race, ethnicity, is_deceased
        )
        SELECT id, age_group, gender, race, ethnicity, is_deceased
        FROM dw_silver.patients
        ON CONFLICT (patient_id) DO NOTHING
    """, 'dim_patients')


def load_dim_payers(cursor):
    cursor.execute('TRUNCATE dw_gold.dim_payers CASCADE')
    _run(cursor, """
        INSERT INTO dw_gold.dim_payers (payer_id, name, ownership)
        SELECT id, name, ownership
        FROM dw_silver.payers
        ON CONFLICT (payer_id) DO NOTHING
    """, 'dim_payers')


def load_dim_conditions(cursor):
    cursor.execute('TRUNCATE dw_gold.dim_conditions CASCADE')
    _run(cursor, """
        INSERT INTO dw_gold.dim_conditions (code, description, condition_category)
        SELECT DISTINCT ON (code)
            code,
            description,
            -- Classify Chronic vs Acute based on SNOMED description keywords
            CASE
                WHEN description ILIKE ANY(ARRAY[
                    '%chronic%','%disorder%','%disease%','%syndrome%',
                    '%diabetes%','%hypertension%','%asthma%','%failure%',
                    '%obesity%','%depression%','%anxiety%','%cancer%',
                    '%leukemia%','%anemia%'
                ]) THEN 'Chronic'
                ELSE 'Acute'
            END AS condition_category
        FROM dw_silver.conditions
        WHERE code IS NOT NULL
        ORDER BY code, description
        ON CONFLICT (code) DO NOTHING
    """, 'dim_conditions')


def load_fact_encounters(cursor):
    cursor.execute('TRUNCATE dw_gold.fact_encounters')
    _run(cursor, """
        INSERT INTO dw_gold.fact_encounters (
            encounter_id, patient_key, payer_key, date_key,
            encounter_class, duration_minutes,
            total_claim_cost, patient_oop,
            condition_count, has_active_careplan, is_readmission
        )
        WITH readmission_flags AS (
            -- Mark inpatient/emergency encounters occurring within 30 days of the prior one
            SELECT
                id,
                CASE
                    WHEN encounter_class IN ('inpatient','emergency')
                     AND LAG(start_ts) OVER (PARTITION BY patient_id ORDER BY start_ts)
                            >= start_ts - INTERVAL '30 days'
                    THEN TRUE ELSE FALSE
                END AS is_readmission
            FROM dw_silver.encounters
        ),
        condition_counts AS (
            SELECT encounter_id, COUNT(*) AS condition_count
            FROM dw_silver.conditions
            GROUP BY encounter_id
        ),
        careplan_flags AS (
            -- Patient had at least one active care plan at the time of the encounter
            SELECT DISTINCT e.id AS encounter_id
            FROM dw_silver.encounters e
            JOIN dw_silver.careplans cp
                ON cp.patient_id = e.patient_id
               AND cp.start_date <= e.start_ts::DATE
               AND cp.is_active = TRUE
        )
        SELECT
            e.id                                                AS encounter_id,
            dp.patient_key,
            dpy.payer_key,
            TO_CHAR(e.start_ts, 'YYYYMMDD')::INT               AS date_key,
            e.encounter_class,
            e.duration_minutes,
            e.total_claim_cost,
            e.patient_oop,
            COALESCE(cc.condition_count, 0)                     AS condition_count,
            (cpf.encounter_id IS NOT NULL)                      AS has_active_careplan,
            COALESCE(rf.is_readmission, FALSE)                  AS is_readmission
        FROM dw_silver.encounters e
        LEFT JOIN dw_gold.dim_patients dp  ON dp.patient_id = e.patient_id
        LEFT JOIN dw_gold.dim_payers   dpy ON dpy.payer_id  = e.payer_id
        LEFT JOIN readmission_flags    rf  ON rf.id          = e.id
        LEFT JOIN condition_counts     cc  ON cc.encounter_id = e.id
        LEFT JOIN careplan_flags       cpf ON cpf.encounter_id = e.id
        WHERE e.start_ts IS NOT NULL
    """, 'fact_encounters')


# -----------------------------------------------------------------------

STEPS = {
    'load_dim_date':       load_dim_date,
    'load_dim_patients':   load_dim_patients,
    'load_dim_payers':     load_dim_payers,
    'load_dim_conditions': load_dim_conditions,
    'load_fact_encounters': load_fact_encounters,
}


def main(step: str):
    if step not in STEPS:
        raise ValueError(f'Unknown step "{step}". Available: {list(STEPS)}')
    print(f'[Gold Q2 Care Quality] running step: {step}')
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()
    try:
        STEPS[step](cur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    print(f'[Gold Q2 Care Quality] done: {step}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Gold Q2 Care Quality loader')
    parser.add_argument('--step', required=True, choices=list(STEPS))
    args = parser.parse_args()
    main(args.step)
