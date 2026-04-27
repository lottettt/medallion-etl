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
                (SELECT MIN(service_date)::DATE FROM dw_silver.claims),
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


def load_fact_claims(cursor):
    cursor.execute('TRUNCATE dw_gold.fact_claims')
    _run(cursor, """
        INSERT INTO dw_gold.fact_claims (
            claim_id, patient_key, payer_key, service_date_key,
            encounter_class, claim_status,
            total_claim_cost, payer_coverage, patient_oop,
            total_outstanding, total_payments, total_adjustments, avg_days_to_payment
        )
        WITH txn_summary AS (
            -- Pre-aggregate transaction metrics per claim
            SELECT
                claim_id,
                SUM(payments)                                               AS total_payments,
                SUM(adjustments)                                            AS total_adjustments,
                AVG(days_to_payment) FILTER (WHERE transaction_type = 'PAYMENT'
                                              AND days_to_payment IS NOT NULL) AS avg_days_to_payment
            FROM dw_silver.claims_transactions
            GROUP BY claim_id
        ),
        -- Approximate join: link claim to encounter via patient + service date
        encounter_match AS (
            SELECT DISTINCT ON (c.id)
                c.id                                    AS claim_id,
                e.encounter_class
            FROM dw_silver.claims c
            LEFT JOIN dw_silver.encounters e
                ON e.patient_id = c.patient_id
               AND e.start_ts::DATE = c.service_date::DATE
            ORDER BY c.id, e.start_ts
        )
        SELECT
            c.id                                                        AS claim_id,
            dp.patient_key,
            dpy.payer_key,
            TO_CHAR(c.service_date, 'YYYYMMDD')::INT                   AS service_date_key,
            COALESCE(em.encounter_class, 'unknown')                     AS encounter_class,
            c.claim_status,
            e_agg.total_claim_cost,
            e_agg.payer_coverage,
            e_agg.patient_oop,
            c.total_outstanding,
            COALESCE(t.total_payments, 0)                               AS total_payments,
            COALESCE(t.total_adjustments, 0)                            AS total_adjustments,
            t.avg_days_to_payment
        FROM dw_silver.claims c
        -- Surrogate key lookups
        LEFT JOIN dw_gold.dim_patients dp  ON dp.patient_id  = c.patient_id
        LEFT JOIN dw_gold.dim_payers   dpy ON dpy.payer_id   = c.payer_id
        -- Encounter financial measures (via same approximate join)
        LEFT JOIN (
            SELECT DISTINCT ON (patient_id, start_ts::DATE)
                patient_id, start_ts::DATE AS enc_date,
                total_claim_cost, payer_coverage, patient_oop
            FROM dw_silver.encounters
            ORDER BY patient_id, start_ts::DATE, start_ts
        ) e_agg ON e_agg.patient_id = c.patient_id AND e_agg.enc_date = c.service_date::DATE
        LEFT JOIN encounter_match em  ON em.claim_id   = c.id
        LEFT JOIN txn_summary     t   ON t.claim_id    = c.id
        -- Only include claims with a valid date key
        WHERE c.service_date IS NOT NULL
    """, 'fact_claims')


# -----------------------------------------------------------------------

STEPS = {
    'load_dim_date':      load_dim_date,
    'load_dim_patients':  load_dim_patients,
    'load_dim_payers':    load_dim_payers,
    'load_fact_claims':   load_fact_claims,
}


def main(step: str):
    if step not in STEPS:
        raise ValueError(f'Unknown step "{step}". Available: {list(STEPS)}')
    print(f'[Gold Q1 Revenue Cycle] running step: {step}')
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
    print(f'[Gold Q1 Revenue Cycle] done: {step}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Gold Q1 Revenue Cycle loader')
    parser.add_argument('--step', required=True, choices=list(STEPS))
    args = parser.parse_args()
    main(args.step)
