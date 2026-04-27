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

    # --- DQ metrics ---
    t = 'dw_silver.encounters'
    dq.log_metrics(cursor, run_id, 'silver_encounters', [
        # table-level
        (None,             'completeness', dq.pct_pass_status(cursor, t),                         95.0),
        # column-level
        ('id',             'completeness', dq.pct_not_null(cursor, t, 'id'),                      100.0),
        ('patient_id',     'completeness', dq.pct_not_null(cursor, t, 'patient_id'),              100.0),
        ('encounter_class','validity',     dq.pct_in_set(cursor, t, 'encounter_class',
                                              ['wellness','ambulatory','outpatient','urgentcare',
                                               'emergency','inpatient','home','snf','hospice','virtual']), 95.0),
        ('total_claim_cost','accuracy',   dq.pct_gte_zero(cursor, t, 'total_claim_cost'),         99.0),
    ])


def load_payers(cursor, run_id: str):
    cursor.execute('TRUNCATE dw_silver.payers')
    _run(cursor, """
        INSERT INTO dw_silver.payers (
            id, name, ownership, amount_covered, amount_uncovered, _silver_processed_at
        )
        WITH latest AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) AS rn
            FROM dw_bronze.payers
        )
        SELECT id, TRIM(name), UPPER(TRIM(ownership)), amount_covered, amount_uncovered, NOW()
        FROM latest
        WHERE rn = 1 AND id IS NOT NULL
    """, 'payers')

    t = 'dw_silver.payers'
    dq.log_metrics(cursor, run_id, 'silver_payers', [
        ('id',        'completeness', dq.pct_not_null(cursor, t, 'id'),        100.0),
        ('ownership', 'validity',     dq.pct_in_set(cursor, t, 'ownership',
                                          ['GOVERNMENT', 'PRIVATE']),           90.0),
    ])


def load_claims(cursor, run_id: str):
    cursor.execute('TRUNCATE dw_silver.claims')
    _run(cursor, """
        INSERT INTO dw_silver.claims (
            id, patient_id, payer_id, service_date,
            claim_status, total_outstanding, last_billed_date,
            _dq_status, _dq_notes, _silver_processed_at
        )
        WITH latest AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) AS rn
            FROM dw_bronze.claims
        ),
        cleaned AS (
            SELECT
                id,
                patientid                                                   AS patient_id,
                primarypatientinsuranceid                                   AS payer_id,
                servicedate                                                 AS service_date,
                CASE
                    WHEN UPPER(status1) = 'CLOSED'
                     AND UPPER(status2) = 'CLOSED'
                     AND UPPER(statusp) = 'CLOSED' THEN 'CLOSED'
                    ELSE 'OPEN'
                END                                                         AS claim_status,
                COALESCE(outstanding1,0)+COALESCE(outstanding2,0)+COALESCE(outstandingp,0)
                                                                            AS total_outstanding,
                lastbilleddate1                                             AS last_billed_date,
                CASE WHEN patientid IS NULL THEN 'WARN' ELSE 'PASS' END    AS _dq_status,
                CASE WHEN patientid IS NULL THEN 'missing patient_id' ELSE NULL END AS _dq_notes
            FROM latest
            WHERE rn = 1 AND id IS NOT NULL
        )
        SELECT id, patient_id, payer_id, service_date,
               claim_status, total_outstanding, last_billed_date,
               _dq_status, _dq_notes, NOW()
        FROM cleaned
    """, 'claims')

    t = 'dw_silver.claims'
    dq.log_metrics(cursor, run_id, 'silver_claims', [
        (None,              'completeness', dq.pct_pass_status(cursor, t),                    95.0),
        ('id',              'completeness', dq.pct_not_null(cursor, t, 'id'),                 100.0),
        ('patient_id',      'completeness', dq.pct_not_null(cursor, t, 'patient_id'),         95.0),
        ('claim_status',    'validity',     dq.pct_in_set(cursor, t, 'claim_status',
                                                ['OPEN', 'CLOSED']),                          100.0),
        ('total_outstanding','accuracy',   dq.pct_gte_zero(cursor, t, 'total_outstanding'),   99.0),
    ])


def load_claims_transactions(cursor, run_id: str):
    cursor.execute('TRUNCATE dw_silver.claims_transactions')
    _run(cursor, """
        INSERT INTO dw_silver.claims_transactions (
            id, claim_id, patient_id, transaction_type,
            amount, payment_method, from_date, to_date, days_to_payment,
            payments, adjustments, outstanding,
            _dq_status, _dq_notes, _silver_processed_at
        )
        WITH latest AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) AS rn
            FROM dw_bronze.claims_transactions
        ),
        cleaned AS (
            SELECT
                id,
                claimid                AS claim_id,
                patientid              AS patient_id,
                UPPER(TRIM(type))      AS transaction_type,
                amount,
                UPPER(TRIM(method))    AS payment_method,
                fromdate               AS from_date,
                todate                 AS to_date,
                CASE
                    WHEN UPPER(TRIM(type)) = 'PAYMENT'
                     AND fromdate IS NOT NULL AND todate IS NOT NULL
                    THEN DATE_PART('day', todate - fromdate)::INTEGER
                END                    AS days_to_payment,
                payments,
                adjustments,
                outstanding,
                CASE
                    WHEN claimid IS NULL THEN 'WARN'
                    WHEN amount < 0      THEN 'WARN'
                    ELSE 'PASS'
                END AS _dq_status,
                CASE
                    WHEN claimid IS NULL THEN 'missing claim_id'
                    WHEN amount < 0      THEN 'negative amount'
                    ELSE NULL
                END AS _dq_notes
            FROM latest
            WHERE rn = 1 AND id IS NOT NULL
        )
        SELECT id, claim_id, patient_id, transaction_type,
               amount, payment_method, from_date, to_date, days_to_payment,
               payments, adjustments, outstanding,
               _dq_status, _dq_notes, NOW()
        FROM cleaned
    """, 'claims_transactions')

    t = 'dw_silver.claims_transactions'
    dq.log_metrics(cursor, run_id, 'silver_claims_transactions', [
        (None,              'completeness', dq.pct_pass_status(cursor, t),                    95.0),
        ('claim_id',        'completeness', dq.pct_not_null(cursor, t, 'claim_id'),           99.0),
        ('transaction_type','validity',     dq.pct_in_set(cursor, t, 'transaction_type',
                                                ['CHARGE','PAYMENT','TRANSFERIN','TRANSFEROUT']), 99.0),
        ('amount',          'accuracy',     dq.pct_gte_zero(cursor, t, 'amount'),             95.0),
    ])


# -----------------------------------------------------------------------

STEPS = {
    'load_encounters':          load_encounters,
    'load_payers':              load_payers,
    'load_claims':              load_claims,
    'load_claims_transactions': load_claims_transactions,
}


def main(step: str):
    run_id = dq.generate_run_id('silver_q1')
    print(f'[Silver Q1 Revenue Cycle] step={step}  run_id={run_id}')
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
    print(f'[Silver Q1 Revenue Cycle] done: {step}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Silver Q1 Revenue Cycle loader')
    parser.add_argument('--step', required=True, choices=list(STEPS))
    args = parser.parse_args()
    main(args.step)
