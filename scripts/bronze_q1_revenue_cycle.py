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

def _run_bronze_insert(cursor, table: str, columns: list[str], source_file: str):
    col_list  = ', '.join(columns)
    cast_list = ', '.join(f'{c}::TEXT' for c in columns)
    sql = f"""
    WITH staged AS (
        SELECT
            {col_list},
            NOW()                             AS _ingested_at,
            '{source_file}'                   AS _source_file,
            MD5(CONCAT_WS('|', {cast_list})) AS _row_hash
        FROM hospital_op.{table}
    )
    INSERT INTO dw_bronze.{table} (
        {col_list}, _ingested_at, _source_file, _row_hash
    )
    SELECT {col_list}, _ingested_at, _source_file, _row_hash
    FROM staged s
    WHERE NOT EXISTS (
        SELECT 1 FROM dw_bronze.{table} b WHERE b._row_hash = s._row_hash
    );
    """
    cursor.execute(sql)
    print(f'  loaded: {table} ({cursor.rowcount} new rows)')


def load_encounters(cursor):
    _run_bronze_insert(cursor, 'encounters', [
        'id', 'patient', 'payer', 'encounterclass',
        'start', 'stop',
        'base_encounter_cost', 'total_claim_cost', 'payer_coverage',
    ], 'encounters.csv')


def load_payers(cursor):
    _run_bronze_insert(cursor, 'payers', [
        'id', 'name', 'ownership',
        'amount_covered', 'amount_uncovered',
    ], 'payers.csv')


def load_claims(cursor):
    _run_bronze_insert(cursor, 'claims', [
        'id', 'patientid', 'primarypatientinsuranceid',
        'servicedate',
        'status1', 'status2', 'statusp',
        'outstanding1', 'outstanding2', 'outstandingp',
        'lastbilleddate1',
    ], 'claims.csv')


def load_claims_transactions(cursor):
    _run_bronze_insert(cursor, 'claims_transactions', [
        'id', 'claimid', 'patientid',
        'type', 'amount', 'method',
        'fromdate', 'todate',
        'payments', 'adjustments', 'transfers', 'outstanding',
    ], 'claims_transactions.csv')


STEPS = {
    'load_encounters':         load_encounters,
    'load_payers':             load_payers,
    'load_claims':             load_claims,
    'load_claims_transactions': load_claims_transactions,
}


def main(step: str):
    if step not in STEPS:
        raise ValueError(f'Unknown step "{step}". Available: {list(STEPS)}')
    print(f'[Q1 Revenue Cycle] running step: {step}')
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
    print(f'[Q1 Revenue Cycle] done: {step}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Q1 Revenue Cycle bronze loader')
    parser.add_argument('--step', required=True, choices=list(STEPS),
                        help='Which step to run')
    args = parser.parse_args()
    main(args.step)
