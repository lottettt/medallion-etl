import os
import psycopg2

DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'postgres'),
    'port':     int(os.getenv('DB_PORT', 5432)),
    'dbname':   os.getenv('DB_NAME', 'airflow'),
    'user':     os.getenv('DB_USER', 'airflow'),
    'password': os.getenv('DB_PASSWORD', 'airflow'),
}

DATASET_DIR = os.getenv('DATASET_DIR', '/opt/airflow/dataset')
SCHEMA_FILE = os.getenv('SCHEMA_FILE', '/opt/airflow/sql/hospital_op/schema.sql')

# Order respects FK dependencies
LOAD_ORDER = [
    'organizations',
    'payers',
    'patients',
    'providers',
    'encounters',
    'conditions',
    'medications',
    'procedures',
    'observations',
    'allergies',
    'immunizations',
    'careplans',
    'imaging_studies',
    'devices',
    'supplies',
    'payer_transitions',
    'claims',
    'claims_transactions',
]


def create_schema(cursor):
    with open(SCHEMA_FILE, 'r') as f:
        cursor.execute(f.read())
    print('Schema created.')


def get_csv_columns(filepath):
    with open(filepath, 'r') as f:
        header = f.readline().strip()
    return header.split(',')


def load_table(cursor, table, filepath):
    cols = get_csv_columns(filepath)
    col_list = ', '.join(cols)
    sql = f"COPY hospital_op.{table} ({col_list}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')"
    with open(filepath, 'r') as f:
        cursor.copy_expert(sql, f)


def already_loaded(cursor) -> bool:
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'hospital_op' AND table_name = 'patients'
        )
    """)
    if not cursor.fetchone()[0]:
        return False
    cursor.execute('SELECT EXISTS (SELECT 1 FROM hospital_op.patients LIMIT 1)')
    return cursor.fetchone()[0]


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    if already_loaded(cur):
        print('hospital_op already populated — skipping ingest.')
        cur.close()
        conn.close()
        return

    print('Creating schema...')
    create_schema(cur)
    conn.commit()

    for table in LOAD_ORDER:
        filepath = os.path.join(DATASET_DIR, f'{table}.csv')
        if not os.path.exists(filepath):
            print(f'  SKIP {table} — file not found')
            continue
        print(f'  Loading {table}...', end=' ', flush=True)
        load_table(cur, table, filepath)
        conn.commit()
        print('done')

    cur.close()
    conn.close()
    print('Ingestion complete.')


if __name__ == '__main__':
    main()
