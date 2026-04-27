"""
Run once at project start to create all schemas and tables.
Order matters: hospital_op and metadata first, then bronze/silver/gold.
"""
import os
import psycopg2

DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'postgres'),
    'port':     int(os.getenv('DB_PORT', 5432)),
    'dbname':   os.getenv('DB_NAME', 'airflow'),
    'user':     os.getenv('DB_USER', 'airflow'),
    'password': os.getenv('DB_PASSWORD', 'airflow'),
}

SQL_DIR = os.getenv('SQL_DIR', '/opt/airflow/sql')

SCHEMAS_IN_ORDER = [
    ('hospital_op', f'{SQL_DIR}/hospital_op/schema.sql'),
    ('metadata',    f'{SQL_DIR}/metadata/schema.sql'),
    ('bronze',      f'{SQL_DIR}/bronze/schema.sql'),
    ('silver',      f'{SQL_DIR}/silver/schema.sql'),
    ('gold',        f'{SQL_DIR}/gold/schema.sql'),
]


def already_initialised(cursor) -> bool:
    cursor.execute("""
        SELECT COUNT(*) FROM information_schema.schemata
        WHERE schema_name IN ('dw_bronze', 'dw_silver', 'dw_gold', 'metadata')
    """)
    return cursor.fetchone()[0] == 4


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()
    if already_initialised(cur):
        print('Schemas already present — skipping init_db.')
        cur.close()
        conn.close()
        return
    try:
        for name, path in SCHEMAS_IN_ORDER:
            print(f'  creating {name} schema...', end=' ', flush=True)
            with open(path) as f:
                cur.execute(f.read())
            conn.commit()
            print('done')
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    print('Database initialised.')


if __name__ == '__main__':
    main()
