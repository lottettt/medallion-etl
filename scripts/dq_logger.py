"""
Shared DQ logging utility.
Used by all silver ETL scripts to write metrics into metadata.dq_log_results.
"""
import os
import uuid
from datetime import datetime


def generate_run_id(prefix: str) -> str:
    """Use Airflow run_id env var if available, else generate a timestamped UUID."""
    airflow_run_id = os.getenv('AIRFLOW_RUN_ID')
    if airflow_run_id:
        return airflow_run_id
    ts = datetime.utcnow().strftime('%Y%m%dT%H%M%S')
    return f'{prefix}__{ts}__{uuid.uuid4().hex[:8]}'


def log_metrics(cursor, run_id: str, table_name: str, metrics: list[tuple]):
    """
    Insert DQ metric rows into metadata.dq_log_results.

    metrics: list of (column_name_or_None, metric_type, metric_value, threshold)
      column_name_or_None — None means table-level metric
      metric_type         — completeness | validity | accuracy | uniqueness
      metric_value        — float 0–100 (percentage)
      threshold           — minimum acceptable percentage
    """
    sql = """
        INSERT INTO metadata.dq_log_results
            (run_id, table_name, column_name, metric_type, metric_value, threshold, status, checked_at)
        VALUES (%s, %s, %s, %s, %s, %s,
            CASE WHEN %s >= %s THEN 'PASSED' ELSE 'FAILED' END,
            NOW())
    """
    for col, mtype, val, threshold in metrics:
        cursor.execute(sql, (run_id, table_name, col, mtype, val, threshold, val, threshold))

    passed = sum(1 for _, _, v, t in metrics if v >= t)
    print(f'  DQ logged [{table_name}]: {passed}/{len(metrics)} checks passed')


def pct_not_null(cursor, table: str, column: str) -> float:
    cursor.execute(f"""
        SELECT 100.0 * COUNT({column}) / NULLIF(COUNT(*), 0)
        FROM {table}
    """)
    return cursor.fetchone()[0] or 0.0


def pct_in_set(cursor, table: str, column: str, values: list[str]) -> float:
    placeholders = ', '.join(['%s'] * len(values))
    cursor.execute(f"""
        SELECT 100.0 * COUNT(*) FILTER (WHERE {column} IN ({placeholders}))
               / NULLIF(COUNT(*), 0)
        FROM {table}
    """, values)
    return cursor.fetchone()[0] or 0.0


def pct_gte_zero(cursor, table: str, column: str) -> float:
    cursor.execute(f"""
        SELECT 100.0 * COUNT(*) FILTER (WHERE {column} >= 0)
               / NULLIF(COUNT(*) FILTER (WHERE {column} IS NOT NULL), 0)
        FROM {table}
    """)
    return cursor.fetchone()[0] or 0.0


def pct_pass_status(cursor, table: str) -> float:
    cursor.execute(f"""
        SELECT 100.0 * COUNT(*) FILTER (WHERE _dq_status = 'PASS')
               / NULLIF(COUNT(*), 0)
        FROM {table}
    """)
    return cursor.fetchone()[0] or 0.0
