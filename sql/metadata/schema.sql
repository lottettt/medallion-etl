-- =============================================================
-- metadata: Data Quality audit log
-- =============================================================
CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.dq_log_results (
    id            SERIAL      PRIMARY KEY,
    run_id        TEXT        NOT NULL,       -- Airflow run_id or script timestamp
    table_name    TEXT        NOT NULL,       -- e.g. 'silver_patients'
    column_name   TEXT,                       -- NULL = table-level metric
    metric_type   TEXT        NOT NULL,       -- completeness / validity / accuracy / uniqueness
    metric_value  FLOAT       NOT NULL,       -- e.g. 98.5  (percentage 0-100)
    threshold     FLOAT       NOT NULL,       -- minimum acceptable value
    status        TEXT        NOT NULL,       -- PASSED / FAILED
    checked_at    TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_table_time ON metadata.dq_log_results (table_name, checked_at);
CREATE INDEX IF NOT EXISTS idx_dq_run       ON metadata.dq_log_results (run_id);
