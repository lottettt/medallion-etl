-- =============================================================
-- dw_gold: Star Schema — Analytics-Ready (BI / Tableau)
-- Surrogate keys (SERIAL) replace source UUIDs for join performance.
-- All measures are pre-calculated; no joins needed from BI tools.
-- =============================================================

-- -----------------------------------------------------------
-- Shared Dimensions
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS dw_gold.dim_date (
    date_key        INT         PRIMARY KEY,    -- YYYYMMDD integer (e.g. 20260427)
    full_date       DATE        NOT NULL,
    day_of_week     TEXT,                       -- Monday … Sunday
    day_of_month    INT,
    week_of_year    INT,
    month           INT,
    month_name      TEXT,                       -- January … December
    quarter         INT,                        -- 1-4
    year            INT,
    is_weekend      BOOLEAN
);

CREATE TABLE IF NOT EXISTS dw_gold.dim_patients (
    patient_key     SERIAL      PRIMARY KEY,
    patient_id      UUID        UNIQUE NOT NULL, -- natural key from silver
    age_group       TEXT,                        -- 0-17 / 18-34 / 35-50 / 51-65 / 65+
    gender          TEXT,                        -- Male / Female / Unknown
    race            TEXT,
    ethnicity       TEXT,
    is_deceased     BOOLEAN
);

CREATE TABLE IF NOT EXISTS dw_gold.dim_payers (
    payer_key       SERIAL      PRIMARY KEY,
    payer_id        UUID        UNIQUE NOT NULL,
    name            TEXT,
    ownership       TEXT                         -- GOVERNMENT / PRIVATE
);

-- -----------------------------------------------------------
-- Q2-specific Dimension
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS dw_gold.dim_conditions (
    condition_key       SERIAL  PRIMARY KEY,
    code                TEXT    UNIQUE NOT NULL,
    description         TEXT,
    condition_category  TEXT    -- Chronic / Acute (derived from description keywords)
);

-- -----------------------------------------------------------
-- Q1: Revenue Cycle — Fact Table
-- Grain: one row per claim
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS dw_gold.fact_claims (
    claim_key           SERIAL          PRIMARY KEY,
    claim_id            UUID,
    patient_key         INT             REFERENCES dw_gold.dim_patients(patient_key),
    payer_key           INT             REFERENCES dw_gold.dim_payers(payer_key),
    service_date_key    INT             REFERENCES dw_gold.dim_date(date_key),
    encounter_class     TEXT,
    claim_status        TEXT,           -- OPEN / CLOSED
    -- pre-calculated measures
    total_claim_cost    NUMERIC(15,2),
    payer_coverage      NUMERIC(15,2),
    patient_oop         NUMERIC(15,2),
    total_outstanding   NUMERIC(15,2),
    total_payments      NUMERIC(15,2),
    total_adjustments   NUMERIC(15,2),
    avg_days_to_payment NUMERIC(10,2)   -- avg across PAYMENT transactions for this claim
);

-- -----------------------------------------------------------
-- Q2: Care Quality — Fact Table
-- Grain: one row per encounter
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS dw_gold.fact_encounters (
    encounter_key       SERIAL      PRIMARY KEY,
    encounter_id        UUID,
    patient_key         INT         REFERENCES dw_gold.dim_patients(patient_key),
    payer_key           INT         REFERENCES dw_gold.dim_payers(payer_key),
    date_key            INT         REFERENCES dw_gold.dim_date(date_key),
    encounter_class     TEXT,
    -- pre-calculated measures
    duration_minutes    INT,
    total_claim_cost    NUMERIC(15,2),
    patient_oop         NUMERIC(15,2),
    condition_count     INT,            -- # of conditions linked to this encounter
    has_active_careplan BOOLEAN,        -- patient had an active care plan at encounter time
    is_readmission      BOOLEAN         -- inpatient/emergency within 30 days of prior encounter
);
