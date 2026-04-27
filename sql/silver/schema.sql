-- =============================================================
-- dw_silver: Cleaned, standardized, validated, de-identified
-- Full refresh on each run (audit trail stays in dw_bronze)
-- _dq_status: PASS | WARN | FAIL
-- =============================================================

-- -----------------------------------------------------------
-- Q1 + Q2 shared
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw_silver.encounters (
    id                  UUID,
    patient_id          UUID,
    payer_id            UUID,
    encounter_class     TEXT,           -- standardized lowercase
    start_ts            TIMESTAMP,
    stop_ts             TIMESTAMP,
    duration_minutes    INTEGER,        -- derived: stop - start
    base_encounter_cost NUMERIC(15,2),
    total_claim_cost    NUMERIC(15,2),
    payer_coverage      NUMERIC(15,2),
    patient_oop         NUMERIC(15,2),  -- derived: total_claim_cost - payer_coverage
    _dq_status          TEXT,
    _dq_notes           TEXT,
    _silver_processed_at TIMESTAMP DEFAULT NOW()
);

-- -----------------------------------------------------------
-- Q1: Revenue Cycle
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw_silver.payers (
    id               UUID,
    name             TEXT,
    ownership        TEXT,             -- standardized uppercase
    amount_covered   NUMERIC(15,2),
    amount_uncovered NUMERIC(15,2),
    _silver_processed_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dw_silver.claims (
    id                UUID,
    patient_id        UUID,
    payer_id          UUID,
    service_date      TIMESTAMP,
    claim_status      TEXT,            -- derived: CLOSED if all statuses CLOSED, else OPEN
    total_outstanding NUMERIC(15,2),   -- derived: outstanding1 + outstanding2 + outstandingp
    last_billed_date  TIMESTAMP,
    _dq_status        TEXT,
    _dq_notes         TEXT,
    _silver_processed_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dw_silver.claims_transactions (
    id               UUID,
    claim_id         UUID,
    patient_id       UUID,
    transaction_type TEXT,             -- standardized uppercase: CHARGE/PAYMENT/TRANSFERIN/TRANSFEROUT
    amount           NUMERIC(15,2),
    payment_method   TEXT,
    from_date        TIMESTAMP,
    to_date          TIMESTAMP,
    days_to_payment  INTEGER,          -- derived: only for PAYMENT rows
    payments         NUMERIC(15,2),
    adjustments      NUMERIC(15,2),
    outstanding      NUMERIC(15,2),
    _dq_status       TEXT,
    _dq_notes        TEXT,
    _silver_processed_at TIMESTAMP DEFAULT NOW()
);

-- -----------------------------------------------------------
-- Q2: Care Quality
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw_silver.patients (
    id          UUID,                  -- original UUID kept (already non-PII)
    birth_year  INTEGER,               -- de-identified: full birthdate removed
    age_group   TEXT,                  -- derived: 0-17 / 18-34 / 35-50 / 51-65 / 65+
    gender      TEXT,                  -- standardized: Male / Female / Unknown
    race        TEXT,
    ethnicity   TEXT,
    is_deceased BOOLEAN,               -- derived: deathdate IS NOT NULL
    _dq_status  TEXT,
    _dq_notes   TEXT,
    _silver_processed_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dw_silver.conditions (
    patient_id   UUID,
    encounter_id UUID,
    code         TEXT,
    description  TEXT,
    start_date   DATE,
    end_date     DATE,
    is_active    BOOLEAN,              -- derived: end_date IS NULL
    _dq_status   TEXT,
    _dq_notes    TEXT,
    _silver_processed_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dw_silver.careplans (
    id                 UUID,
    patient_id         UUID,
    encounter_id       UUID,
    code               TEXT,
    description        TEXT,
    start_date         DATE,
    end_date           DATE,
    is_active          BOOLEAN,        -- derived: end_date IS NULL
    reason_code        TEXT,
    reason_description TEXT,
    _dq_status         TEXT,
    _dq_notes          TEXT,
    _silver_processed_at TIMESTAMP DEFAULT NOW()
);
