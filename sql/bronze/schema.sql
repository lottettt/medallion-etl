-- =============================================================
-- dw_bronze: Append-only raw layer
-- Columns selected to answer 2 business questions:
--   Q1: Revenue Cycle  — payers, encounters, claims, claims_transactions
--   Q2: Care Quality   — patients, encounters, conditions, careplans
-- Every table carries 3 lineage columns:
--   _ingested_at  TIMESTAMP  when the row was pulled in
--   _source_file  TEXT       which CSV file it came from
--   _row_hash     TEXT       MD5 of all source columns (change detection)
-- =============================================================

-- -----------------------------------------------------------
-- Q1 + Q2 shared: encounters
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw_bronze.encounters (
    -- source columns
    id                  UUID,
    patient             UUID,
    payer               UUID,
    encounterclass      TEXT,           -- wellness / ambulatory / emergency / inpatient …
    start               TIMESTAMP,
    stop                TIMESTAMP,
    base_encounter_cost NUMERIC(15,2),
    total_claim_cost    NUMERIC(15,2),
    payer_coverage      NUMERIC(15,2),
    -- lineage
    _ingested_at        TIMESTAMP   NOT NULL DEFAULT NOW(),
    _source_file        TEXT        NOT NULL,
    _row_hash           TEXT        NOT NULL
);

-- -----------------------------------------------------------
-- Q1: Revenue Cycle
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw_bronze.payers (
    -- source columns
    id                  UUID,
    name                TEXT,
    ownership           TEXT,           -- GOVERNMENT / PRIVATE …
    amount_covered      NUMERIC(15,2),
    amount_uncovered    NUMERIC(15,2),
    -- lineage
    _ingested_at        TIMESTAMP   NOT NULL DEFAULT NOW(),
    _source_file        TEXT        NOT NULL,
    _row_hash           TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS dw_bronze.claims (
    -- source columns
    id                          UUID,
    patientid                   UUID,
    primarypatientinsuranceid   UUID,   -- FK → payers.id
    servicedate                 TIMESTAMP,
    status1                     TEXT,   -- OPEN / CLOSED / PENDING
    status2                     TEXT,
    statusp                     TEXT,
    outstanding1                NUMERIC(15,2),
    outstanding2                NUMERIC(15,2),
    outstandingp                NUMERIC(15,2),
    lastbilleddate1             TIMESTAMP,
    -- lineage
    _ingested_at                TIMESTAMP   NOT NULL DEFAULT NOW(),
    _source_file                TEXT        NOT NULL,
    _row_hash                   TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS dw_bronze.claims_transactions (
    -- source columns
    id              UUID,
    claimid         UUID,               -- FK → claims.id
    patientid       UUID,
    type            TEXT,               -- CHARGE / PAYMENT / TRANSFERIN / TRANSFEROUT
    amount          NUMERIC(15,2),
    method          TEXT,               -- COPAY / ECHECK / CASH …
    fromdate        TIMESTAMP,
    todate          TIMESTAMP,
    payments        NUMERIC(15,2),
    adjustments     NUMERIC(15,2),
    transfers       NUMERIC(15,2),
    outstanding     NUMERIC(15,2),
    -- lineage
    _ingested_at    TIMESTAMP   NOT NULL DEFAULT NOW(),
    _source_file    TEXT        NOT NULL,
    _row_hash       TEXT        NOT NULL
);

-- -----------------------------------------------------------
-- Q2: Care Quality
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw_bronze.patients (
    -- source columns
    id          UUID,
    birthdate   DATE,
    deathdate   DATE,
    gender      TEXT,
    race        TEXT,
    ethnicity   TEXT,
    -- lineage
    _ingested_at    TIMESTAMP   NOT NULL DEFAULT NOW(),
    _source_file    TEXT        NOT NULL,
    _row_hash       TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS dw_bronze.conditions (
    -- source columns
    patient     UUID,
    encounter   UUID,
    code        TEXT,
    description TEXT,
    start       DATE,
    stop        DATE,               -- NULL = condition still active (chronic)
    -- lineage
    _ingested_at    TIMESTAMP   NOT NULL DEFAULT NOW(),
    _source_file    TEXT        NOT NULL,
    _row_hash       TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS dw_bronze.careplans (
    -- source columns
    id          UUID,
    patient     UUID,
    encounter   UUID,
    code        TEXT,
    description TEXT,
    start       DATE,
    stop        DATE,               -- NULL = care plan still active
    reasoncode  TEXT,
    reasondescription TEXT,
    -- lineage
    _ingested_at    TIMESTAMP   NOT NULL DEFAULT NOW(),
    _source_file    TEXT        NOT NULL,
    _row_hash       TEXT        NOT NULL
);
