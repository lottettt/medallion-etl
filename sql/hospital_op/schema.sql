-- =============================================================
-- Medallion ETL - Database Schema Setup
-- =============================================================

CREATE SCHEMA IF NOT EXISTS hospital_op;
CREATE SCHEMA IF NOT EXISTS dw_bronze;
CREATE SCHEMA IF NOT EXISTS dw_silver;
CREATE SCHEMA IF NOT EXISTS dw_gold;

-- =============================================================
-- hospital_op: Operational tables (raw CSV dump)
-- =============================================================

-- Reference / Lookup Tables (no FK dependencies)

CREATE TABLE IF NOT EXISTS hospital_op.organizations (
    id              UUID PRIMARY KEY,
    name            TEXT,
    address         TEXT,
    city            TEXT,
    state           TEXT,
    zip             TEXT,
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION,
    phone           TEXT,
    revenue         NUMERIC(15,2),
    utilization     INTEGER,
    location        TEXT
);

CREATE TABLE IF NOT EXISTS hospital_op.payers (
    id                      UUID PRIMARY KEY,
    name                    TEXT,
    ownership               TEXT,
    address                 TEXT,
    city                    TEXT,
    state_headquartered      TEXT,
    zip                     TEXT,
    phone                   TEXT,
    amount_covered          NUMERIC(15,2),
    amount_uncovered        NUMERIC(15,2),
    revenue                 NUMERIC(15,2),
    covered_encounters      INTEGER,
    uncovered_encounters    INTEGER,
    covered_medications     INTEGER,
    uncovered_medications   INTEGER,
    covered_procedures      INTEGER,
    uncovered_procedures    INTEGER,
    covered_immunizations   INTEGER,
    uncovered_immunizations INTEGER,
    unique_customers        INTEGER,
    qols_avg                DOUBLE PRECISION,
    member_months           INTEGER
);

CREATE TABLE IF NOT EXISTS hospital_op.patients (
    id                   UUID PRIMARY KEY,
    birthdate            DATE,
    deathdate            DATE,
    ssn                  TEXT,
    drivers              TEXT,
    passport             TEXT,
    prefix               TEXT,
    first                TEXT,
    last                 TEXT,
    suffix               TEXT,
    maiden               TEXT,
    marital              TEXT,
    race                 TEXT,
    ethnicity            TEXT,
    gender               TEXT,
    birthplace           TEXT,
    address              TEXT,
    city                 TEXT,
    state                TEXT,
    county               TEXT,
    fips                 TEXT,
    zip                  TEXT,
    lat                  DOUBLE PRECISION,
    lon                  DOUBLE PRECISION,
    healthcare_expenses  NUMERIC(15,2),
    healthcare_coverage  NUMERIC(15,2),
    income               INTEGER,
    location             TEXT
);

CREATE TABLE IF NOT EXISTS hospital_op.providers (
    id           UUID PRIMARY KEY,
    organization UUID REFERENCES hospital_op.organizations(id),
    name         TEXT,
    gender       TEXT,
    speciality   TEXT,
    address      TEXT,
    city         TEXT,
    state        TEXT,
    zip          TEXT,
    lat          DOUBLE PRECISION,
    lon          DOUBLE PRECISION,
    utilization  INTEGER,
    location     TEXT
);

-- Core Clinical Tables

CREATE TABLE IF NOT EXISTS hospital_op.encounters (
    id                  UUID PRIMARY KEY,
    start               TIMESTAMP,
    stop                TIMESTAMP,
    patient             UUID REFERENCES hospital_op.patients(id),
    organization        UUID REFERENCES hospital_op.organizations(id),
    provider            UUID REFERENCES hospital_op.providers(id),
    payer               UUID REFERENCES hospital_op.payers(id),
    encounterclass      TEXT,
    code                TEXT,
    description         TEXT,
    base_encounter_cost NUMERIC(15,2),
    total_claim_cost    NUMERIC(15,2),
    payer_coverage      NUMERIC(15,2),
    reasoncode          TEXT,
    reasondescription   TEXT
);

CREATE TABLE IF NOT EXISTS hospital_op.conditions (
    start        DATE,
    stop         DATE,
    patient      UUID REFERENCES hospital_op.patients(id),
    encounter    UUID REFERENCES hospital_op.encounters(id),
    code         TEXT,
    description  TEXT
);

CREATE TABLE IF NOT EXISTS hospital_op.medications (
    start             TIMESTAMP,
    stop              TIMESTAMP,
    patient           UUID REFERENCES hospital_op.patients(id),
    payer             UUID REFERENCES hospital_op.payers(id),
    encounter         UUID REFERENCES hospital_op.encounters(id),
    code              TEXT,
    description       TEXT,
    base_cost         NUMERIC(15,2),
    payer_coverage    NUMERIC(15,2),
    dispenses         INTEGER,
    totalcost         NUMERIC(15,2),
    reasoncode        TEXT,
    reasondescription TEXT
);

CREATE TABLE IF NOT EXISTS hospital_op.procedures (
    start             TIMESTAMP,
    stop              TIMESTAMP,
    patient           UUID REFERENCES hospital_op.patients(id),
    encounter         UUID REFERENCES hospital_op.encounters(id),
    code              TEXT,
    description       TEXT,
    base_cost         NUMERIC(15,2),
    reasoncode        TEXT,
    reasondescription TEXT
);

CREATE TABLE IF NOT EXISTS hospital_op.observations (
    date        TIMESTAMP,
    patient     UUID REFERENCES hospital_op.patients(id),
    encounter   UUID REFERENCES hospital_op.encounters(id),
    category    TEXT,
    code        TEXT,
    description TEXT,
    value       TEXT,
    units       TEXT,
    type        TEXT
);

CREATE TABLE IF NOT EXISTS hospital_op.allergies (
    start        DATE,
    stop         DATE,
    patient      UUID REFERENCES hospital_op.patients(id),
    encounter    UUID REFERENCES hospital_op.encounters(id),
    code         TEXT,
    system       TEXT,
    description  TEXT,
    type         TEXT,
    category     TEXT,
    reaction1    TEXT,
    description1 TEXT,
    severity1    TEXT,
    reaction2    TEXT,
    description2 TEXT,
    severity2    TEXT
);

CREATE TABLE IF NOT EXISTS hospital_op.immunizations (
    date        TIMESTAMP,
    patient     UUID REFERENCES hospital_op.patients(id),
    encounter   UUID REFERENCES hospital_op.encounters(id),
    code        TEXT,
    description TEXT,
    base_cost   NUMERIC(15,2)
);

CREATE TABLE IF NOT EXISTS hospital_op.careplans (
    id                UUID PRIMARY KEY,
    start             DATE,
    stop              DATE,
    patient           UUID REFERENCES hospital_op.patients(id),
    encounter         UUID REFERENCES hospital_op.encounters(id),
    code              TEXT,
    description       TEXT,
    reasoncode        TEXT,
    reasondescription TEXT
);

CREATE TABLE IF NOT EXISTS hospital_op.imaging_studies (
    id                   UUID PRIMARY KEY,
    date                 TIMESTAMP,
    patient              UUID REFERENCES hospital_op.patients(id),
    encounter            UUID REFERENCES hospital_op.encounters(id),
    series_uid           TEXT,
    bodysite_code        TEXT,
    bodysite_description TEXT,
    modality_code        TEXT,
    modality_description TEXT,
    instance_uid         TEXT,
    sop_code             TEXT,
    sop_description      TEXT,
    procedure_code       TEXT
);

CREATE TABLE IF NOT EXISTS hospital_op.devices (
    start       TIMESTAMP,
    stop        TIMESTAMP,
    patient     UUID REFERENCES hospital_op.patients(id),
    encounter   UUID REFERENCES hospital_op.encounters(id),
    code        TEXT,
    description TEXT,
    udi         TEXT
);

CREATE TABLE IF NOT EXISTS hospital_op.supplies (
    date        DATE,
    patient     UUID REFERENCES hospital_op.patients(id),
    encounter   UUID REFERENCES hospital_op.encounters(id),
    code        TEXT,
    description TEXT,
    quantity    INTEGER
);

-- Insurance / Billing Tables

CREATE TABLE IF NOT EXISTS hospital_op.payer_transitions (
    patient         UUID REFERENCES hospital_op.patients(id),
    memberid        UUID,
    start_date      TIMESTAMP,
    end_date        TIMESTAMP,
    payer           UUID REFERENCES hospital_op.payers(id),
    secondary_payer UUID REFERENCES hospital_op.payers(id),
    plan_ownership  TEXT,
    owner_name      TEXT
);

CREATE TABLE IF NOT EXISTS hospital_op.claims (
    id                          UUID PRIMARY KEY,
    patientid                   UUID REFERENCES hospital_op.patients(id),
    providerid                  UUID REFERENCES hospital_op.providers(id),
    primarypatientinsuranceid   UUID REFERENCES hospital_op.payers(id),
    secondarypatientinsuranceid UUID,
    departmentid                INTEGER,
    patientdepartmentid         INTEGER,
    diagnosis1                  TEXT,
    diagnosis2                  TEXT,
    diagnosis3                  TEXT,
    diagnosis4                  TEXT,
    diagnosis5                  TEXT,
    diagnosis6                  TEXT,
    diagnosis7                  TEXT,
    diagnosis8                  TEXT,
    referringproviderid         UUID,
    appointmentid               UUID REFERENCES hospital_op.encounters(id),
    currentillnessdate          TIMESTAMP,
    servicedate                 TIMESTAMP,
    supervisingproviderid       UUID REFERENCES hospital_op.providers(id),
    status1                     TEXT,
    status2                     TEXT,
    statusp                     TEXT,
    outstanding1                NUMERIC(15,2),
    outstanding2                NUMERIC(15,2),
    outstandingp                NUMERIC(15,2),
    lastbilleddate1             TIMESTAMP,
    lastbilleddate2             TIMESTAMP,
    lastbilleddatep             TIMESTAMP,
    healthcareclaimtypeid1      INTEGER,
    healthcareclaimtypeid2      INTEGER
);

CREATE TABLE IF NOT EXISTS hospital_op.claims_transactions (
    id                    UUID PRIMARY KEY,
    claimid               UUID REFERENCES hospital_op.claims(id),
    chargeid              INTEGER,
    patientid             UUID REFERENCES hospital_op.patients(id),
    type                  TEXT,
    amount                NUMERIC(15,2),
    method                TEXT,
    fromdate              TIMESTAMP,
    todate                TIMESTAMP,
    placeofservice        UUID REFERENCES hospital_op.organizations(id),
    procedurecode         TEXT,
    modifier1             TEXT,
    modifier2             TEXT,
    diagnosisref1         INTEGER,
    diagnosisref2         INTEGER,
    diagnosisref3         INTEGER,
    diagnosisref4         INTEGER,
    units                 INTEGER,
    departmentid          INTEGER,
    notes                 TEXT,
    unitamount            NUMERIC(15,2),
    transferoutid         UUID,
    transfertype          TEXT,
    payments              NUMERIC(15,2),
    adjustments           NUMERIC(15,2),
    transfers             NUMERIC(15,2),
    outstanding           NUMERIC(15,2),
    appointmentid         UUID REFERENCES hospital_op.encounters(id),
    linenote              TEXT,
    patientinsuranceid    UUID,
    feescheduleid         INTEGER,
    providerid            UUID REFERENCES hospital_op.providers(id),
    supervisingproviderid UUID REFERENCES hospital_op.providers(id)
);
