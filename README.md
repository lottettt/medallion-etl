# Medallion ETL — Synthetic Medical Records

A **Bronze → Silver → Gold** data pipeline built with Apache Airflow and PostgreSQL, processing synthetic healthcare data to answer two clinical and operational business questions.

**Dataset source:** [Synthetic AL Medical Records — Kaggle](https://www.kaggle.com/datasets/mexwell/synthetic-al-medical-records)

---

## Business Questions

### Q1 — Revenue Cycle
> Which payers and encounter types drive the highest outstanding balances, and what is the average days-to-payment?

**Why it matters:** $16M is sitting uncollected. This tells finance which payer contracts to renegotiate, which encounter types to flag for faster billing, and whether government vs. private payers settle faster.

### Q2 — Care Quality
> Do patients with chronic conditions (hypertension, diabetes, stress) who have an active care plan have fewer emergency and inpatient revisits than those without one?

**Why it matters:** Emergency and inpatient are the most expensive encounter types. If care plans reduce revisits, clinical leadership can justify expanding care management programs — directly cutting cost and improving outcomes.

---

## Architecture

```
dataset/ (18 CSV files from Synthea)
    │
    ▼  scripts/ingest.py
hospital_op        ← raw operational tables (COPY FROM CSV)
    │
    ▼  bronze_*.py  (called by Airflow DAG)
dw_bronze          ← append-only, MD5 row-hash dedup, full lineage
    │
    ▼  silver_*.py  (called by Airflow DAG)
dw_silver          ← cleaned, standardised, PDPA de-identified, DQ-flagged
    │
    ▼  gold_*.py    (called by Airflow DAG)
dw_gold            ← star schema, surrogate keys, pre-calculated KPIs
```

| Layer | Strategy | Key features |
|---|---|---|
| Bronze | Append-only INSERT | `_ingested_at`, `_source_file`, `_row_hash` — never overwrites source data |
| Silver | TRUNCATE + full refresh | Dedup via `ROW_NUMBER()`, `_dq_status` (PASS/WARN), PDPA de-identification |
| Gold | TRUNCATE + full refresh | SERIAL surrogate keys, pre-calculated flags and KPIs, analytics-ready |

---

## Project structure

```
medallion-etl/
├── dags/
│   ├── q1_revenue_cycle.py          # Airflow DAG: full bronze→silver→gold for Q1
│   └── q2_care_quality.py           # Airflow DAG: full bronze→silver→gold for Q2
├── scripts/
│   ├── ingest.py                    # One-time: load CSVs into hospital_op
│   ├── init_db.py                   # One-time: create all DW schemas and tables
│   ├── dq_logger.py                 # Shared DQ metric logging utility
│   ├── bronze_q1_revenue_cycle.py
│   ├── bronze_q2_care_quality.py
│   ├── silver_q1_revenue_cycle.py
│   ├── silver_q2_care_quality.py
│   ├── gold_q1_revenue_cycle.py
│   ├── gold_q2_care_quality.py
│   └── verify.py
├── sql/
│   ├── hospital_op/schema.sql
│   ├── bronze/schema.sql
│   ├── silver/schema.sql
│   ├── gold/schema.sql
│   └── metadata/schema.sql
├── dataset/                         # Place Kaggle CSV files here
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## Setup

### Prerequisites
- Docker and Docker Compose
- The dataset CSV files from Kaggle placed in `dataset/`

### 1. Download the dataset

Go to [https://www.kaggle.com/datasets/mexwell/synthetic-al-medical-records](https://www.kaggle.com/datasets/mexwell/synthetic-al-medical-records), download and extract all CSV files into the `dataset/` folder:

```
dataset/
├── patients.csv
├── encounters.csv
├── claims.csv
├── claims_transactions.csv
├── conditions.csv
├── careplans.csv
├── payers.csv
└── ... (18 files total)
```

### 2. Start everything

```bash
docker-compose up -d
```

On **first run** this automatically:
1. Starts PostgreSQL and waits until it is healthy
2. Migrates the Airflow metadata database and creates the admin user (`admin` / `admin`)
3. Runs `ingest.py` — creates the `hospital_op` schema and bulk-loads all 18 CSVs
4. Runs `init_db.py` — creates `dw_bronze`, `dw_silver`, `dw_gold`, and `metadata` schemas
5. Starts the Airflow webserver and scheduler

On **subsequent runs**, `ingest.py` and `init_db.py` detect that data already exists and exit early — no duplicates, no errors.

### 3. Open Airflow

[http://localhost:8080](http://localhost:8080) — username: `admin`, password: `admin`

### 4. Run the pipelines

Trigger either DAG from the Airflow UI:

| DAG | Business question |
|---|---|
| `q1_revenue_cycle` | Revenue Cycle — payers, claims, outstanding balances |
| `q2_care_quality` | Care Quality — patients, conditions, care plans, readmissions |

Each DAG runs the full bronze → silver → gold pipeline for its question on a `@daily` schedule, or can be triggered manually.

### 5. Tear down

```bash
docker-compose down       # stop containers, keep the data volume
docker-compose down -v    # stop containers AND delete all data
```

---

## ETL Process Detail

### Phase 1 — Data Ingestion (`hospital_op`)

`scripts/ingest.py` runs once at startup. It creates the `hospital_op` schema and bulk-loads all CSVs using `COPY FROM STDIN` via psycopg2. Tables are loaded in FK-safe order:

```
organizations → payers → patients → providers → encounters
    → conditions, medications, procedures, observations,
      allergies, immunizations, careplans, imaging_studies,
      devices, supplies, payer_transitions, claims, claims_transactions
```

| Group | Tables |
|---|---|
| Reference / Lookup | `organizations`, `payers`, `patients`, `providers` |
| Clinical | `encounters`, `conditions`, `medications`, `procedures`, `observations`, `allergies`, `immunizations`, `careplans`, `imaging_studies`, `devices`, `supplies` |
| Insurance / Billing | `payer_transitions`, `claims`, `claims_transactions` |

---

### Phase 2 — Bronze Layer (`dw_bronze`)

**Design principle:** Bronze is append-only. Every row gets three lineage columns: `_ingested_at`, `_source_file`, `_row_hash` (MD5 of all source columns). Rows that already exist (same hash) are silently skipped — re-running is always safe.

Only columns needed to answer the two business questions are included:

| Table | Question |
|---|---|
| `encounters` | Q1 + Q2 (shared) |
| `payers`, `claims`, `claims_transactions` | Q1 Revenue Cycle |
| `patients`, `conditions`, `careplans` | Q2 Care Quality |

---

### Phase 3 — Silver Layer (`dw_silver`)

**Design principle:** Silver is a full refresh (TRUNCATE + INSERT) on every run. The append-only audit trail lives in Bronze. Every row carries `_dq_status` (PASS / WARN) and `_dq_notes` explaining why it was flagged.

#### Q1 — Revenue Cycle

**`encounters`**

| Transform | Rule | Reason |
|---|---|---|
| Deduplication | `ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC)` | Bronze is append-only; pick the latest version of each encounter |
| Drop | `WHERE id IS NOT NULL` | An encounter without an ID cannot be linked to any claim |
| Standardize | `LOWER(TRIM(encounterclass))` → `encounter_class` | Source mixes cases (`Wellness`, `WELLNESS`); lowercase is the standard |
| Derive | `duration_minutes = EXTRACT(EPOCH FROM (stop - start)) / 60` | Duration is needed to compare encounter cost efficiency |
| Derive | `patient_oop = total_claim_cost - payer_coverage` | Out-of-pocket cost is not stored directly; must be calculated |
| DQ WARN | `total_claim_cost < 0` | Negative cost indicates a billing error or reversal |
| DQ WARN | `stop < start` | Physically impossible; likely a data entry error |

**`payers`**

| Transform | Rule | Reason |
|---|---|---|
| Deduplication | `PARTITION BY id ORDER BY _ingested_at DESC` | Same payer may appear in multiple bronze loads |
| Drop | `WHERE id IS NOT NULL` | A payer without an ID cannot be joined to claims |
| Standardize | `UPPER(TRIM(ownership))` | Source has mixed casing (`Government`, `GOVERNMENT`) |
| Standardize | `TRIM(name)` | Remove leading/trailing whitespace from free-text names |

**`claims`**

| Transform | Rule | Reason |
|---|---|---|
| Deduplication | `PARTITION BY id ORDER BY _ingested_at DESC` | Claim status can change over time (OPEN → CLOSED); always use the latest |
| Drop | `WHERE id IS NOT NULL` | Claim cannot be tracked without an ID |
| Derive | `claim_status` — CLOSED only if status1 + status2 + statusp all CLOSED, else OPEN | Source stores billing status in 3 columns; merge into one for reporting |
| Derive | `total_outstanding = outstanding1 + outstanding2 + outstandingp` | Finance needs a single outstanding figure per claim |
| DQ WARN | `patientid IS NULL` | Claim cannot be attributed to a patient |

**`claims_transactions`**

| Transform | Rule | Reason |
|---|---|---|
| Deduplication | `PARTITION BY id ORDER BY _ingested_at DESC` | Transactions can be amended (adjustments, reversals) |
| Drop | `WHERE id IS NOT NULL` | Untraceable transaction |
| Standardize | `UPPER(TRIM(type))` → `transaction_type` | Normalize CHARGE / PAYMENT / TRANSFERIN / TRANSFEROUT |
| Standardize | `UPPER(TRIM(method))` → `payment_method` | Normalize COPAY / ECHECK / CASH etc. |
| Derive | `days_to_payment = DATE_PART('day', todate - fromdate)` for PAYMENT rows only | Measures how long it takes for a charge to be paid — core Q1 metric |
| DQ WARN | `claimid IS NULL` | Transaction cannot be linked to a claim |
| DQ WARN | `amount < 0` | Negative amount may be a reversal or error |

---

#### Q2 — Care Quality

**`patients`**

| Transform | Rule | Reason |
|---|---|---|
| Deduplication | `PARTITION BY id ORDER BY _ingested_at DESC` | Patient records may be updated |
| Drop | `WHERE id IS NOT NULL` | Cannot analyze care quality without patient identity |
| De-identify | `birthdate → birth_year` (exact date removed) | PDPA compliance — exact birthdate is unnecessary for age-group analysis |
| Derive | `age_group`: 0-17 / 18-34 / 35-50 / 51-65 / 65+ | Age group is sufficient for cohort analysis |
| De-identify | `deathdate → is_deceased (BOOLEAN)` | Only need to know if the patient is alive, not the exact date |
| Standardize | `M/MALE → Male`, `F/FEMALE → Female`, else `Unknown` | Source is inconsistent; standardize for demographic breakdowns |
| DQ WARN | `birthdate IS NULL` | Age group cannot be derived |

**`encounters`** — same transforms as Q1 (shared silver table)

**`conditions`**

| Transform | Rule | Reason |
|---|---|---|
| Deduplication | `PARTITION BY patient, encounter, code ORDER BY _ingested_at DESC` | No unique ID column; composite key is the identity of a condition record |
| Drop | `WHERE patient IS NOT NULL` | A condition with no patient cannot be used for cohort analysis |
| Derive | `is_active = (stop IS NULL)` | NULL end date = ongoing condition — key flag for identifying chronic patients |
| Standardize | `TRIM(code)`, `TRIM(description)` | Remove whitespace from SNOMED codes |
| DQ WARN | `encounter IS NULL` | Condition cannot be linked to a visit |

**`careplans`**

| Transform | Rule | Reason |
|---|---|---|
| Deduplication | `PARTITION BY id ORDER BY _ingested_at DESC` | Care plan status and end date can be updated |
| Drop | `WHERE patient IS NOT NULL` | Cannot evaluate effectiveness without patient linkage |
| Derive | `is_active = (stop IS NULL)` | Active care plans are the intervention group in Q2 — critical flag |
| Rename | `reasoncode → reason_code`, `reasondescription → reason_description` | Clean snake_case naming |
| DQ WARN | `encounter IS NULL` | Care plan cannot be tied to a specific visit |

---

### Phase 4 — Data Quality Logging (`metadata`)

Every silver run writes per-column metrics to `metadata.dq_log_results` via `scripts/dq_logger.py`. Each metric has an explicit threshold; rows below it are marked `FAILED`.

Two levels of checks per table:
- **Table-level** (`column_name = NULL`) — overall PASS rate % for dashboard trending
- **Column-level** — per-column completeness / validity / accuracy with thresholds

| Table | Checks logged |
|---|---|
| `silver_encounters` | table PASS rate, id/patient_id completeness, encounter_class validity, cost accuracy |
| `silver_payers` | id completeness, ownership validity |
| `silver_claims` | table PASS rate, id/patient_id completeness, claim_status validity, outstanding accuracy |
| `silver_claims_transactions` | table PASS rate, claim_id completeness, transaction_type validity, amount accuracy |
| `silver_patients` | table PASS rate, id/birth_year completeness, birth_year/gender/age_group validity |
| `silver_conditions` | table PASS rate, patient_id/code/start_date completeness |
| `silver_careplans` | table PASS rate, patient_id/code completeness |

`run_id` is taken from the `AIRFLOW_RUN_ID` environment variable set by the DAG, or auto-generated as a timestamped UUID when running scripts manually.

---

### Phase 5 — Gold Layer (`dw_gold`)

**Design principle:** Gold is analytics-ready and denormalized. No joins needed from BI tools. Surrogate keys (SERIAL integers) replace source UUIDs for join performance. All KPIs are pre-calculated at load time.

#### Shared Dimensions

| Table | Grain | Key columns |
|---|---|---|
| `dim_date` | One row per calendar day | `date_key` (YYYYMMDD int), day/month/quarter/year, `is_weekend` |
| `dim_patients` | One row per patient | surrogate `patient_key`, `age_group`, `gender`, `race`, `is_deceased` |
| `dim_payers` | One row per payer | surrogate `payer_key`, `name`, `ownership` |

#### Q2-only Dimension

| Table | Grain | Key columns |
|---|---|---|
| `dim_conditions` | One row per SNOMED code | surrogate `condition_key`, `code`, `description`, **`condition_category` (Chronic / Acute)** |

#### Fact Tables

| Table | Grain | Pre-calculated KPIs |
|---|---|---|
| `fact_claims` | One row per claim | `total_claim_cost`, `payer_coverage`, `patient_oop`, `total_outstanding`, `avg_days_to_payment` |
| `fact_encounters` | One row per encounter | `duration_minutes`, `patient_oop`, `condition_count`, **`has_active_careplan`**, **`is_readmission`** |

#### Pre-calculated Flags

| Flag | Logic | Answers |
|---|---|---|
| `is_readmission` | inpatient/emergency encounter within 30 days of prior encounter (same patient, via `LAG()`) | Q2: do patients without care plans readmit more? |
| `has_active_careplan` | patient had an active care plan on the encounter date | Q2: intervention group identifier |
| `condition_category` | Chronic vs Acute based on SNOMED description keywords (diabetes, hypertension, asthma, etc.) | Q2: chronic condition cohort filter |
| `avg_days_to_payment` | avg days from charge to PAYMENT transaction per claim | Q1: payer settlement speed |

---

## Database schemas

| Schema | Purpose |
|---|---|
| `hospital_op` | Raw operational tables — direct CSV dump, never modified |
| `dw_bronze` | Append-only bronze layer with lineage columns |
| `dw_silver` | Cleaned and validated silver layer with DQ flags |
| `dw_gold` | Star schema gold layer — ready for BI tools |
| `metadata` | DQ run logs (`metadata.dq_log_results`) |

---

## Example queries

### Monitor data quality

```sql
-- All failed DQ checks
SELECT run_id, table_name, column_name, metric_type,
       ROUND(metric_value, 1) AS value, threshold
FROM metadata.dq_log_results
WHERE status = 'FAILED'
ORDER BY checked_at DESC;

-- DQ trend by table over time
SELECT table_name, status, COUNT(*), ROUND(AVG(metric_value), 1) AS avg_value
FROM metadata.dq_log_results
GROUP BY table_name, status
ORDER BY table_name;
```

### Q1 — Revenue Cycle

```sql
-- Avg days-to-payment and outstanding balance by payer and encounter class
SELECT
    dp.name              AS payer,
    dp.ownership,
    fc.encounter_class,
    ROUND(AVG(fc.avg_days_to_payment), 1) AS avg_days_to_payment,
    ROUND(SUM(fc.total_outstanding), 2)   AS total_outstanding
FROM dw_gold.fact_claims fc
JOIN dw_gold.dim_payers dp ON dp.payer_key = fc.payer_key
WHERE fc.claim_status = 'OPEN'
GROUP BY dp.name, dp.ownership, fc.encounter_class
ORDER BY total_outstanding DESC;
```

### Q2 — Care Quality

```sql
-- Readmission rate: patients with vs without an active care plan
SELECT
    has_active_careplan,
    COUNT(*)                                                  AS total_encounters,
    SUM(is_readmission::INT)                                  AS readmissions,
    ROUND(100.0 * SUM(is_readmission::INT) / COUNT(*), 2)    AS readmission_rate_pct
FROM dw_gold.fact_encounters
WHERE encounter_class IN ('inpatient', 'emergency')
GROUP BY has_active_careplan;
```

---

## Testing

### Check all layers are populated

```bash
# hospital_op — all 18 tables should have rows
docker-compose exec airflow python /opt/airflow/scripts/verify.py --layer hospital_op

# bronze — 7 tables populated, no duplicate _row_hash
docker-compose exec airflow python /opt/airflow/scripts/verify.py --layer bronze

# silver + DQ log — PASS rate >= 95% per table
docker-compose exec airflow python /opt/airflow/scripts/verify.py --layer silver
docker-compose exec airflow python /opt/airflow/scripts/verify.py --layer dq

# gold — all FK keys resolve, flags populated
docker-compose exec airflow python /opt/airflow/scripts/verify.py --layer gold

# full end-to-end
docker-compose exec airflow python /opt/airflow/scripts/verify.py --layer all
```

### Run a single step manually

```bash
# bronze
docker-compose exec airflow python /opt/airflow/scripts/bronze_q1_revenue_cycle.py --step load_encounters

# silver
docker-compose exec airflow python /opt/airflow/scripts/silver_q1_revenue_cycle.py --step load_claims

# gold
docker-compose exec airflow python /opt/airflow/scripts/gold_q2_care_quality.py --step load_fact_encounters
```

### Quick spot-check via psql

```bash
docker-compose exec postgres psql -U airflow -d airflow
```

```sql
-- Readmission answer preview
SELECT has_active_careplan,
       COUNT(*) AS encounters,
       SUM(is_readmission::INT) AS readmissions,
       ROUND(100.0 * SUM(is_readmission::INT) / COUNT(*), 1) AS readmission_pct
FROM dw_gold.fact_encounters
WHERE encounter_class IN ('inpatient','emergency')
GROUP BY has_active_careplan;

-- Payer settlement speed
SELECT p.ownership, ROUND(AVG(f.avg_days_to_payment), 1) AS avg_days
FROM dw_gold.fact_claims f
JOIN dw_gold.dim_payers p ON p.payer_key = f.payer_key
WHERE f.avg_days_to_payment IS NOT NULL
GROUP BY p.ownership ORDER BY avg_days;
```

---

## Tech stack

| Tool | Version | Role |
|---|---|---|
| Apache Airflow | 2.9.3 | Orchestration |
| PostgreSQL | 13 | Storage |
| Python | 3.9 | ETL scripts |
| psycopg2 | latest | DB driver |
| Docker Compose | v2 | Infrastructure |
