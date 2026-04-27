from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

BRONZE = 'python /opt/airflow/scripts/bronze_q1_revenue_cycle.py --step'
SILVER = 'python /opt/airflow/scripts/silver_q1_revenue_cycle.py --step'
GOLD   = 'python /opt/airflow/scripts/gold_q1_revenue_cycle.py --step'

RUN_ENV = {'AIRFLOW_RUN_ID': '{{ run_id }}'}

with DAG(
    dag_id='q1_revenue_cycle',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Q1 Revenue Cycle: daily bronze → silver → gold',
    tags=['q1', 'revenue-cycle'],
) as dag:

    # ------------------------------------------------------------------
    # BRONZE
    # ------------------------------------------------------------------
    b_encounters = BashOperator(
        task_id='bronze_load_encounters',
        bash_command=f'{BRONZE} load_encounters',
    )
    b_payers = BashOperator(
        task_id='bronze_load_payers',
        bash_command=f'{BRONZE} load_payers',
    )
    b_claims = BashOperator(
        task_id='bronze_load_claims',
        bash_command=f'{BRONZE} load_claims',
    )
    b_claims_tx = BashOperator(
        task_id='bronze_load_claims_transactions',
        bash_command=f'{BRONZE} load_claims_transactions',
    )

    # ------------------------------------------------------------------
    # SILVER
    # ------------------------------------------------------------------
    s_encounters = BashOperator(
        task_id='silver_load_encounters',
        bash_command=f'{SILVER} load_encounters',
        env=RUN_ENV, append_env=True,
    )
    s_payers = BashOperator(
        task_id='silver_load_payers',
        bash_command=f'{SILVER} load_payers',
        env=RUN_ENV, append_env=True,
    )
    s_claims = BashOperator(
        task_id='silver_load_claims',
        bash_command=f'{SILVER} load_claims',
        env=RUN_ENV, append_env=True,
    )
    s_claims_tx = BashOperator(
        task_id='silver_load_claims_transactions',
        bash_command=f'{SILVER} load_claims_transactions',
        env=RUN_ENV, append_env=True,
    )

    # ------------------------------------------------------------------
    # GOLD
    # ------------------------------------------------------------------
    g_dim_date = BashOperator(
        task_id='gold_load_dim_date',
        bash_command=f'{GOLD} load_dim_date',
    )
    g_dim_patients = BashOperator(
        task_id='gold_load_dim_patients',
        bash_command=f'{GOLD} load_dim_patients',
    )
    g_dim_payers = BashOperator(
        task_id='gold_load_dim_payers',
        bash_command=f'{GOLD} load_dim_payers',
    )
    g_fact_claims = BashOperator(
        task_id='gold_load_fact_claims',
        bash_command=f'{GOLD} load_fact_claims',
    )

    # ------------------------------------------------------------------
    # Dependencies
    # ------------------------------------------------------------------

    # Bronze
    [b_encounters, b_payers]
    b_encounters >> b_claims >> b_claims_tx

    # Bronze → Silver (all bronze done before silver starts)
    [b_claims_tx, b_payers] >> s_encounters
    [b_claims_tx, b_payers] >> s_payers

    # Silver
    s_encounters >> s_claims >> s_claims_tx

    # Silver → Gold (all silver done before gold starts)
    [s_claims_tx, s_payers] >> g_dim_date
    [s_claims_tx, s_payers] >> g_dim_patients
    [s_claims_tx, s_payers] >> g_dim_payers

    # Gold
    [g_dim_date, g_dim_patients, g_dim_payers] >> g_fact_claims
