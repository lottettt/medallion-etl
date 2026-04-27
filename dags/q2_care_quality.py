from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

BRONZE = 'python /opt/airflow/scripts/bronze_q2_care_quality.py --step'
SILVER = 'python /opt/airflow/scripts/silver_q2_care_quality.py --step'
GOLD   = 'python /opt/airflow/scripts/gold_q2_care_quality.py --step'

RUN_ENV = {'AIRFLOW_RUN_ID': '{{ run_id }}'}

with DAG(
    dag_id='q2_care_quality',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Q2 Care Quality: daily bronze → silver → gold',
    tags=['q2', 'care-quality'],
) as dag:

    # ------------------------------------------------------------------
    # BRONZE
    # ------------------------------------------------------------------
    b_patients = BashOperator(
        task_id='bronze_load_patients',
        bash_command=f'{BRONZE} load_patients',
    )
    b_encounters = BashOperator(
        task_id='bronze_load_encounters',
        bash_command=f'{BRONZE} load_encounters',
    )
    b_conditions = BashOperator(
        task_id='bronze_load_conditions',
        bash_command=f'{BRONZE} load_conditions',
    )
    b_careplans = BashOperator(
        task_id='bronze_load_careplans',
        bash_command=f'{BRONZE} load_careplans',
    )

    # ------------------------------------------------------------------
    # SILVER
    # ------------------------------------------------------------------
    s_patients = BashOperator(
        task_id='silver_load_patients',
        bash_command=f'{SILVER} load_patients',
        env=RUN_ENV, append_env=True,
    )
    s_encounters = BashOperator(
        task_id='silver_load_encounters',
        bash_command=f'{SILVER} load_encounters',
        env=RUN_ENV, append_env=True,
    )
    s_conditions = BashOperator(
        task_id='silver_load_conditions',
        bash_command=f'{SILVER} load_conditions',
        env=RUN_ENV, append_env=True,
    )
    s_careplans = BashOperator(
        task_id='silver_load_careplans',
        bash_command=f'{SILVER} load_careplans',
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
    g_dim_conditions = BashOperator(
        task_id='gold_load_dim_conditions',
        bash_command=f'{GOLD} load_dim_conditions',
    )
    g_fact_encounters = BashOperator(
        task_id='gold_load_fact_encounters',
        bash_command=f'{GOLD} load_fact_encounters',
    )

    # ------------------------------------------------------------------
    # Dependencies
    # ------------------------------------------------------------------

    # Bronze: patients and encounters run in parallel, then conditions/careplans
    [b_patients, b_encounters]
    b_encounters >> [b_conditions, b_careplans]

    # Bronze → Silver: all bronze must finish before silver starts
    [b_patients, b_conditions, b_careplans] >> s_patients
    [b_patients, b_conditions, b_careplans] >> s_encounters

    # Silver: encounters first, then conditions/careplans
    s_encounters >> [s_conditions, s_careplans]

    # Silver → Gold: all silver must finish before gold starts
    [s_patients, s_conditions, s_careplans] >> g_dim_date
    [s_patients, s_conditions, s_careplans] >> g_dim_patients
    [s_patients, s_conditions, s_careplans] >> g_dim_payers
    [s_patients, s_conditions, s_careplans] >> g_dim_conditions

    # Gold: all dims in parallel, then fact
    [g_dim_date, g_dim_patients, g_dim_payers, g_dim_conditions] >> g_fact_encounters
