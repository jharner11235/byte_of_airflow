import datetime

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from utils.data_builder_tools import build_connections, prep_variables, init_database


default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'start_date': datetime.datetime(2023, 4, 1),
    'depends_on_past': False,
    'retries': 0
}
target_conn = 'oltp'

with DAG(
    dag_id="database_builder",
    start_date=datetime.datetime(2023, 4, 1),
    schedule="@once",
    catchup=False
) as dag:
    add_connections = PythonOperator(
        task_id='add_connections',
        python_callable=build_connections
    )
    add_variables = PythonOperator(
        task_id='add_variables',
        python_callable=prep_variables
    )
    build_databases = PythonOperator(
        task_id='build_databases',
        python_callable=init_database,
        op_args={target_conn}
    )

    add_connections >> add_variables >> build_databases
