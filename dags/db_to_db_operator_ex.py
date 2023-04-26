from datetime import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from db_to_db_task_group import db_to_db_task_group


default_args = {
    'owner': 'de_team',
    'email_on_failure': False,
    'start_date': datetime(2023, 4, 1),
    'depends_on_past': False,
    'retries': 0
}

target_schema = 'target'
target_table = 'orders'
source_table = 'orders'
target_conn = 'ods'
source_conn = 'oltp'
query = 'select * from source.orders'
prep_target_sql = 'truncate stage.orders'
update_target_sql = 'insert into target.orders select * from stage.orders'

with DAG(
    dag_id='orders',
    catchup=False,
    schedule_interval='*/15 * * * *',
    default_args=default_args,
    tags=['example']
) as dag:
    etl_tg = db_to_db_task_group(
        dag,
        f'etl_{target_table}',
        source_conn,
        query,
        target_conn,
        target_schema,
        target_table
    )

    # TODO: create task group or operator to do incremental/full ETL from stage
    prep_target = PostgresOperator(
        task_id='prep_target_table',
        sql=prep_target_sql,
        postgres_conn_id=target_conn,
    )

    from_stage = PostgresOperator(
        task_id='update_target_table',
        sql=update_target_sql,
        postgres_conn_id=target_conn,
    )

    etl_tg >> prep_target >> from_stage
