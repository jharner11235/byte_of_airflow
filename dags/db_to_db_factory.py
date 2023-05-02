"""
Pending docs
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from db_to_db_task_group import db_to_db_task_group


target_conn = 'ods'
source_conn = 'oltp'

default_args = {
    'owner': 'de_team',
    'email_on_failure': False,
    'start_date': datetime(2023, 4, 1),
    'depends_on_past': False,
    'retries': 0
}

for table in ['addresses', 'customers', 'orders', 'refunds']:
    # lower and upper bound updated in db_to_db_op
    source_query = f"""
        select *
        from source.{table} """ + \
        "where updated_at between '{lower_bound}' and '{upper_bound}'"
    with DAG(
        dag_id=f'oltp_to_ods_{table}',
        catchup=False,
        schedule_interval='*/15 * * * *',
        default_args=default_args,
        doc_md=__doc__.format(table=table),
        tags=['example']
    ) as dag:
        etl_tg = db_to_db_task_group(
            dag,
            f'etl_{table}',
            source_conn,
            source_query,
            target_conn,
            'stage',
            table
        )
