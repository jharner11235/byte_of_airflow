"""
This is a DAG Factory. We define a DAG class and use a list of DAGs to build each pipeline - this helps keep code DRY
and keeps all logic relating to a Factory within one file. As time goes on, the class can be expanded if new needs
arise. I've used a Factory like this to combine 80 DAGs into 1 file and this has helped save us a ton when making minor
tweaks to these DAGs.

The DAG class isn't built here, but you can see it in action in RS_partitioned_external_tables.py

This DAG takes data from the `production db`, source.{table}, and stages in the `DW/ODS` in stage.{table}, so that we
can delete the overlapping IDs from target.{table} before inserting the new records from stage.
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
