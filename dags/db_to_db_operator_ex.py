"""
This is an example of a very basic full truncation and reload of a target table from the source database. All the work
is hidden away in the Task Group - We pull from the source with a source query, write to a stage table, and then trunc
and fill a target table. The source query could be as simple or complex as you'd like, though it may be ideal to do all
heavier queries OUTSIDE the production db... many a website has crashed from a rogue query and a bump in traffic.

The task group here is handy because this may just be a simple fill, but later you may want to add another 5 tasks-worth
of work, and having all the ETL rolled up into a single TG clears up the UI.
"""
import pendulum

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from db_to_db_task_group import db_to_db_task_group


target_schema = 'target'
target_table = 'items'
target_conn = 'ods'
source_conn = 'oltp'
source_query = 'select * from source.items'

default_args = {
    'owner': 'de_team',
    'email_on_failure': False,
    'start_date': pendulum.datetime(2023, 4, 1, tz="America/Los_Angeles"),  # makes DAG timezone aware
    'depends_on_past': False,
    'retries': 0
}


with DAG(
    dag_id='oltp_to_ods_items',
    catchup=False,
    schedule_interval='0 0 * * *',   # since we're tz aware, will run at midnight PT
    default_args=default_args,
    doc_md=__doc__,
    tags=['example']
) as dag:
    # the DAG will just grab whatever is in `source` and truncate `target` and fill it
    etl_tg = db_to_db_task_group(
        dag,
        f'etl_{target_table}',
        source_conn,
        source_query,
        target_conn,
        target_schema,
        target_table,
        full_load=True
    )
