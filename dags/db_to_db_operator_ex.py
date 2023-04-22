from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator
from db_to_db_task_group import db_to_db_task_group


default_args = {
    'owner': 'airflow',
    'email': Variable.get('notification_email'),
    'email_on_failure': False,
    'start_date': datetime(2023, 4, 1),
    'depends_on_past': False,
    'retries': 0,
}


target_schema = 'example'
target_table = 'hha_asmt_fed'
source_table = 'hha_asmt_fed'
target_conn = 'postgres_oltp'
source_conn = 'redshift_dw'
query = 'select * from hha_rpt.hha_asmt_fed limit 1001'
prep_target_sql = 'truncate example.hha_asmt_fed_live'
update_target_sql = 'insert into example.hha_asmt_fed_live select * from example.hha_asmt_fed'

with DAG(
        dag_id='aa_test',
        catchup=False,
        schedule_interval=None,
        default_args=default_args
) as dag:
    etl_tg = db_to_db_task_group(dag, 'etl_hha_asmt_fed', source_conn, query, target_conn, target_schema, target_table)

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
