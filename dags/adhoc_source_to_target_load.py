"""
Adhoc DAG for running one time manual loads. Uses environment variable adhoc_source_to_target_load_metadata (see below).
This is useful for moving big batches of data between RDS and Redshift, and can write RDS to RDS, RS to RDS, etc. This
is specfically meant for AWS but could logically be used for on-prem instances of PG as well. Note that this WILL NOT
work here (yet, it's a low priority since on-prem is growingly not common) but has been set up as though it could. This
DAG is able to create the target object if needed and could be adapted to assume data types from the query to skip the
column mapping.

This has been used to move ~80gb/hr from PG to RS for a data migration.

You'll need to enable the aws_s3 extensions for RDS and ensure permissions are in place for it and unload
https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html
https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html
https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/oracle-s3-integration.html
It's worth the read :)

{
    "source_connection": A Postgres or Redshift connection type, the database to read from,
    "source_query": SELECT statement to be executed in source database,
    "target_connection": A Postgres or Redshift connection type, the database to write to,
    "trunc_target": Will truncate target if True, default True,
    "drop_target": Will drop target if True, default False,
    "target_schema": Name of existing schema in target database to write data to,
    "target_table": Name of non/existing table in target database to write data to,
    "target_column_mapping": List of dicts where each dict has the col name as a key and dtype as a value which will
            be used to build target table, e.g.
    [
      {"customer_reference_id": "numeric(18,0)"},
      {"old_wp_subscriber_id": "bigint"},
      {"new_laravel_subscriber_id": "integer"},
      {"state_cd": "character varying(4)"}
    ]
  }

helpful SQL to build column_mapping
select '{"'||column_name||'": "'||case
									when data_type like '%char%'
										then data_type||'('||character_maximum_length||')'
									when data_type like '%int%' or data_type like 'timestamp%' or data_type = 'date'
										then data_type
									when data_type = 'numeric'
										then data_type||'('||numeric_precision||','||numeric_scale||')'
end||'"},', column_name||',', *
from information_schema.columns c
where table_schema = 'debug'
and table_name = 'table'
order by ordinal_position



"""
import subprocess
from datetime import datetime, timedelta
import re

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


def get_connection(conn_id):
    """validates passed connection ids"""
    conn = BaseHook.get_connection(conn_id)
    if conn is None:
        raise Exception(f'connection {conn} not found')
    if conn.conn_type not in ['postgres', 'aws']:   # this should be changed to redshift, not aws, post 2.0
        raise Exception(f'connection type {conn.conn_type} not accepted')
    return conn


def rs_command_wrapper(connection, sql):
    """cleans up code, template for execution of code in psql"""
    return f"""psql postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema} -c "{sql}"
     """


def run_bash_cmd(cmd, db_conn):
    """runs and returns applicable info for a bash command. db_conn is passed to remove password from logs"""
    resp = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(f"{resp.returncode}: {resp.stdout}")
    if resp.returncode != 0:
        print(f"{resp.stderr.replace(db_conn.password, '*******')}")
        raise Exception()
    records = 0
    records_match = re.search(r"-\n\s*(\d*)", resp.stdout)
    if records_match:
        records = int(records_match.group(1))
    return records


def run_sql(connection_id, sql):
    """runs the passed sql on the passed RS connection"""
    rs_hook = PostgresHook(connection_id)
    rs_hook.run(sql)
    # return record count for QA
    records = 0
    for output in rs_hook.conn.notices:
        print(output)
        records_match = re.search(r"(\d*) record\(s\)", output)
        if records_match:
            records = int(records_match.group(1))
    return records


default_args = {
    'owner': 'airflow',
    'email': Variable.get('notification_email'),
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2019, 1, 1),
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=1),
    'retries': 0
}

dag = DAG(
    dag_id="ADHOC_SOURCE_TO_TARGET_LOAD",
    description="Enables high-speed data movement between Postgres and Redshift",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    schedule_interval=None,
    max_active_runs=1,
    doc_md=__doc__
)


# set constants
metadata = Variable.get("adhoc_source_to_target_load_metadata", deserialize_json=True)
s3_bucket = Variable.get('stage_data_bucket')
iam_role = Variable.get('iam_role')
batch_id = '{{ ts_nodash_with_tz }}'
s3_key = f'adhoc_stage/data'
region = 'us-east-1'

# these passed connection_ids may be changed out for passing one of [redshift, iqies, qbic]
src_connection_id = metadata['source_connection']
src_conn = get_connection(src_connection_id)
tgt_connection_id = metadata['target_connection']
tgt_conn = get_connection(tgt_connection_id)
tgt_schema = metadata['target_schema']
tgt_table = metadata['target_table']
tgt_cols = metadata.get('target_column_mapping')
src_sql = metadata['source_query']
#
should_trunc = metadata.get('trunc_target', True)
should_drop = metadata.get('drop_target', False)
if ';' in src_sql or 'drop' in src_sql.lower() or \
  'delete' in src_sql.lower() or 'select' not in src_sql.lower() \
  or 'create' in src_sql.lower():
    raise ValueError("Bad SQL found. Should only be a SELECT: %s" % src_sql)


def extract_data(ts_nodash_with_tz, **context):
    """exports data from source database depending on db type"""
    print(f"Exporting from {src_connection_id}")
    if src_conn.conn_type == 'postgres':
        pg_extract_sql = "SELECT * from aws_s3.query_export_to_s3(" + \
            f"'{src_sql}'," + \
            f"aws_commons.create_s3_uri('{s3_bucket}','{s3_key}/{ts_nodash_with_tz}/data.csv','{region}')," + \
            f"options :='format csv');"
        cmd = rs_command_wrapper(src_conn, pg_extract_sql)
        records_for_qa = run_bash_cmd(cmd, src_conn)
    else:
        # due to get_connection() we know anything not PG is RS
        rs_extract_sql = f"""
            unload ('{src_sql.replace("'", "''")}')   
            to 's3://{s3_bucket}/{s3_key}/{ts_nodash_with_tz}/data_' 
            iam_role '{iam_role}'
            ALLOWOVERWRITE
            CSV
            EXTENSION 'csv';
        """
        records_for_qa = run_sql(src_connection_id, rs_extract_sql)

    return records_for_qa


def load_data(ts_nodash_with_tz, **context):
    """builds the target table and fills it from S3"""
    print(f"Importing {tgt_schema}.{tgt_table} to {tgt_connection_id}")
    if should_drop:
        prep_sql = f"""drop table if exists {tgt_schema}.{tgt_table};"""
    elif should_trunc:
        prep_sql = f"""truncate table {tgt_schema}.{tgt_table};"""
    else:
        prep_sql = "select 0;"
    if tgt_cols:
        create_sql = f"""create table if not exists {tgt_schema}.{tgt_table} ({', '.join([f'{list(col.keys())[0]} {list(col.values())[0]}' for col in tgt_cols])})"""
    else:
        create_sql = "select 0;"
    records_for_qa = 0

    if tgt_conn.conn_type == 'postgres':
        cmds = [rs_command_wrapper(tgt_conn, prep_sql), rs_command_wrapper(tgt_conn, create_sql)]
        s3 = S3Hook()
        files = s3.list_keys(bucket_name=s3_bucket, prefix=f"{s3_key}/{ts_nodash_with_tz}")
        # loop through files in adhoc_stage target folder and copy to postgres
        for file in files:
            copy_sql = "select aws_s3.table_import_from_s3 (" + \
                f"'{tgt_schema}.{tgt_table}'," + \
                "''," + \
                "'(format csv)'," + \
                f"aws_commons.create_s3_uri('{s3_bucket}', '{file}', '{region}'));"
            cmds.append(rs_command_wrapper(tgt_conn, copy_sql))
        # loop through commands and fire off, if one fails it'll die mid-loop
        for cmd in cmds:
            new_records = run_bash_cmd(cmd, tgt_conn)
            records_for_qa += new_records
    else:
        # due to get_connection() we know anything not PG is RS
        sqls = [prep_sql, create_sql]
        copy_sql = f"""COPY {tgt_schema}.{tgt_table}
            FROM 's3://{s3_bucket}/{s3_key}/{ts_nodash_with_tz}'
            iam_role '{iam_role}'
            region '{region}'
            FORMAT CSV"""
        sqls.append(copy_sql)
        for sql in sqls:
            new_records = run_sql(tgt_connection_id, sql)
            records_for_qa += new_records

    print(f"data loaded to {tgt_schema}.{tgt_table}")
    return records_for_qa


def check_values(src_ct, tgt_ct):
    if src_ct != tgt_ct:
        raise Exception('Target count does not match source')


extract_data_from_source = PythonOperator(
    task_id="extract_data_from_source",
    dag=dag,
    python_callable=extract_data,
    provide_context=True
)

load_data_to_target = PythonOperator(
    task_id="load_data_to_target",
    dag=dag,
    python_callable=load_data,
    provide_context=True
)

dq_check = PythonOperator(
    task_id="dq_check",
    dag=dag,
    python_callable=check_values,
    op_kwargs={
        'src_ct': '{{ ti.xcom_pull(task_ids="extract_data_from_source", key="return_value") }}',
        'tgt_ct': '{{ ti.xcom_pull(task_ids="load_data_to_target", key="return_value") }}'
    }
)

extract_data_from_source >> load_data_to_target >> dq_check
