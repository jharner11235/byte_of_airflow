"""
DAG Factory to build partitioned tables from a source into Redshift. Keep in mind that this is poorly written in that it
puts too much burden on the airflow worker, which is not meant to do this type of work. However, it is used in a prod
environment currently, and if run during a quiet time it's performant enough.

This was written for 1.10.12 and may need updates for 2.0
"""
from datetime import datetime
import pendulum
import os
from pyspark.sql import SparkSession
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.oracle_hook import OracleHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook

eastern_tz = pendulum.timezone("US/Eastern")
oracle_conn_name = 'oracle_conn'
pg_conn_name = 'postgres_oltp'
redshift_conn_name = 'redshift_dw'
oracle_jar = "/usr/local/airflow/plugins/instantclient_18_5/ojdbc8.jar"
pg_jar = "/usr/local/airflow/plugins/jars/postgresql-42.5.0.jar"

class DagDef:
    def __init__(
            self,
            schema,
            table,
            schedule,
            start_date,
            source,
            frequency,
            folder_path,
            partition_col,
            append_col=None,
            doc=None
    ):
        """
        Base logic needed for the definition of a DAG to pull from any source database and pull into RS as a target db
        as partitioned tables. If a {table}_predicates.sql file exists, it will be used to filter the queried
        table into separate .parquet files
        :param schema: The name of the schema (what it will be called in RS and S3)
        :type schema: str
        :param table: The name of the table (what it will be called in RS and S3)
        :type table: str
        :param schedule: Cron schedule that the table should be updated
        :type schedule: str
        :param start_date: Start date for the DAG (to backfill) (ideally pass in timezone as well)
        :type start_date: datetime
        :param source: One of 'oracle', 'postgres', 'redshift' to denote source db type to get connection info
        :type source: str
        :param frequency: How often the table will be updated (used to get A. dag name B. partition logic)
        :type frequency: str
        :param folder_path: NOT the bucket name, and NOT the direct folder of the table in S3 - but what's in the middle
        :type folder_path: str
        :param partition_col: Name of the col that the partition is on
        :type partition_col: str
        :param append_col: If the table is to be updated at a higher freq than the partition_col (e.g. is partitioned by
            month but is updated daily), this is the col used to get the latest sub-partition and append to the table
        :type append_col: str
        :param doc: Dag documentation
        :type doc: str
        """
        self.folder_path = folder_path
        self.schema = schema
        self.table = table
        self.schedule = schedule
        self.dag_name = f"{table}_{frequency}"
        self.start_date = start_date
        self.source = source
        self.frequency = frequency
        self.partition_col = partition_col
        self.append_col = append_col
        self.doc = doc
        if source == 'oracle':
            self.conn = OracleHook(oracle_conn_name).get_connection(oracle_conn_name)
            self.url = f"jdbc:oracle:thin:@//{self.conn.host}"
            self.jar = oracle_jar
            driver = 'oracle.jdbc.driver.OracleDriver'
        elif source == 'postgres':
            self.conn = PostgresHook(pg_conn_name).get_connection(pg_conn_name)
            self.url = f"jdbc:postgresql://{self.conn.host}:{self.conn.port}/{self.conn.schema}"
            self.jar = pg_jar
            driver = 'org.postgresql.Driver'
        elif source == 'redshift':
            raise NotImplementedError()
        else:
            raise Exception()
        # this is used for pyspark queries on the target db
        self.properties = {
            'driver': driver,
            'user': self.conn.login,
            'password': self.conn.password
        }

dag_defs = [
    DagDef(
        'schema_a',
        'table_z',
        '0 0 1 */3 *',
        datetime(2011, 9, 30, tzinfo=eastern_tz),
        'oracle',
        'quarterly',
        'dw_data/schema_a',
        'trgt_qtr'
    ),
    DagDef(
        'schema_a',
        'table_y',
        '0 0 1 */3 *',
        datetime(2011, 9, 30, tzinfo=eastern_tz),
        'oracle',
        'quarterly',
        'dw_data/schema_a',
        'trgt_qtr'
    ),
    DagDef(
        'schema_a',
        'table_x',
        '0 0 * * *',
        datetime(2023, 4, 13, tzinfo=eastern_tz),
        'postgres',
        'daily',
        'dw_data/schema_a/external_tables',
        'target_dt_month',
        doc="This dag is partitioned by month but runs daily. This could be modified to be partitioned by date as well."
    ),
    DagDef(
        'schema_a',
        'table_w',
        '0 0 1 * *',
        datetime(2022, 3, 31, tzinfo=eastern_tz),
        'postgres',
        'monthly',
        'dw_data/schema_a/external_tables',
        'report_end_dt'
    ),
    DagDef(
        'schema_a',
        'table_v',
        '0 0 1 * *',
        datetime(2022, 3, 31, tzinfo=eastern_tz),
        'postgres',
        'monthly',
        'dw_data/schema_a/external_tables',
        'report_end_dt'
    ),
    DagDef(
        'schema_a',
        'table_u',
        '0 0 1 * *',
        datetime(2022, 3, 31, tzinfo=eastern_tz),
        'postgres',
        'monthly',
        'dw_data/schema_a/external_tables',
        'report_end_dt'
    ),
    DagDef(
        'schema_a',
        'table_t',
        '0 0 1 * *',
        datetime(2022, 3, 31, tzinfo=eastern_tz),
        'postgres',
        'monthly',
        'dw_data/schema_a/external_tables',
        'report_end_dt'
    )
]


def dag_factory(dag_def: DagDef):
    default_args = {
        'owner': 'data-team',
        'email': Variable.get('notification_email'),
        'email_on_failure': True,
        'email_on_retry': False,
        'start_date': dag_def.start_date,
        'depends_on_past': False,
        'retries': 0
    }

    dag = DAG(
        dag_id=dag_def.dag_name,
        default_args=default_args,
        catchup=True,
        schedule_interval=dag_def.schedule,
        max_active_runs=1,
        doc_md=dag_def.doc
    )

    def query_and_bucket(dag_def, partition):
        spark = SparkSession.builder \
            .config("spark.jars", dag_def.jar) \
            .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
            .getOrCreate()

        # get the base query used to replicate the table from source into RS
        s3_bucket = Variable.get('data_bucket').replace('s3://', '')
        with open(f'/usr/local/airflow/dags/dw_iqies/sql/{dag_def.table}.sql') as f:
            sql_query = f"( {f.read().format(partition=partition)} ) query"

        predicates = None
        if os.path.isfile(f'/usr/local/byte_of_airflow/dags/sql/{dag_def.table}_predicates.sql'):
            print('predicates found, grabbing...')
            with open(f'/usr/local/byte_of_airflow/dags/sql/{dag_def.table}_predicates.sql') as f:
                get_predicates = f"( {f.read().format(partition=partition)} ) query"
            predicates_df = spark.read.jdbc(
                url=dag_def.url,
                table=get_predicates,
                properties=dag_def.properties
            )
            predicates = list(predicates_df.select('predicates').toPandas()['predicates'])
            print(f'{len(predicates)} predicates found')

        s3 = S3Hook()
        local_dir = f'{dag_def.table}_{partition}'
        partition_folder = f"{dag_def.partition_col}={partition}"

        print('querying database')
        spark.read.jdbc(
            url=dag_def.url,
            table=sql_query,
            predicates=predicates,
            properties=dag_def.properties
        ).write \
            .format('parquet') \
            .option('header', 'true') \
            .save(local_dir, mode='overwrite')
        print('data written locally')

        keys = s3.list_keys(bucket_name=s3_bucket, prefix=os.path.join(target_folder, partition_folder))
        if keys is not None:
            s3.delete_objects(bucket=s3_bucket, keys=keys)
        for file in os.listdir(local_dir):
            if file.endswith('.snappy.parquet'):
                s3.load_file(filename=f'{local_dir}/{file}', key=os.path.join(target_folder, partition_folder, file), bucket_name=s3_bucket)

    def replace_partition(dag_def, partition):
        target_folder = f'{dag_def.folder_path}/{dag_def.table}'
        rs = PostgresHook(redshift_conn_name).get_conn()
        rs.autocommit = True

        check_partition_query = f"""
        select	1
        from	svv_external_partitions 
        where	schemaname = '{dag_def.schema}'
            and tablename = '{dag_def.table}'
            and values = '["{partition}"]'"""
        print(check_partition_query)

        drop_partition_query = f"""
            alter table {dag_def.schema}.{dag_def.table} drop
            partition({dag_def.partition_col}='{partition}')"""
        print(drop_partition_query)

        with rs.cursor() as curs:
            curs.execute(check_partition_query)
            resp = curs.fetchone()
            print('resp', resp)
            if resp is not None:
                curs.execute(drop_partition_query)

        add_partition_query = f"""
            alter table {dag_def.schema}.{dag_def.table} add
            partition({dag_def.partition_col}='{partition}')
            location '{Variable.get('data_bucket')}/{target_folder}/{dag_def.partition_col}={partition}/'"""

        with rs.cursor() as curs:
            curs.execute(add_partition_query)

    if dag_def.frequency == 'quarterly':
        partition = '{{execution_date.year}}' + 'Q' + '{{(execution_date.month - 1) // 3 + 1}}'
    elif dag_def.frequency in ('monthly', 'daily'):
        # add 31 days to get to the next month, hardcode as first day of that month, then subtract a day to get the last
        # day of the month we need.
        partition = "{{(macros.datetime((execution_date+macros.timedelta(days=30)).year, (execution_date+macros.timedelta(days=30)).month, 1) - macros.timedelta(days=1)).strftime('%Y-%m-%d')}}"
        print(partition)
    else:
        raise Exception()
    target_folder = f'{dag_def.folder_path}/{dag_def.table}'

    update_table = PythonOperator(
        task_id=f'update_{dag_def.frequency}',
        python_callable=query_and_bucket,
        op_kwargs={'dag_def': dag_def, 'partition': partition},
        dag=dag
    )

    add_partition = PythonOperator(
        task_id='add_partition',
        python_callable=replace_partition,
        op_kwargs={'dag_def': dag_def, 'partition': partition},
        dag=dag
    )

    update_table >> add_partition
    return dag


for dag_def in dag_defs:
    globals()[dag_def.dag_name] = dag_factory(dag_def)
