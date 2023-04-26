from airflow.operators.postgres_operator import PostgresOperator
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from operators.db_to_db_operator import DBToDBOperator


def db_to_db_task_group(
        dag: DAG,
        group_id: str,
        source_conn_id: str,
        source_query: str,
        target_conn_id: str,
        target_schema: str,
        target_table: str,
        tooltip: str = None
) -> TaskGroup:
    """
    :param dag: The DAG that this TaskGroup belongs to.
    :param group_id: a unique, meaningful id for the TaskGroup. group_id must not conflict
        with group_id of TaskGroup or task_id of tasks in the DAG. Root TaskGroup has group_id
        set to None.
    :param source_conn_id: Airflow connection id to execute source_query on and extract data from.
    :param source_query: Query to be run on the source_conn_id database.
    :param target_conn_id: Airflow connection id to load data to.
    :param target_schema: Target schema of data in target database.
    :param target_table: Target table of data in target database.
    :param tooltip: Optional, The tooltip of the TaskGroup node when displayed in the UI.
    :return:
    """
    # sql templates
    truncate_stage_table_sql = """
        TRUNCATE TABLE {target_schema}.{target_table}
    """.format(target_schema=target_schema, target_table=target_table)
    # TODO: these touches on the etl.extract_executions table need to be pulled into a custom operator(s)
    create_batch_record_sql = """
           INSERT INTO etl.extract_executions
           ( table_name, extract_timestamp, batch_id, is_extract_done)
           VALUES ( '{table_name}', '{ts}', '{batch_id}', false)
        """.format(
        table_name=target_table,
        ts='{{ data_interval_start }}',
        batch_id='{{ ts_nodash_with_tz }}'
    )

    update_batch_record_sql = """
                    UPDATE etl.extract_executions SET batch_size = {batch_size}
                    WHERE batch_id = '{batch_id}' AND table_name = '{table_name}'
                """

    metadata_conn_id = 'postgres_oltp'
    if not tooltip:
        tooltip = f'etls {target_table}'

    with TaskGroup(group_id, tooltip=tooltip) as task_group:
        truncate_stage_table = PostgresOperator(
            task_id='truncate_stage',
            sql=truncate_stage_table_sql,
            postgres_conn_id=target_conn_id,
            dag=dag
        )

        create_batch_record = PostgresOperator(
            task_id='create_batch_record',
            sql=create_batch_record_sql,
            postgres_conn_id=metadata_conn_id,
            dag=dag
        )

        move_data = DBToDBOperator(
            task_id="move_data",
            sql=source_query,
            target_schema=target_schema,
            target_table=target_table,
            source_connection_id=source_conn_id,
            target_connection_id=target_conn_id,
            dag=dag
        )

        update_batch_record = PostgresOperator(
            task_id='update_batch_record',
            sql=update_batch_record_sql.format(
                batch_size='{{ ti.xcom_pull(task_ids="%s.move_data", key="return_value") }}' % group_id,
                batch_id='{{ ts_nodash_with_tz }}',
                table_name=target_table
            ),
            postgres_conn_id=metadata_conn_id,
            dag=dag
        )

        truncate_stage_table >> create_batch_record >> move_data >> update_batch_record

    return task_group
