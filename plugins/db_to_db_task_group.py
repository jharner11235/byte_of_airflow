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
        tooltip: str = None,
        full_load=False
) -> TaskGroup:
    """
    This is a more general TaskGroup that is able to do most ETL and can be built to take more parameters and give more
    control if desired - This allows us to pull data from a production db into a DW or ODS and we can do some T during
    the E.

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
    :param full_load: Default False, if True then the target will be fully truncated rather than upserted
    :return:
    """
    extract_at = '{{ data_interval_end }}'
    batch_id = '{{ ts_nodash.replace("T", "") }}'
    # sql templates
    # TODO: these touches on the etl.extract_executions table need to be pulled into a custom operator(s)
    truncate_stage_table_sql = """
        TRUNCATE TABLE {target_schema}.{target_table}
    """.format(target_schema=target_schema, target_table=target_table)
    create_batch_record_sql = """
           INSERT INTO etl.extract_executions
           ( table_name, extract_timestamp, batch_id, is_extract_done)
           VALUES ( '{table_name}', '{ts}', '{batch_id}', false)
        """.format(
        table_name=target_table,
        ts=extract_at,
        batch_id=batch_id
    )
    # if there's not an entry, then substitute a very low date - return as varchar
    get_latest_batch_sql = """
select	extract_timestamp::varchar dt
from	(
    select  extract_timestamp
    from    etl.extract_executions
    where   table_name = '{table_name}'
        and is_extract_done = true
    union all
    select '1900-01-01'
) a
order by extract_timestamp desc
limit 1""".format(
        table_name=target_table
    )

    update_batch_record_sql = """
        UPDATE etl.extract_executions
        SET batch_size = {batch_size},
            is_extract_done = true
        WHERE batch_id = '{batch_id}' AND table_name = '{table_name}'
        """
    if full_load:
        prep_sql = f"truncate table target.{target_table}"
    else:
        prep_sql = f"""
            delete from target.{target_table} a
            using stage.{target_table} b
            where a.id = b.id"""

    write_sql = f"""
        insert into target.{target_table}
        select *
        from stage.{target_table}"""

    metadata_conn_id = 'ods'
    if not tooltip:
        tooltip = f'etls {target_table}'

    with TaskGroup(group_id, tooltip=tooltip) as task_group:
        create_batch_record = PostgresOperator(
            task_id='create_batch_record',
            sql=create_batch_record_sql,
            postgres_conn_id=metadata_conn_id,
            dag=dag
        )
        # if we're expecting to trunc/reload, no need to query the ETL table
        if full_load:
            lower_bound = None
        else:
            lower_bound = '{{ ti.xcom_pull(task_ids="%s.get_latest_batch", key="return_value")[0][0] }}' % group_id
            get_latest_batch = PostgresOperator(
                task_id='get_latest_batch',
                sql=get_latest_batch_sql,
                postgres_conn_id=metadata_conn_id,
                dag=dag
            )
            # don't forget to order the tasks still
            create_batch_record >> get_latest_batch

        truncate_stage_table = PostgresOperator(
            task_id='truncate_stage',
            sql=truncate_stage_table_sql,
            postgres_conn_id=target_conn_id,
            dag=dag
        )

        move_data = DBToDBOperator(
            task_id="move_data",
            lower_bound=lower_bound,
            sql=source_query,
            target_schema=target_schema,
            target_table=target_table,
            source_connection_id=source_conn_id,
            target_connection_id=target_conn_id,
            dag=dag
        )

        prep_target = PostgresOperator(
            task_id='prep_target_table',
            sql=prep_sql,
            postgres_conn_id=target_conn_id,
        )

        from_stage = PostgresOperator(
            task_id='update_target_table',
            sql=write_sql,
            postgres_conn_id=target_conn_id,
        )

        update_batch_record = PostgresOperator(
            task_id='update_batch_record',
            sql=update_batch_record_sql.format(
                batch_size='{{ ti.xcom_pull(task_ids="%s.move_data", key="return_value") }}' % group_id,
                batch_id=batch_id,
                table_name=target_table
            ),
            postgres_conn_id=metadata_conn_id,
            dag=dag
        )

        create_batch_record >> truncate_stage_table >> move_data >> prep_target >> from_stage >> update_batch_record

    return task_group
