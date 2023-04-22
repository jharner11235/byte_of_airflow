from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults


class DBToDBOperator(BaseOperator):
    """
    Copies data from one database to another by opening a cursor in both and writing batches between them,
     future versions may leverage copy functions for queries returning over XXXX amount of data.
     Currently only validated with Oracle, Redshift, and Postgres, but may already work with other db types as well.
    """

    @apply_defaults
    def __init__(self,
                 sql,
                 target_schema,
                 target_table,
                 source_connection_id,
                 target_connection_id,
                 itersize=20000,
                 meta_db_conn_id='postgres_oltp',
                 extract_executions='etl.extract_executions',
                 provide_context=True,
                 parameters=None,
                 *args,
                 **kwargs):
        """
        :param sql: The SQL to execute on the source table
        :type sql: string
        :param target_schema: The schema to load the data to
        :type target_schema: string
        :param target_table: The table to load the data to
        :type target_table: string
        :param source_connection_id: The database to pull from
        :type source_connection_id: string
        :param target_connection_id: The database to write to
        :type target_connection_id: string
        :param itersize:  The itersize of the cursors used
        :type itersize: integer
        :param meta_db_conn_id: Reference to a db connection for storing metadata about the dag run
        :type meta_db_conn_id: string
        :param extract_executions: table to use for storing batch metadata
        :type extract_executions: string
        :param provide_context: if set to true, Airflow will pass a set of
            keyword arguments that can be used in your function. This set of
            kwargs correspond exactly to what you can use in your jinja
            templates. For this to work, you need to define `**kwargs` in your
            function header.  This should always be set to True.  This needs to
            be present as well to prevent nag warnings
        :type provide_context: bool
        """
        super(DBToDBOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.target_schema = target_schema
        self.target_table = target_table
        self.source_connection_id = source_connection_id
        self.target_connection_id = target_connection_id
        self.itersize = itersize
        self.meta_db_conn_id = meta_db_conn_id
        self.record_ct = 0
        self.extract_executions = extract_executions
        self.provide_context = provide_context
        self.parameters = parameters
        self.columns = []

    def execute(self, context):
        """
        Operator method for code to run when operator is run by Airflow.
        """
        with self._query_db() as src_cursor:
            self._get_columns(src_cursor)
            self._write_to_target(src_cursor)

        return self.record_ct

    def _query_db(self):
        """
        Queries source db and returns a cursor to the results.
        """
        conn = BaseHook.get_hook(self.source_connection_id).get_conn()
        cursor = conn.cursor()
        cursor.itersize = self.itersize
        cursor.execute(self.sql, self.parameters)
        return cursor

    def _get_columns(self, cursor):
        self.columns = [col[0] for col in cursor.description]

    def _write_to_target(self, src_cursor):
        tgt_conn = BaseHook.get_hook(self.target_connection_id).get_conn()
        write_sql = f"insert into {self.target_schema}.{self.target_table} ({','.join(self.columns)}) values " + \
                    "{values}"
        col_ct = len(self.columns)
        with tgt_conn.cursor() as tgt_cursor:
            while True:
                data = src_cursor.fetchmany(self.itersize)

                if not data:
                    break
                template = ",".join(
                    [f"""({','.join(["%s"] * col_ct)})"""] * len(data))
                arguments = [item for row in data for item in row]
                tgt_cursor.execute(write_sql.format(values=template), arguments)
                # CursorResult.rowcount doesn't work in this use-case as it's only useful in delete/update statements
                self.record_ct += len(data)

            tgt_conn.commit()
        tgt_conn.close()
        self.log.info(f'{self.record_ct} records written')
