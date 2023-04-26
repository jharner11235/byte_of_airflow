import datetime
from random import randint

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from utils.data_builder_tools import create_rand_order, create_rand_customer, create_rand_refund, create_rand_address


default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'start_date': datetime.datetime(2023, 4, 1),
    'depends_on_past': False,
    'retries': 0
}
target_conn = 'oltp'


def dice_roll():
    # TODO: grab AF var for randomness (take int 0-100 to assess how often to insert)
    return randint(0, 2) < 1


with DAG(
    dag_id="data_builder",
    start_date=datetime.datetime(2023, 4, 1),
    schedule="* * * * *",
    catchup=False
) as dag:
    # TODO: grab AF var for record cts, add more or less per run (default 1)
    data_makers = [
        {'data': 'address', 'callable': create_rand_address},
        {'data': 'customer', 'callable': create_rand_customer},
        {'data': 'order', 'callable': create_rand_order},
        {'data': 'refund', 'callable': create_rand_refund}
    ]
    conn = PostgresHook(target_conn).get_conn()
    for data_maker in data_makers:
        hamlet = ShortCircuitOperator(
            task_id=f"to_add_new_{data_maker['data']}",
            python_callable=dice_roll   # only write new data occasionally
        )
        add_data = PythonOperator(
            task_id=f"add_{data_maker['data']}",
            python_callable=data_maker['callable'],
            op_args={conn}
        )

        hamlet >> add_data
