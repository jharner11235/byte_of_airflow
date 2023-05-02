from typing import Any, Callable, List
import sqlalchemy.future as sql
from sqlalchemy import text
import os
import re
import configparser
from datetime import datetime, timezone
from sqlalchemy.engine import Connection
from faker import Faker
from random import randint
from airflow import settings
from airflow.models import Connection, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

# TODO: Rebuild, probably as a class


def build_connections() -> None:
    """
    Builds connections representing the data source and the etl target
    Assumes that the airflow metadata db will be used
    :return: None
    """
    config = configparser.ConfigParser()
    airflow_home = os.environ.get('AIRFLOW_HOME')
    config.read(airflow_home + '/airflow.cfg')
    # creating connections in Airflow for our ETL processes
    connections = [
        {'conn_id': 'oltp', 'desc': "represents our prod database, the 'source' schema"},
        {'conn_id': 'ods', 'desc': "represents anything for DE: etl, stage, target schemas"}
    ]
    for connection in connections:
        n_conn = Connection(
            conn_id=connection['conn_id'],
            uri=config['database']['sql_alchemy_conn'].replace('+psycopg2', '')
        )  # create a connection object
        session = settings.Session()  # get the session
        conn_name = session.query(Connection).filter(Connection.conn_id == n_conn.conn_id).first()
        if not str(conn_name) == str(n_conn.conn_id):
            session.add(n_conn)
            session.commit()  # it will insert the connection object programmatically.


def prep_variables() -> None:
    """
    Adds variables useful for DAGs
    :return: None
    """
    variables = [
        {'key': 'new_records', 'val': 1, 'description': 'The number of new records to write per pass in data_builder'},
        {'key': 'rate_of_record_creation', 'val': 50, 'description': '%, how often to pass in data_builder'}
    ]
    for variable in variables:
        Variable.set(
            key=variable['key'],
            value=variable['val'],
            description=variable['description']
        )


def init_database(target_conn: str) -> None:
    """
    Builds all underlying schemas/tables. Using SERIAL for now for source IDs for ease, may add triggers for
     created/updated cols and foreign key constraints as well to better mimic prod DBs but not necessary
    Not intended to tear down schemas for user to prevent data loss (so a user can't run twice)
    TODO: Revamp to be more DRY, as we have DDL here and column-specific inserts later for faux data creation
    :param target_conn: Name of connection to write with
    :return: None
    """
    connection = PostgresHook(target_conn).get_conn()
    db_prep_cmds = [
        'create schema etl;',
        """create table etl.extract_executions (
        id serial primary key,
        table_name varchar(256) not null,
        extract_timestamp timestamp not null,
        batch_id bigint not null,
        batch_size int null,
        is_extract_done bool not null
        )"""
    ]

    for schema in ['stage', 'source', 'target']:
        # specific for source id cols since they're generally incremental
        if schema == 'source':
            descriptor = 'serial primary key'
        else:
            descriptor = 'int not null'
        db_prep_cmds.append(f'create schema {schema};')
        db_prep_cmds.append(f"""create table {schema}.orders (
        id {descriptor},
        customer_id int not null,
        shipping_address_id int not null,
        billing_address_id int not null,
        amount int not null,
        created_at timestamp not null,
        updated_at timestamp not null
        )""")
        db_prep_cmds.append(f"""create table {schema}.customers (
        id {descriptor},
        email varchar(256) not null,
        phone_number varchar(50) null,
        date_of_birth date null,
        created_at timestamp not null,
        updated_at timestamp not null
        )""")
        db_prep_cmds.append(f"""create table {schema}.addresses (
        id {descriptor},
        address_line varchar(256) not null,
        city varchar(256) not null,
        state_cd varchar(2) not null,
        zip_code varchar(10) not null,
        created_at timestamp not null,
        updated_at timestamp not null
        )""")
        db_prep_cmds.append(f"""create table {schema}.items (
        id {descriptor},
        item_type_id int not null,
        description varchar(256) not null,
        price int not null,
        created_at timestamp not null,
        updated_at timestamp not null
        )""")
        db_prep_cmds.append(f"""create table {schema}.refunds (
        id {descriptor},
        order_id int not null,
        amount int not null,
        created_at timestamp not null,
        updated_at timestamp not null
        )""")

    # order_items, items, item_types, subscriptions
    with connection.cursor() as cursor:
        for cmd in db_prep_cmds:
            cursor.execute(cmd)
        connection.commit()


def nullable_field(value: Any) -> Any:
    """
    Takes in a value (i.e. from Faker) and returns it or None randomly
    :param value: Value to possibly return
    :return: Passed value or None
    """
    if randint(0, 2) < 1:
        value = None

    return value


def random_amount(max_value: int = 100000) -> int:
    """
    Used to get a random dollar value ( * 100) and can have a max (e.g. for refunds)
    :param max_value: The maximum value allowed, useful for refunds
    :return: the amount to be used
    """
    return randint(1, max_value)


def create_rand_data(func: Callable, connection: Connection, records: int) -> List[int]:
    """
    Wrapper for calling data creation functions a count of times. More DRY than iterating within each function
    :param func: The data creation function to call
    :param connection: Connection to pass to data creation function
    :param records: number of times to call
    :return:
    """
    ids = []
    for _ in range(0, records):
        ids.append(func(connection))
    return ids


def create_rand_address(connection: Connection) -> int:
    """
    Ideally used in a transaction block, writes a address record and returns the new id
    :param connection: Connection to write records with
    :return: id of inserted record
    """
    fake = Faker()
    matches = None
    while not matches:
        address = fake.address()
        matches = re.search('([\w\s\.]*)\n([\w\s]*), (\w{2}) ([\d-]*)', address)
    address_line, city, state_cd, zip_code = matches.groups()
    now = datetime.now(tz=timezone.utc)
    query = """insert into source.addresses (
    address_line,
    city,
    state_cd,
    zip_code,
    created_at,
    updated_at
    )
    values (%s, %s, %s, %s, %s, %s)
    returning id"""

    with connection.cursor() as cursor:
        cursor.execute(query, [address_line, city, state_cd, zip_code, now, now])
        pk = cursor.fetchone()
        connection.commit()
    return pk[0]


def create_rand_customer(connection: Connection) -> int:
    """
    Ideally used in a transaction block, writes a customer record and returns the new id
    :param connection: Connection to write records with
    :return: id of inserted record
    """
    fake = Faker()
    email = fake.email()
    dob = nullable_field(fake.date_of_birth())
    phone = nullable_field(fake.phone_number())
    now = datetime.now(tz=timezone.utc)
    query = """insert into source.customers (
    email,
    phone_number,
    date_of_birth,
    created_at,
    updated_at
    )
    values (%s, %s, %s, %s, %s)
    returning id"""

    with connection.cursor() as cursor:
        cursor.execute(query, [email, phone, dob, now, now])
        pk = cursor.fetchone()
        connection.commit()
    return pk[0]


def create_rand_order(connection: Connection) -> int:
    """
    Ideally used in a transaction block, writes an order record and returns the new id
    :param connection: Connection to write records with
    :return: id of inserted record
    """
    # since we're adding new customers, we can use prior runs to get a 'new customer'
    find_customer = """
    select  id
    from    source.customers
    order by id desc
    limit 100
    """
    with connection.cursor() as cursor:
        cursor.execute(find_customer)
        customers = cursor.fetchall()
    if len(customers) == 0:
        return
    customer_id = customers[randint(0, len(customers) - 1)][0]
    # logically the customer would reuse their address or use a new one
    get_last_address = f"""
    select  shipping_address_id
    from    source.orders
    where customer_id = {customer_id}
    limit 1"""
    with connection.cursor() as cursor:
        cursor.execute(get_last_address)
        shipping_address_id = cursor.fetchall()
    print(shipping_address_id)
    # if there's no address, get a new one
    if not shipping_address_id:
        shipping_address_id = create_rand_address(connection)
    else:
        shipping_address_id = shipping_address_id[0]
    billing_address_id = shipping_address_id
    # rarely the 2 addresses will not align
    if randint(0, 10) < 1:
        billing_address_id = create_rand_address(connection)
    amount = random_amount()
    now = datetime.now(tz=timezone.utc)
    query = """insert into source.orders (
    customer_id,
    shipping_address_id,
    billing_address_id,
    amount,
    created_at,
    updated_at
    )
    values (%s, %s, %s, %s, %s, %s)
    returning id"""

    with connection.cursor() as cursor:
        cursor.execute(query, [customer_id, shipping_address_id, billing_address_id, amount, now, now])
        pk = cursor.fetchone()
        connection.commit()
    return pk[0]


def create_rand_refund(connection: Connection) -> int | None:
    """
    Ideally used in a transaction block, finds an old order to refund and refunds up to 100% of the unrefunded amount
    :param connection: Connection to write records with
    :return: id of inserted record
    """
    find_orders = """
    select o.id, o.amount, o.amount - sum(coalesce(r.amount, 0)) as refundable
    from source.orders o
    left join source.refunds r
    on o.id = r.order_id
    group by o.id
    having sum(coalesce(r.amount, 0)) < o.amount
    limit 100"""
    with connection.cursor() as cursor:
        cursor.execute(find_orders)
        refundables = cursor.fetchall()
    if len(refundables) == 0:
        return
    refundable_order = refundables[randint(0, len(refundables) - 1)]
    # use default for random_amount to have higher likelihood of full refunds (more common IRL)
    refund_amount = min(refundable_order[2], random_amount())
    now = datetime.now(tz=timezone.utc)
    query = """insert into source.refunds (
    order_id,
    amount,
    created_at,
    updated_at
    )
    values (%s, %s, %s, %s)
    returning id"""

    with connection.cursor() as cursor:
        cursor.execute(query, [refundable_order[0], refund_amount, now, now])
        pk = cursor.fetchone()
        connection.commit()
    return pk[0]

"""
if __name__ == '__main__':
    # init_database()
    random_write(create_rand_address)
    random_write(create_rand_customer)
    random_write(create_rand_order)
    random_write(create_rand_refund)
    with eng.connect() as connection:
        resp = conn.execute(text('select id from source.customers'))
        print(resp.fetchall())
        resp = conn.execute(text('select id from source.addresses'))
        print(resp.fetchall())
        resp = conn.execute(text('select id from source.orders'))
        print(resp.fetchall())
        resp = conn.execute(text('select id from source.refunds'))
        print(resp.fetchall())
"""