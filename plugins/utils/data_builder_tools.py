from typing import Any, Callable
import sqlalchemy.future as sql
from sqlalchemy import text
import re
from datetime import datetime, timezone
from sqlalchemy.engine import Connection
from faker import Faker
from random import randint
from airflow import settings
from airflow.models import Connection

# not worried about sharing creds for local docker db
# TODO: Rebuild as a class


def build_connections() -> None:
    """
    Builds connections representing the data source and the etl target
    :return: None
    """

    # creating connections in Airflow for our ETL processes
    host = 'localhost'
    login = 'postgres'
    password = 'postgrespw'
    port = 55001
    connections = [
        {'conn_id': 'oltp', 'database': 'postgres', 'desc': "represents our prod database, the 'source' schema"},
        {'conn_id': 'ods', 'database': 'postgres', 'desc': "represents anything for DE: etl, stage, target schemas"}
    ]
    for connection in connections:
        n_conn = Connection(
            conn_id=connection['conn_id'],
            conn_type='Postgres',
            host=host,
            login=login,
            password=password,
            port=port,
            schema=connection['database']
        )  # create a connection object
        session = settings.Session()  # get the session
        conn_name = session.query(Connection).filter(Connection.conn_id == n_conn.conn_id).first()
        if not str(conn_name) == str(n_conn.conn_id):
            session.add(n_conn)
            session.commit()  # it will insert the connection object programmatically.


def init_database(connection: Connection) -> None:
    """
    Builds all underlying schemas/tables. Using SERIAL for now for source IDs for ease, may add triggers for
     created/updated cols and foreign key constraints as well to better mimic prod DBs but not necessary
    Not intended to tear down schemas for user to prevent data loss (so a user can't run twice)
    TODO: Revamp to be more DRY, as we have DDL here and column-specific inserts later for faux data creation
    :param connection: Connection to write records with
    :return: None
    """
    db_prep_cmds = [
        'create schema etl;',
        """create table etl.extract_executions (
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


def create_rand_address(connection: Connection) -> int:
    """
    Ideally used in a transaction block, writes a address record and returns the new id
    :param connection: Connection to write records with
    :return: id of inserted record
    """
    fake = Faker()
    address = fake.address()
    matches = re.search('([\w\s\.]*)\n([\w\s]*), (\w{2}) ([\d-]*)', address)
    try:
        address_line, city, state_cd, zip_code = matches.groups()
    except AttributeError as e:
        # TODO: accept deployed military addresses
        print(f'Bad address {address}')
        print(e)
        raise Exception()
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
    customer_id = create_rand_customer(connection)
    shipping_address_id = create_rand_address(connection)
    billing_address_id = shipping_address_id
    if randint(0, 2) < 1:
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
    refund_amount = random_amount(refundable_order[2])
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