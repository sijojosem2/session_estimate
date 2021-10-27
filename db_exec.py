import io
import os
from tabulate import tabulate
from contextlib import contextmanager


import config

SQL_DIR = config.SQL_DIR


@contextmanager
def get_cursor(conn):
    with conn:
        with conn.cursor() as cursor:
            yield cursor


def insert_data(conn, df, table_name):
    with get_cursor(conn) as cursor:
        f = io.StringIO()
        df.to_csv(f, sep="\t", header=False, index=False)
        f.seek(0)
        cursor.copy_from(f, table_name, sep="\t", null="")
        return


def execute_sql(conn, scriptname):
    with get_cursor(conn) as cursor:
        file_path = os.path.relpath("{}/{}".format(SQL_DIR, scriptname), os.path.dirname(__file__))
        with open(file_path) as ddl_sql:
            cursor.execute(ddl_sql.read())
            return


def execute_sql_qry(conn, input):
    with get_cursor(conn) as cursor:
        cursor.execute(input)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return (tabulate(result, headers=columns , tablefmt='psql'))


def delete_data(conn, df, table_name):
    pass


def update(conn, df, table_name):
    pass


def incremental_insert(conn, df, table_name):
    pass
