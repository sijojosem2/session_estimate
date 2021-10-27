from contextlib import contextmanager
from psycopg2.pool import SimpleConnectionPool
import config

pool = SimpleConnectionPool(minconn=1, maxconn=10, dsn=config.DATABASE_URI)


@contextmanager
def get_connection():
    connection = pool.getconn()

    try:
        yield connection
    finally:
        pool.putconn(connection)
