import db_exec
from db_conn_pool import get_connection



class DbOps:

    def __init__(self,name='DbOps'):
        self.name = name

    def __repr__(self) -> str:
        return f"{self.name}"

    def insert_records(self, df, **kwargs):
        with get_connection() as conn:
            db_exec.insert_data(conn, df, kwargs["target_table"]["name"])
            return

    def execute_sql_file(self,**kwargs):
        with get_connection() as conn:
            db_exec.execute_sql(conn, kwargs["exec_sql"]["name"])
            return


    @classmethod
    def execute_console_qry(cls,input) -> "DbOps":
        with get_connection() as conn:
            result = db_exec.execute_sql_qry(conn, input)
            return cls(result)

    def write_logs_to_db(self,**kwargs):
        pass