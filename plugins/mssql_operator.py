from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from sqlalchemy import create_engine

class MsSqlOperators:
    def __init__(self, conn_id):
        self.conn_id = conn_id

    def get_hook(self):
        try:
            return MsSqlHook(mssql_conn_id=self.conn_id)
        except Exception as e:
            print(f"Lỗi lấy Hook: {e}")
            return None

    def get_data_to_pd(self, sql):
        return self.get_hook().get_pandas_df(sql)

    


   