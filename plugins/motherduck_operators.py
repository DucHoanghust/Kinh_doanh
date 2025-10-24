import duckdb
import pandas as pd
from airflow.hooks.base import BaseHook
import logging

class MotherduckOperators:
    def __init__(self, conn_id):
        self.conn_id = conn_id

    def _get_token(self):
        """
        Lấy token từ Airflow Connection (Extra JSON field: motherduck_token)
        """
        conn = BaseHook.get_connection(self.conn_id)
        extra = conn.extra_dejson
        token = extra.get("motherduck_token", None)
        if not token:
            raise ValueError(f"No 'motherduck_token' found in connection {self.conn_id}")
        return token

    def _connect(self):
        """
        Kết nối đến MotherDuck (DuckDB cloud)
        """
        token = self._get_token()
        con = duckdb.connect(f"md:kd_dw_cloud?motherduck_token={token}")
        return con

    # ------- giống hệt PostgresOperators -------
    def get_data_to_pd(self, sql):
        """
        Query DuckDB → trả về pandas DataFrame
        """
        con = self._connect()
        try:
            df = con.execute(sql).fetchdf()
            return df
        finally:
            con.close()

    def save_data_to_motherduck(self, df, table_name, schema, if_exists="append"):
        """
        df → MotherDuck.DuckDB table
        """
        con = self._connect()
        try:
            con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

            if if_exists == "replace":
                con.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}"')

            con.register("df_src", df)
            con.execute(f'CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" AS SELECT * FROM df_src')
            
            if if_exists == "append":
                con.execute(f'INSERT INTO "{schema}"."{table_name}" SELECT * FROM df_src')
            elif if_exists == "replace":
                pass

            con.unregister("df_src")
        finally:
            con.close()

    def execute_query(self, sql):
        """
        Giống PostgresOperators
        """
        con = self._connect()
        try:
            con.execute(sql)
        finally:
            con.close()

    def run_sql(self, sql: str, autocommit=False, parameters=None):
        """
        Giữ signature y chang PostgresOperators
        """
        if parameters:
            sql = sql % parameters

        con = self._connect()
        try:
            con.execute(sql)
        finally:
            con.close()
