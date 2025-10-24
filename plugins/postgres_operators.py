from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
import logging
import pandas as pd

class PostgresOperators:
    def __init__(self, conn_id):
        self.conn_id = conn_id

    def get_hook(self):
        return PostgresHook(postgres_conn_id=self.conn_id)

    def get_connection(self):
        return self.get_hook().get_conn()
        

    def get_data_to_pd(self, sql):
        return self.get_hook().get_pandas_df(sql)

    def save_data_to_postgres(self, df, table_name, schema, if_exists):
        uri = self.get_hook().get_uri()
        engine = create_engine(uri)
        df.to_sql(table_name, engine, schema=schema, if_exists=if_exists, index=False)


    def execute_query(self, sql):
        self.get_hook().run(sql)

    def run_sql(self, sql: str, autocommit=False, parameters=None):
        self.get_hook().run(sql, autocommit=autocommit, parameters=parameters)
