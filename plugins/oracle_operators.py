# plugins/oracle_operator.py
from airflow.hooks.base import BaseHook
import oracledb
import pandas as pd
import logging

class OracleHookThick:
    """Hook kết nối Oracle thick mode để extract data"""
    def __init__(self, conn_id: str, lib_dir="/opt/oracle/instantclient_19_28"):
        self.conn_id = conn_id
        self.conn = None
        self.lib_dir = lib_dir

    def get_conn(self):
        if self.conn:
            return self.conn
        conn_info = BaseHook.get_connection(self.conn_id)
        oracledb.init_oracle_client(lib_dir=self.lib_dir)
        self.conn = oracledb.connect(
            user=conn_info.login,
            password=conn_info.password,
            dsn=f"{conn_info.host}:{conn_info.port}/orcl.ximangcampha.net"
        )
        return self.conn
    # oracle+cx_oracle://xmcp:Admin123@10.0.0.10:1521/?service_name=orcl.ximangcampha.net

    def fetchall(self, sql, parameters=None):
        conn = self.get_conn()
        with conn.cursor() as cur:
            if parameters:
                cur.execute(sql, parameters)
            else:
                cur.execute(sql)
            return cur.fetchall()

    def get_data_to_pandas(self, sql, parameters=None,chunksize=None):
        """Trả về DataFrame trực tiếp"""
        conn = self.get_conn()
        df = pd.read_sql(sql, conn, params=parameters, chunksize=chunksize)
        return df
    
    def get_data_to_pandas_with_chunks(self, sql, parameters=None, chunksize=None):
        """Trả về DataFrame với chunksize"""
        conn = self.get_conn()
        chunks = pd.read_sql(sql, conn, params=parameters, chunksize=chunksize)
        df= pd.concat([chunk for chunk in chunks], ignore_index=True)


        logging.info(f"Loaded DataFrame row count: {df.shape[0]}")
        logging.info(f"Loaded DataFrame columns count: {df.shape[1]} ")

        return df