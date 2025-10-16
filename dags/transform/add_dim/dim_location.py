from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from plugins.postgres_operators import PostgresOperators
import pandas as pd
import logging
import os

def load_dim_location():
    
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")

    

    df = staging_operator.get_data_to_pd("SELECT c_market_sk, c_market_id FROM kd_dw.dim_c_market")
    logging.info(df.columns)
    

    dag_folder = os.path.dirname(__file__)  # /opt/airflow/dags/transform/add_dim
    file_path = os.path.join(dag_folder, "data", "lat_long.csv")
    df_csv=pd.read_csv(file_path, header=0)
    logging.info(f"CSV exists: {os.path.exists(file_path)}")
    logging.info(f"Columns from CSV: {df_csv.columns}")


    df_final = pd.merge(df, df_csv, on="c_market_id", how="left")


    dw_operator.save_data_to_postgres(
        df_final,
        table_name="dim_location",
        schema="kd_dw",
        if_exists="append"
    
    )
