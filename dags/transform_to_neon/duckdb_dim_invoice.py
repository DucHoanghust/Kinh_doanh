from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from plugins.postgres_operators import PostgresOperators
import logging

def test_neon_connection():
    
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")
    neon = PostgresOperators(conn_id="NEON")

    df = dw_operator.get_data_to_pd("SELECT * FROM kd_dw.dim_ad_org")
    logging.info(df.columns)

    neon.run_sql("""
        CREATE SCHEMA IF NOT EXISTS kd_dw;
        CREATE TABLE IF NOT EXISTS kd_dw.dim_ad_org (
            ad_org_sk SERIAL PRIMARY KEY,
            ad_org_id INT,
            name VARCHAR(255),
            value VARCHAR(255),
            isactive VARCHAR(2) NOT NULL,
            created TIMESTAMP,
            updated TIMESTAMP,
            valid_from TIMESTAMP,
            valid_to TIMESTAMP,
            is_current INT

);""")
    neon.save_data_to_postgres(
        df,
        table_name="dim_ad_org",
        schema="kd_dw",
        if_exists="append"
    
    )

with DAG(
    dag_id="test_neon_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "neon"],
) as dag:

    test_task = PythonOperator(
        task_id="test_connection",
        python_callable=test_neon_connection,
    )
