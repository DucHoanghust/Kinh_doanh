from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from plugins.oracle_operators import OracleHookThick
from plugins.postgres_operators import PostgresOperators


def extract_c_tax():
    source_operator = OracleHookThick(conn_id="SOURCE_ORACLE")
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    

    df = source_operator.get_data_to_pandas("select c_tax_id, c_taxcategory_id, name, rate, value, isactive, created, updated from c_tax")
    print(df.columns)
    print(df.shape)
    df.columns = [col.lower() for col in df.columns]
    df.columns = [col.lower() for col in df.columns]
    print(df.columns)
    staging_operator.save_data_to_postgres(
        df,
        table_name="c_tax",
        schema="kd_stag",
        if_exists="replace"
    
    )