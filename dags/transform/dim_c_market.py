from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from plugins.postgres_operators import PostgresOperators
import pandas as pd
import logging
import os

def load_c_market_full():
    
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")

    

    df = staging_operator.get_data_to_pd("SELECT * FROM xmcp_staging.c_market")
    logging.info(df.columns)
    

    dag_folder = os.path.dirname(__file__)  # /opt/airflow/dags/transform/
    file_path = os.path.join(dag_folder, "data", "lat_long.csv")
    df_csv=pd.read_csv(
        file_path, 
        usecols=["c_market_id","name_chuan","lat","long"],
        header=0
        )
    logging.info(f"CSV exists: {os.path.exists(file_path)}")
    logging.info(f"Columns from CSV: {df_csv.columns}")


    df_final = pd.merge(df, df_csv, on="c_market_id", how="left")

    
    # Thêm Surrogate Key  -- Bước này sẽ tự thêm trong Serial trong sql
    # df['c_tax_sk'] = df.index + 1
    
    # Xử lí SCD Type 2
    df_final['valid_from'] = pd.Timestamp.now()
    df_final['valid_to'] = pd.Timestamp("9999-12-31 23:59:59")
    df_final['is_current'] = 1
    

    dw_operator.save_data_to_postgres(
        df_final,
        table_name="dim_c_market",
        schema="xmcp_dw",
        if_exists="append"
    
    )

def load_c_market_incremental():
    # staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")

    sql="""
        UPDATE xmcp_dw.dim_c_market dw
        SET valid_to=NOW(),
            is_current=0
        FROM xmcp_staging.c_market stg
        WHERE stg.c_market_id = dw.c_market_id
            AND dw.is_current = 1
            AND (                 
                dw.name        IS DISTINCT FROM stg.name
                OR dw.value       IS DISTINCT FROM stg.value
                );

        INSERT INTO xmcp_dw.dim_c_market (
            c_market_id,
            name,
            value,
            created,
            updated,
            valid_from,
            valid_to,
            is_current
        )
        SELECT 
                stg.c_market_id,
                stg.name,
                stg.value,
                stg.created,
                stg.updated,
                NOW(),
                '9999-12-31 23:59:59',
                1
        FROM xmcp_staging.c_market stg
        LEFT JOIN xmcp_dw.dim_c_market dw
            ON stg.c_market_id = dw.c_market_id
            AND dw.is_current = 1
        WHERE dw.c_market_id IS NULL
            OR dw.name IS DISTINCT FROM stg.name
            OR dw.value IS DISTINCT FROM stg.value;
    """

    

    # staging_operator.save_data_to_postgres(
    #     df,
    #     table_name="c_tax",
    #     schema="xmcp_dw",
    #     if_exists="append"
    # )
    dw_operator.run_sql(sql)