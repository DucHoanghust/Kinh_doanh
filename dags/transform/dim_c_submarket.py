from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from plugins.postgres_operators import PostgresOperators
import pandas as pd
import logging

def load_c_submarket_full():
    
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")
    # INSERT INTO xmcp_dw.dim_c_submarket (
    #         c_submarket_id,
    #         c_market_sk,
    #         name,
    #         value,
    #         isactive,
    #         created,
    #         updated
    #     )
    sql="""
        
        SELECT 
            sm.c_submarket_id,
            COALESCE(m.c_market_sk, -1) as c_market_sk,   
            COALESCE(sm.name, 'n/a') as name,
            COALESCE(sm.value,'n/a') as value,
            sm.isactive as isactive,
            sm.created,
            sm.updated
        FROM xmcp_staging.c_submarket sm
        LEFT JOIN xmcp_dw.dim_c_market m
            ON sm.c_market_id = m.c_market_id;
    """

    df = staging_operator.get_data_to_pd(sql)
    logging.info(df.columns)
    
    # Chuẩn hóa lại is active sang boolean từ Y/N sang 1/0
    df['isactive'] = df['isactive'].map({'Y': 1, 'N': 0})
    # Thêm Surrogate Key  -- Bước này sẽ tự thêm trong Serial trong sql
    # df['c_tax_sk'] = df.index + 1

    # Xử lí SCD Type 2
    df['valid_from'] = pd.Timestamp.now()
    df['valid_to'] = pd.Timestamp("9999-12-31 23:59:59")
    df['is_current'] = 1
    

    dw_operator.save_data_to_postgres(
        df,
        table_name="dim_c_submarket",
        schema="xmcp_dw",
        if_exists="append"
    
    )

def load_c_submarket_incremental():
    # staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")

    sql="""
        UPDATE xmcp_dw.dim_c_submarket dw
        SET valid_to=NOW(),
            is_current=0
        FROM xmcp_staging.c_submarket stg
        WHERE stg.c_submarket_id = dw.c_submarket_id
            AND dw.is_current = 1
            AND (                 
                    dw.c_market_id IS DISTINCT FROM stg.c_market_id
                OR dw.name        IS DISTINCT FROM stg.name
                OR dw.value       IS DISTINCT FROM stg.value
                );

        INSERT INTO xmcp_dw.dim_c_submarket (
            c_submarket_id,
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
                stg.c_submarket_id,
                stg.c_market_id,
                stg.name,
                stg.value,
                stg.created,
                stg.updated,
                NOW(),
                '9999-12-31 23:59:59',
                1
        FROM xmcp_staging.c_submarket stg
        LEFT JOIN xmcp_dw.dim_c_submarket dw
            ON stg.c_submarket_id = dw.c_submarket_id
            AND dw.is_current = 1
        WHERE dw.c_submarket_id IS NULL
            OR dw.name IS DISTINCT FROM stg.name
            OR dw.value IS DISTINCT FROM stg.value
            OR dw.c_market_id IS DISTINCT FROM stg.c_market_id;
    """

    

    # staging_operator.save_data_to_postgres(
    #     df,
    #     table_name="c_tax",
    #     schema="xmcp_dw",
    #     if_exists="append"
    # )
    dw_operator.run_sql(sql)