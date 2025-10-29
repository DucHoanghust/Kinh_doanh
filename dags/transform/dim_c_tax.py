from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from plugins.postgres_operators import PostgresOperators
import pandas as pd
import logging

def load_c_tax_full():
    
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")

    

    df = staging_operator.get_data_to_pd("SELECT * FROM xmcp_staging.c_tax")
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
        table_name="dim_c_tax",
        schema="xmcp_dw",
        if_exists="append"
    
    )

def load_c_tax_incremental():
    # staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")

    sql="""
        UPDATE xmcp_dw.dim_c_tax dw
        SET valid_to=NOW(),
            is_current=0
        FROM xmcp_staging.c_tax stg
        WHERE stg.c_tax_id = dw.c_tax_id
            AND dw.is_current = 1
            AND (dw.name        IS DISTINCT FROM stg.name
                OR dw.rate        IS DISTINCT FROM stg.rate
                OR dw.value       IS DISTINCT FROM stg.value
                OR dw.isactive IS DISTINCT FROM (CASE stg.isactive WHEN 'Y' THEN 1 ELSE 0 END)
                OR dw.c_taxcategory_id IS DISTINCT FROM stg.c_taxcategory_id);

        
        INSERT INTO xmcp_dw.dim_c_tax (
            c_tax_id,
            c_taxcategory_id,
            name,
            rate,
            value,
            isactive,
            created,
            updated,
            valid_from,
            valid_to,
            is_current
        )
        SELECT 
                stg.c_tax_id,
                stg.c_taxcategory_id,
                stg.name,
                stg.rate,
                stg.value,
                CASE stg.isactive WHEN 'Y' THEN 1 ELSE 0 END,
                stg.created,
                stg.updated,
                NOW(),
                '9999-12-31 23:59:59',
                1
        FROM xmcp_staging.c_tax stg
        LEFT JOIN xmcp_dw.dim_c_tax dw
            ON stg.c_tax_id = dw.c_tax_id
            AND dw.is_current = 1
        WHERE dw.c_tax_id IS NULL
            OR dw.name IS DISTINCT FROM stg.name
            OR dw.rate IS DISTINCT FROM stg.rate
            OR dw.value IS DISTINCT FROM stg.value
            OR dw.isactive IS DISTINCT FROM (CASE stg.isactive WHEN 'Y' THEN 1 ELSE 0 END)
            OR dw.c_taxcategory_id IS DISTINCT FROM stg.c_taxcategory_id;
    """

    

    # staging_operator.save_data_to_postgres(
    #     df,
    #     table_name="c_tax",
    #     schema="xmcp_dw",
    #     if_exists="append"
    # )
    dw_operator.run_sql(sql)