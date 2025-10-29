from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from plugins.postgres_operators import PostgresOperators
import pandas as pd
import logging

def load_m_product_full():

    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")

    

    df = staging_operator.get_data_to_pd("""
                                        SELECT
                                            m.m_product_id, 
                                            COALESCE(u.c_uom_sk, -1) as c_uom_sk,
                                            COALESCE(c.c_producttype_sk, -1) as c_producttype_sk, 
                                            COALESCE(m.name, 'N/A') as name, 
                                            COALESCE(m.value, 'N/A') as value,
                                            m.isactive,
                                            m.created, 
                                            m.updated 
                                            FROM xmcp_staging.m_product m
                                            LEFT JOIN xmcp_dw.dim_c_producttype c on m.c_producttype_id=c.c_producttype_id  AND c.is_current = 1
                                            LEFT JOIN xmcp_dw.dim_c_uom u on m.c_uom_id=u.c_uom_id  AND u.is_current = 1
                                         """)
    

      
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
        table_name="dim_m_product",
        schema="xmcp_dw",
        if_exists="append"
    
    )

def load_m_product_incremental():
    # staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")

    sql="""
        UPDATE xmcp_dw.dim_m_product dw
        SET valid_to=NOW(),
            is_current=0
        FROM xmcp_staging.m_product stg
        WHERE stg.m_product_id = dw.m_product_id
            AND dw.is_current = 1
            AND (dw.c_uom_id IS DISTINCT FROM stg.c_uom_id
                OR dw.c_producttype_id IS DISTINCT FROM stg.c_producttype_id
                OR dw.name        IS DISTINCT FROM stg.name
                OR dw.value       IS DISTINCT FROM stg.value
                OR dw.isactive IS DISTINCT FROM (CASE stg.isactive WHEN 'Y' THEN 1 ELSE 0 END));

        
        INSERT INTO xmcp_dw.dim_m_product (
            m_product_id,
            c_uom_id,
            c_producttype_id,
            name,
            value,
            isactive,
            created,
            updated,
            valid_from,
            valid_to,
            is_current
        )
        SELECT 
                stg.m_product_id,
                COALESCE(stg.c_uom_id, -1),
                COALESCE(stg.c_producttype_id, -1),
                COALESCE(stg.name, 'N/A'),
                COALESCE(stg.value, 'N/A'),
                CASE stg.isactive WHEN 'Y' THEN 1 ELSE 0 END,
                stg.created,
                stg.updated,
                NOW(),
                '9999-12-31 23:59:59',
                1
        FROM xmcp_staging.m_product stg
        LEFT JOIN xmcp_dw.dim_m_product dw
            ON stg.m_product_id = dw.m_product_id
            AND dw.is_current = 1
        WHERE dw.m_product_id IS NULL
            OR COALESCE(dw.c_uom_id, -1) IS DISTINCT FROM COALESCE(stg.c_uom_id, -1)
            OR COALESCE(dw.c_producttype_id, -1) IS DISTINCT FROM COALESCE(stg.c_producttype_id, -1)
            OR COALESCE(dw.name, 'N/A') IS DISTINCT FROM COALESCE(stg.name, 'N/A')
            OR COALESCE(dw.value, 'N/A') IS DISTINCT FROM COALESCE(stg.value, 'N/A')
            OR dw.isactive IS DISTINCT FROM (CASE stg.isactive WHEN 'Y' THEN 1 ELSE 0 END);
    """

    

    # staging_operator.save_data_to_postgres(
    #     df,
    #     table_name="c_tax",
    #     schema="xmcp_dw",
    #     if_exists="append"
    # )
    dw_operator.run_sql(sql)