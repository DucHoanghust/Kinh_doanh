from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from plugins.postgres_operators import PostgresOperators
import pandas as pd
import logging

def load_c_bpartner_full():
    
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")

    # INSERT INTO kd_dw.dim_c_bpartner (
    #         c_bpartner_id,
    #         c_bp_group_sk,
    #         name,
    #         value,
    #         isactive,
    #         created,
    #         updated
    #     )

    # "SELECT * FROM kd_stag.c_bpartner"
    sql="""
        
        SELECT 
            sm.c_bpartner_id,
            COALESCE(m.c_bp_group_sk, -1) as c_bp_group_sk,  
            sm.name,
            sm.value,
            sm.isactive,
            sm.created,
            sm.updated
        FROM kd_stag.c_bpartner sm
        LEFT JOIN kd_dw.dim_c_bp_group m
            ON sm.c_bp_group_id = m.c_bp_group_id;
    """
    df = staging_operator.get_data_to_pd(sql)
    logging.info(df.columns)
    
    
    # Thêm Surrogate Key  -- Bước này sẽ tự thêm trong Serial trong sql
    # df['c_tax_sk'] = df.index + 1

    # Xử lí SCD Type 2
    df['valid_from'] = pd.Timestamp.now()
    df['valid_to'] = pd.Timestamp("9999-12-31 23:59:59")
    df['is_current'] = 1
    

    dw_operator.save_data_to_postgres(
        df,
        table_name="dim_c_bpartner",
        schema="kd_dw",
        if_exists="append"
    
    )

def load_c_bpartner_incremental():
    # staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")

    sql="""
        UPDATE kd_dw.dim_c_bpartner dw
        SET valid_to=NOW(),
            is_current=0
        FROM kd_stag.c_bpartner stg
        WHERE stg.c_bpartner_id = dw.c_bpartner_id
            AND dw.is_current = 1
            AND (    
                dw.c_bp_group_id IS DISTINCT FROM stg.c_bp_group_id
                OR dw.name        IS DISTINCT FROM stg.name
                OR dw.value       IS DISTINCT FROM stg.value
                OR dw.isactive IS DISTINCT FROM (CASE stg.isactive WHEN 'Y' THEN 1 ELSE 0 END)
                );

        INSERT INTO kd_dw.dim_c_bpartner (
            c_bpartner_id,
            c_bp_group_id,
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
                stg.c_bpartner_id,
                stg.c_bp_group_id,
                stg.name,
                stg.value,
                CASE stg.isactive WHEN 'Y' THEN 1 ELSE 0 END,
                stg.created,
                stg.updated,
                NOW(),
                '9999-12-31 23:59:59',
                1
        FROM kd_stag.c_bpartner stg
        LEFT JOIN kd_dw.dim_c_bpartner dw
            ON stg.c_bpartner_id = dw.c_bpartner_id
            AND dw.is_current = 1
        WHERE dw.c_bpartner_id IS NULL
            OR dw.c_bp_group_id IS DISTINCT FROM stg.c_bp_group_id
            OR dw.name IS DISTINCT FROM stg.name
            OR dw.value IS DISTINCT FROM stg.value
            OR dw.isactive IS DISTINCT FROM (CASE stg.isactive WHEN 'Y' THEN 1 ELSE 0 END);
    """

    

    # staging_operator.save_data_to_postgres(
    #     df,
    #     table_name="c_tax",
    #     schema="kd_dw",
    #     if_exists="append"
    # )
    dw_operator.run_sql(sql)