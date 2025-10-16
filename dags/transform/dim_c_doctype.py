from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from plugins.postgres_operators import PostgresOperators
import pandas as pd
import logging

def load_c_doctype_full():
    
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")

    sql="""
    SELECT c_doctype_id,
        CASE WHEN name='test' OR name = '** New **' THEN 'n/a' ELSE name END,
        CASE 
        WHEN printname = 'abc' OR printname = 'test' OR printname = '** New **' THEN 'n/a' 
        ELSE COALESCE(printname,'n/a') 
        END AS printname,
        COALESCE(docbasetype, 'n/a') as docbasetype, 
        issotrx, 
        isactive,
        created, 
        updated FROM kd_stag.c_doctype
    """

    df = staging_operator.get_data_to_pd(sql)
    logging.info(df.columns)
    # Chuẩn hóa lại is active sang boolean từ Y/N sang 1/0
    df['isactive'] = df['isactive'].map({'Y': 1, 'N': 0})

    # Chuẩn hóa lại issotrx sang boolean từ Y/N sang 1/0
    df['issotrx'] = df['issotrx'].map({'Y': 1, 'N': 0})

    # Thêm Surrogate Key  -- Bước này sẽ tự thêm trong Serial trong sql
    # df['c_tax_sk'] = df.index + 1

    # Xử lí SCD Type 2
    df['valid_from'] = pd.Timestamp.now()
    df['valid_to'] = pd.Timestamp("9999-12-31 23:59:59")
    df['is_current'] = 1
    

    dw_operator.save_data_to_postgres(
        df,
        table_name="dim_c_doctype",
        schema="kd_dw",
        if_exists="append"
    
    )

def load_c_doctype_incremental():
    # staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")

    sql="""
        UPDATE kd_dw.dim_c_doctype dw
        SET valid_to=NOW(),
            is_current=0
        FROM kd_stag.c_doctype stg
        WHERE stg.c_doctype_id = dw.c_doctype_id
            AND dw.is_current = 1
            AND (
                dw.name IS DISTINCT FROM (CASE WHEN stg.name IN ('test','** New **') THEN 'n/a' ELSE stg.name END)
                OR dw.printname IS DISTINCT FROM (CASE WHEN stg.printname IN ('test','** New **') THEN 'n/a' ELSE stg.printname END)
                OR dw.docbasetype IS DISTINCT FROM stg.docbasetype
                OR dw.issotrx IS DISTINCT FROM (CASE stg.issotrx WHEN 'Y' THEN 1 ELSE 0 END)
                OR dw.isactive IS DISTINCT FROM (CASE stg.isactive WHEN 'Y' THEN 1 ELSE 0 END));

        
        INSERT INTO kd_dw.dim_c_doctype (
            c_doctype_id,
            name,
            printname,
            docbasetype,
            issotrx,
            isactive,
            created,
            updated,
            valid_from,
            valid_to,
            is_current
        )
        SELECT 
                stg.c_doctype_id,
                CASE WHEN stg.name IN ('test','** New **') THEN 'n/a' ELSE stg.name END,
                CASE WHEN stg.printname IN ('test','** New **') THEN 'n/a' ELSE stg.printname END,
                COALESCE(stg.docbasetype, 'n/a'),
                CASE stg.issotrx WHEN 'Y' THEN 1 ELSE 0 END,
                CASE stg.isactive WHEN 'Y' THEN 1 ELSE 0 END,
                stg.created,
                stg.updated,
                NOW(),
                '9999-12-31 23:59:59',
                1
        FROM kd_stag.c_doctype stg
        LEFT JOIN kd_dw.dim_c_doctype dw
            ON stg.c_doctype_id = dw.c_doctype_id
            AND dw.is_current = 1
        WHERE dw.c_doctype_id IS NULL
            OR dw.name IS DISTINCT FROM (CASE WHEN stg.name IN ('test','** New **') THEN 'n/a' ELSE stg.name END)
            OR dw.printname IS DISTINCT FROM (CASE WHEN stg.printname IN ('test','** New **') THEN 'n/a' ELSE stg.printname END)
            OR dw.docbasetype IS DISTINCT FROM COALESCE(stg.docbasetype, 'n/a')
            OR dw.issotrx IS DISTINCT FROM (CASE stg.issotrx WHEN 'Y' THEN 1 ELSE 0 END)
            OR dw.isactive IS DISTINCT FROM (CASE stg.isactive WHEN 'Y' THEN 1 ELSE 0 END);

    """

    

    # staging_operator.save_data_to_postgres(
    #     df,
    #     table_name="c_tax",
    #     schema="kd_dw",
    #     if_exists="append"
    # )
    dw_operator.run_sql(sql)