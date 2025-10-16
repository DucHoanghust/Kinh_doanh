from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from plugins.postgres_operators import PostgresOperators
import pandas as pd
import logging

def load_hr_employee_full():
    
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")



    sql="""
        SELECT 
            hr.hr_employee_id,
            COALESCE(d.c_department_sk, -1) as c_department_sk,
            COALESCE(hr.name, 'n/a') as name,
            COALESCE(hr.value, 'n/a') as value,
            hr.gender as gender,
            COALESCE(hr.date_birth, 'n/a') as date_birth,
            COALESCE(hr.mobile, 'n/a') as mobile,
            hr.isactive as isactive,
            hr.created as created,
            hr.updated as updated
        FROM kd_stag.hr_employee hr
        LEFT JOIN kd_dw.dim_c_department d
            ON hr.c_department_id = d.c_department_id;
    """



    df = staging_operator.get_data_to_pd(sql)
    logging.info(df.columns)
    # Chuẩn hóa lại is active sang boolean từ Y/N sang 1/0
    df['isactive'] = df['isactive'].map({'Y': 1, 'N': 0})
    # Chuẩn hóa Na/Nu thành Nam Nữ
    df['gender'] = df['gender'].map({'Na': 'Nam', 'Nu': 'Nữ'})

    
    # Thêm Surrogate Key  -- Bước này sẽ tự thêm trong Serial trong sql
    # df['c_tax_sk'] = df.index + 1

    # Xử lí SCD Type 2
    df['valid_from'] = pd.Timestamp.now()
    df['valid_to'] = pd.Timestamp("9999-12-31 23:59:59")
    df['is_current'] = 1
    

    dw_operator.save_data_to_postgres(
        df,
        table_name="dim_hr_employee",
        schema="kd_dw",
        if_exists="append"
    
    )

def load_ad_org_incremental():
    # staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")

    sql="""
        UPDATE kd_dw.dim_ad_org dw
        SET valid_to=NOW(),
            is_current=0
        FROM kd_stag.ad_org stg
        WHERE stg.ad_org_id = dw.ad_org_id
            AND dw.is_current = 1
            AND (dw.name        IS DISTINCT FROM stg.name
                OR dw.value       IS DISTINCT FROM stg.value
                OR dw.isactive IS DISTINCT FROM (CASE stg.isactive WHEN 'Y' THEN 1 ELSE 0 END));

        
        INSERT INTO kd_dw.dim_ad_org (
            ad_org_id,
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
                stg.ad_org_id,
                stg.name,
                stg.value,
                CASE stg.isactive WHEN 'Y' THEN 1 ELSE 0 END,
                stg.created,
                stg.updated,
                NOW(),
                '9999-12-31 23:59:59',
                1
        FROM kd_stag.ad_org stg
        LEFT JOIN kd_dw.dim_ad_org dw
            ON stg.ad_org_id = dw.ad_org_id
            AND dw.is_current = 1
        WHERE dw.ad_org_id IS NULL
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