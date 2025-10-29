from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from plugins.postgres_operators import PostgresOperators
import pandas as pd
import logging

def load_c_invoice_full():
    
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")

    sql = """
        SELECT  iv.c_invoice_id,
                COALESCE(cb.c_bpartner_sk, -1) as c_bpartner_sk,
                COALESCE(iv.m_product_id, -1) as m_product_id,
                COALESCE(cd.c_doctype_sk,-1) as c_doctype_sk,
                COALESCE(ad.ad_org_id,-1) as ad_org_id,
                COALESCE(cur.c_currency_sk,-1) as c_currency_sk,
                iv.documentno, 
                iv.currencyrate as currency_rate,
                iv.isactive,
                iv.created, 
                iv.updated, 
                COALESCE(iv.dateinvoice, iv.created) as dateinvoice
                
                
        FROM xmcp_staging.c_invoice iv 
        LEFT JOIN xmcp_dw.dim_c_doctype cd on iv.c_doctype_id = cd.c_doctype_id
        LEFT JOIN xmcp_dw.dim_c_bpartner cb on iv.c_bpartner_id = cb.c_bpartner_id
        LEFT JOIN xmcp_dw.dim_ad_org ad on iv.ad_org_id = ad.ad_org_id
        LEFT JOIN xmcp_dw.dim_c_currency cur on iv.c_currency_id = cur.c_currency_id
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
        table_name="dim_c_invoice",
        schema="xmcp_dw",
        if_exists="append"
    
    )

def load_c_invoice_incremental():
    # staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")

    sql="""
        UPDATE xmcp_dw.dim_c_invoice dw
        SET valid_to=NOW(),
            is_current=0
        FROM xmcp_staging.c_invoice stg
        WHERE stg.c_invoice_id = dw.c_invoice_id
            AND dw.is_current = 1
            AND (
                dw.c_bpartner_id IS DISTINCT FROM stg.c_bpartner_id
                OR dw.m_product_id IS DISTINCT FROM stg.m_product_id
                OR dw.c_doctype_id IS DISTINCT FROM stg.c_doctype_id
                OR dw.ad_org_id IS DISTINCT FROM stg.ad_org_id
                OR dw.documentno IS DISTINCT FROM stg.documentno
                OR dw.isactive IS DISTINCT FROM (CASE stg.isactive WHEN 'Y' THEN 1 ELSE 0 END)
                OR dw.dateinvoice IS DISTINCT FROM stg.dateinvoice
            );

        
        INSERT INTO xmcp_dw.dim_c_invoice (
            c_invoice_id,
            c_bpartner_id,
            m_product_id,
            c_doctype_id,
            ad_org_id,
            documentno,
            isactive,
            created,
            updated,
            dateinvoice,
            valid_from,
            valid_to,
            is_current
        )
        SELECT 
                stg.c_invoice_id,
                stg.c_bpartner_id,
                stg.m_product_id,
                stg.c_doctype_id,
                stg.ad_org_id,
                stg.documentno,
                CASE stg.isactive WHEN 'Y' THEN 1 ELSE 0 END,
                stg.created,
                stg.updated,
                stg.dateinvoice,
                NOW(),
                '9999-12-31 23:59:59',
                1
        FROM xmcp_staging.c_invoice stg
        LEFT JOIN xmcp_dw.dim_c_invoice dw
            ON stg.c_invoice_id = dw.c_invoice_id
            AND dw.is_current = 1
        WHERE dw.c_invoice_id IS NULL
            OR dw.c_bpartner_id IS DISTINCT FROM stg.c_bpartner_id
            OR dw.m_product_id IS DISTINCT FROM stg.m_product_id
            OR dw.c_doctype_id IS DISTINCT FROM stg.c_doctype_id
            OR dw.ad_org_id IS DISTINCT FROM stg.ad_org_id
            OR dw.documentno IS DISTINCT FROM stg.documentno
            OR dw.isactive IS DISTINCT FROM (CASE stg.isactive WHEN 'Y' THEN 1 ELSE 0 END)
            OR dw.dateinvoice IS DISTINCT FROM stg.dateinvoice;
    """

    

    # staging_operator.save_data_to_postgres(
    #     df,
    #     table_name="c_tax",
    #     schema="xmcp_dw",
    #     if_exists="append"
    # )
    dw_operator.run_sql(sql)