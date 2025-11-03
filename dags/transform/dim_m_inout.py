from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from plugins.postgres_operators import PostgresOperators
import pandas as pd
import logging

def load_m_inout_full():

    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")


    df = staging_operator.get_data_to_pd("""
    SELECT
        m.m_inout_id as m_inout_id,
        COALESCE(c.c_department_sk,-1) as c_department_sk,
        COALESCE(d.c_doctype_sk,-1) as c_doctype_sk,
        COALESCE(ct.c_department_sk,-1) as c_department_create_sk,
        COALESCE(p.c_bpartner_sk,-1) as c_bpartner_sk,
        
                                        
        CASE
            WHEN position('+' in m.movementtype) > 0 THEN 'Import'
            WHEN position('-' in m.movementtype) > 0 THEN 'Export'
            ELSE 'n/a'
        END AS inout_type,

        m.documentno,
        m.currencyrate,
        m.movementtype,
        m.docstatus,
        
        m.register_status,
        m.isinvoiced,
        m.isprinted,
        m.isactive,
        m.created,
        m.updated
    from xmcp_staging.m_inout m
    left join xmcp_dw.dim_c_department c on c.c_department_id=m.c_department_id 
    left join xmcp_dw.dim_c_department ct on ct.c_department_id = m.c_department_create_id
    left join xmcp_dw.dim_c_doctype d on d.c_doctype_id=m.c_doctype_id
    left join xmcp_dw.dim_c_bpartner p on p.c_bpartner_id = m.c_bpartner_id 
  
""")
    

    logging.info(df.columns)
    # Chuẩn hóa lại is active sang boolean từ Y/N sang 1/0
    df['isactive'] = df['isactive'].map({'Y': 1, 'N': 0})
       
    df['isinvoiced'] = df['isinvoiced'].map({'Y': 1, 'N': 0})
    df['isprinted'] = df['isprinted'].map({'Y': 1, 'N': 0})

    # Xử lí SCD Type 2
    df['valid_from'] = pd.Timestamp.now()
    df['valid_to'] = pd.Timestamp("9999-12-31 23:59:59")
    df['is_current'] = 1
    

    dw_operator.save_data_to_postgres(
        df,
        table_name="dim_m_inout",
        schema="xmcp_dw",
        if_exists="append"
    
    )
