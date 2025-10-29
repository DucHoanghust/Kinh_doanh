from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from plugins.postgres_operators import PostgresOperators
import pandas as pd
import logging

def load_m_inoutline_full():
    
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")
    # Sửa lại load sql
    

    df = staging_operator.get_data_to_pd("""SELECT * FROM xmcp_staging.m_inoutline""")
    logging.info(df.columns)
    logging.info("🔄 Start full load fact_m_inoutline")
    
    # Các trường thừa đã bỏ
    # qtyentered,
    # movementqty,

    sql="""
        INSERT INTO xmcp_dw.fact_m_inoutline (
            m_inoutline_id,
            c_invoice_sk,
            c_submarket_sk,
            m_product_sk,
            ad_org_sk,
            c_uom_sk,
            c_tax_sk,
            date_sk,
            c_bpartner_sk,

            movementtype,
            
            qtyinvoiced,
            priceactual,
            
            linenetamt,
            linenetamtconvert,

            discount,
            discountamt,
            discountamtconvert,

            discount2amt,
            discountamt2convert,

            percenttax,
            taxamount,
            taxamountconvert,
            
            grandtotal,
            grandtotalconvert

        )
        SELECT 
            ci.m_inoutline_id,

            COALESCE(inv.c_invoice_sk, -1),
            COALESCE(m.c_submarket_sk, -1),
            COALESCE(p.m_product_sk, -1),
            COALESCE(org.ad_org_sk, -1),
            COALESCE(u.c_uom_sk, -1),
            COALESCE(t.c_tax_sk, -1),

            COALESCE(d.date_sk, -1),

            COALESCE(bg.c_bpartner_sk, -1),

            ci.qtyinvoiced,
            ci.priceactual,

            ci.linenetamt,
            ci.linenetamtconvert,

           
            COALESCE(ci.discount, 0),
            COALESCE(ci.discountamt, 0),
            COALESCE(ci.discountamtconvert, 0),

            COALESCE(ci.discount2amt, 0),
            COALESCE(ci.discountamt2convert, 0),

            
            COALESCE(ci.percenttax, 0),
            COALESCE(ci.taxamount, 0),
            COALESCE(ci.taxamountconvert, 0),

            ci.grandtotal,
            ci.grandtotalconvert

        FROM xmcp_staging.m_inoutline ci
        LEFT JOIN xmcp_dw.dim_c_invoice inv ON ci.c_invoice_id = inv.c_invoice_id

        LEFT JOIN xmcp_dw.dim_c_submarket m ON ci.c_submarket_id = m.c_submarket_id
        LEFT JOIN xmcp_dw.dim_m_product p ON ci.m_product_id = p.m_product_id
        LEFT JOIN xmcp_dw.dim_ad_org org ON ci.ad_org_id = org.ad_org_id
        LEFT JOIN xmcp_dw.dim_c_uom u ON ci.c_uom_id = u.c_uom_id
        LEFT JOIN xmcp_dw.dim_c_tax t ON ci.c_tax_id = t.c_tax_id

        LEFT JOIN xmcp_dw.dim_date d ON inv.dateinvoice::DATE = d.full_date

        LEFT JOIN xmcp_dw.dim_c_bpartner bg ON ci.c_bpartner_id = bg.c_bpartner_id;
    """

    dw_operator.run_sql(sql)
