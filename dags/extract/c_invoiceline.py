
from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from plugins.oracle_operators import OracleHookThick
from plugins.postgres_operators import PostgresOperators


def extract_c_invoiceline():
    source_operator = OracleHookThick(conn_id="SOURCE_ORACLE")
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    
    """
        Trong đó có qtyentered, movementqty là chưa được xác định
    """

    sql= """select 
                il.c_invoiceline_id,
                il.c_invoice_id, 
                il.c_submarket_id, 
                il.m_product_id,
                il.ad_org_id,  
                il.c_uom_id ,
                il.c_tax_id,

                inv.c_bpartner_id,

                il.qtyinvoiced,  
                il.priceactual, 
                il.linenetamt, 
                il.linetotalamt,
                il.linenetamtconvert, 
                il.discount,
                il.discountamt,
                il.discountamtconvert,
                il.discount2amt,
                il.discountamt2convert,
                il.percenttax, 
                il.taxamount,
                il.taxamountconvert, 
                il.grandtotal,
                il.grandtotalconvert,
                il.qtyentered,
                il.movementqty,
                il.isactive,
                il.created,
                il.updated 
            FROM c_invoiceline il
            LEFT JOIN c_invoice inv on il.c_invoice_id = inv.c_invoice_id
            
            """
    
    # df = source_operator.get_data_to_pandas(sql, chunksize=10000)
    # print(df.columns)
    # print(df.shape)
    df = source_operator.get_data_to_pandas_with_chunks(sql, chunksize=20000)

    df.columns = [col.lower() for col in df.columns]
    staging_operator.save_data_to_postgres(
        df,
        table_name="c_invoiceline",
        schema="xmcp_staging",
        if_exists="replace"
    
    )