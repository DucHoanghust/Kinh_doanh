
from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from plugins.oracle_operators import OracleHookThick
from plugins.postgres_operators import PostgresOperators


def extract_c_invoice():
    source_operator = OracleHookThick(conn_id="SOURCE_ORACLE")
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    

    df = source_operator.get_data_to_pandas("""select 
                                                        c_invoice_id,
                                                        c_bpartner_id,
                                                        m_product_id,
                                                        c_doctype_id,
                                                        c_currency_id,
                                                        ad_org_id,
                                                        documentno, 
                                                        currencyrate,
                                                        isactive, 
                                                        created, 
                                                        updated, 
                                                        dateinvoice 
                                            from c_invoice""")
    print(df.columns)
    print(df.shape)
    df.columns = [col.lower() for col in df.columns]
    staging_operator.save_data_to_postgres(
        df,
        table_name="c_invoice",
        schema="xmcp_staging",
        if_exists="replace"
    
    )