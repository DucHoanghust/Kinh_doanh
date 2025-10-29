from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.utils.email import send_email


####
from dags.transform.dim_c_currency import load_c_currency_full
from extract.c_tax import extract_c_tax ### Cần sửa lại
from extract.c_submarket import extract_c_submarket
from extract.c_market import extract_c_market
from extract.m_product import extract_m_product
from extract.c_invoice import extract_c_invoice
from extract.c_bpartner import extract_c_bpartner
from extract.c_bp_group import extract_c_bp_group
from extract.ad_org import extract_ad_org
from extract.c_uom import extract_c_uom
from extract.c_producttype import extract_c_producttype
from extract.c_invoiceline import extract_c_invoiceline
from extract.c_doctype import extract_c_doctype
from extract.c_currency import extract_c_currency
####
from transform.dim_c_tax import load_c_tax_full
from transform.dim_ad_org import load_ad_org_full
from transform.dim_c_bp_group import load_c_bp_group_full
from transform.dim_c_doctype import load_c_doctype_full
from transform.dim_c_market import load_c_market_full
from transform.dim_c_submarket import load_c_submarket_full
from transform.dim_c_uom import load_c_uom_full
from transform.dim_m_product import load_m_product_full
from transform.dim_cbpartner import load_c_bpartner_full
from transform.dim_c_producttype import load_c_producttype_full
from transform.dim_c_invoice import load_c_invoice_full
from transform.dim_date import load_dim_date
from transform.dim_c_currency import load_c_currency_full

## Invoiceline
from transform.fact_c_invoiceline import load_c_invoiceline_full

def safe_email_alert(context):
    ti = context.get("ti")
    # Bỏ qua mark_success_url nếu không có
    if not hasattr(ti, "mark_success_url"):
        ti.mark_success_url = ""  # patch tạm thời
    send_email(to="you@domain.com",
               subject=f"Task Failed: {ti.task_id}",
               html_content="Task failed, check Airflow UI.")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "end_date": None,
    "tags": ["full_load", "ETL"],

    ## email
    "email": ["buihoang123zz@gmail.com"],
    "email_on_failure": True,
    "on_failure_callback": safe_email_alert,
    "email_on_retry": False,

    "timezone": "Asia/Ho_Chi_Minh",   
    }

with DAG("KD_FULL_LOAD",
        default_args=default_args,
        schedule="0 7 * * *",
        catchup=False,
         ) as dag:
    
    with TaskGroup("EXTRACT") as extract_group:
        
        create_table_stag = SQLExecuteQueryOperator(
            task_id="create_stag_tables",
            conn_id="STAGING_POSTGRES",
            sql="./CREATE_STAG.sql"
        )

        E_c_tax = PythonOperator(
            task_id="E_c_tax",
            python_callable=extract_c_tax,
            dag=dag
        )

        E_c_submarket = PythonOperator(
            task_id="E_c_submarket",
            python_callable=extract_c_submarket,
            dag=dag
        )

        E_c_market = PythonOperator(
            task_id="E_c_market",
            python_callable=extract_c_market,
            dag=dag
        )

        E_m_product = PythonOperator(
            task_id="E_m_product",
            python_callable=extract_m_product,
            dag=dag
        )

        E_c_invoice = PythonOperator(
            task_id="E_c_invoice",
            python_callable=extract_c_invoice,
            dag=dag
        )

        E_c_currency = PythonOperator(
            task_id="E_c_currency",
            python_callable=extract_c_currency,
            dag=dag
        )

        E_c_bp_group = PythonOperator(
            task_id="E_c_bp_group",
            python_callable=extract_c_bp_group,
            dag=dag
        )
        E_bpartner = PythonOperator(
            task_id="E_bpartner",
            python_callable=extract_c_bpartner,
            dag=dag
        )

        E_ad_group = PythonOperator(
            task_id="E_ad_group",
            python_callable=extract_ad_org,
            dag=dag
        )

        E_c_uom=PythonOperator(
            task_id="E_c_uom",
            python_callable=extract_c_uom,
            dag=dag
        )

        E_c_producttype = PythonOperator(
            task_id="E_c_producttype",
            python_callable=extract_c_producttype,
            dag=dag
        )

        E_c_invoiceline = PythonOperator(
            task_id="E_c_invoiceline",
            python_callable=extract_c_invoiceline,
            dag=dag
        )

        E_c_doctype=PythonOperator(
            task_id="E_c_doctype",
            python_callable=extract_c_doctype,
            dag=dag
        )

        create_table_stag >> [E_c_tax, E_c_submarket, E_c_market, E_m_product, E_c_invoice, E_bpartner, E_c_bp_group, E_ad_group, E_c_uom, E_c_producttype, E_c_invoiceline, E_c_doctype, E_c_currency]

    with TaskGroup("TRANSFORM") as transform_group:

        create_table_dim = SQLExecuteQueryOperator(
            task_id="create_dimension_tables",
            conn_id="STAGING_POSTGRES",
            sql="./CREATE_DIM_XMCP.sql"
        )

    
        TF_dim_c_tax = PythonOperator(
            task_id="TF_dim_c_tax",
            python_callable=load_c_tax_full,
            dag=dag
        )

        TF_dim_c_producttype = PythonOperator(
            task_id="TF_dim_c_producttype",
            python_callable=load_c_producttype_full,
            dag=dag
        )

        TF_dim_m_product = PythonOperator(
            task_id="TF_dim_m_product",
            python_callable=load_m_product_full,
            dag=dag
        )

        TF_dim_c_uom = PythonOperator(
            task_id="TF_dim_c_uom",
            python_callable=load_c_uom_full,
            dag=dag
        )

        TF_dim_ad_org = PythonOperator(
            task_id="TF_dim_ad_org",
            python_callable=load_ad_org_full,
            dag=dag
        )

        TF_dim_c_bp_group = PythonOperator(
            task_id="TF_dim_c_bp_group",
            python_callable=load_c_bp_group_full,
            dag=dag
        )

        TF_dim_c_bpartner = PythonOperator(
            task_id="TF_dim_bpartner",
            python_callable=load_c_bpartner_full,
            dag=dag
        )

        TF_dim_c_doctype = PythonOperator(
            task_id="TF_dim_c_doctype",
            python_callable=load_c_doctype_full,
            dag=dag
        )
        TF_dim_c_currency = PythonOperator(
            task_id="TF_dim_c_currency",
            python_callable=load_c_currency_full,
            dag=dag
        )

        TF_dim_c_market = PythonOperator(
            task_id="TF_dim_c_market",
            python_callable=load_c_market_full,
            dag=dag
        )

        TF_dim_c_submarket= PythonOperator(
            task_id="TF_dim_c_submarket",
            python_callable=load_c_submarket_full,
            dag=dag
        )

        
        TF_dim_c_invoice = PythonOperator(
            task_id="TF_dim_c_invoice",
            python_callable=load_c_invoice_full,
            dag=dag
        )

        TF_dim_date = PythonOperator(
            task_id="TF_dim_date",
            python_callable=load_dim_date,
            dag=dag
        )
        
        # create_table_dim >> [TF_dim_c_tax, TF_dim_c_market, TF_dim_c_producttype,
        #                         TF_dim_c_bp_group, TF_dim_ad_org,
        #                         TF_dim_c_uom,TF_dim_c_doctype, TF_dim_date] >> [TF_dim_m_product, TF_dim_c_submarket,TF_dim_c_bpartner] >> [TF_dim_c_invoice]

        group1 = [TF_dim_c_tax, TF_dim_c_market, TF_dim_c_producttype,
          TF_dim_c_bp_group, TF_dim_ad_org,TF_dim_c_currency,
          TF_dim_c_uom, TF_dim_c_doctype, TF_dim_date]

        group2 = [TF_dim_m_product, TF_dim_c_submarket, TF_dim_c_bpartner]

        create_table_dim >> group1
        for t in group1:
            t >> group2
        for t in group2:
            t >> TF_dim_c_invoice
    
    # with TaskGroup("ADD_DIM") as added_dim:
    #     TF_dim_location = PythonOperator(
    #             task_id="TF_dim_location",
    #             python_callable=load_dim_location,
    #             dag=dag
    #         )

    with TaskGroup("LOAD") as load_group:

        L_fact_c_invoiceline = PythonOperator(
            task_id="TF_fact_c_invoiceline",
            python_callable=load_c_invoiceline_full,
            dag=dag
        )
    
    with TaskGroup("CREATE_VIEW") as create_view:
        create_view = SQLExecuteQueryOperator(
            task_id="create_view",
            conn_id="DW_POSTGRES",
            sql="./CREATE_VIEW.sql"
        )

extract_group >> transform_group  >> load_group >> create_view

# transform_group >> load_group