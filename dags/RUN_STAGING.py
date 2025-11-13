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
from extract.m_locator import extract_m_locator
from extract.m_warehouse import extract_m_warehouse
from extract.m_inout import extract_m_inout
from extract.m_inoutline import extract_m_inoutline
from extract.m_step import extract_m_step
from extract.m_product_category import extract_m_product_category
###
from extract.c_department import extract_c_department


####################
## EMAIL CALLBACK
####################
def safe_email_alert(context):
    ti = context.get("ti")
    dag_id = ti.dag_id
    task_id = ti.task_id
    run_id = ti.run_id
    log_url = ti.log_url

    html_content = f"""
    <h3>⚠️ Task Failed in Airflow</h3>
    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Task:</b> {task_id}</p>
    <p><b>Run ID:</b> {run_id}</p>
    <p><a href="{log_url}">🔗 Xem log trong Airflow</a></p>
    """

    send_email(
        to=["hoangbd1@viettel.com.vn"],
        subject=f"[Airflow Alert] Task Failed: {task_id}",
        html_content=html_content,
        conn_id="smtp_default"  # ✅ dùng connection SMTP_CONN thay vì mặc định
    )

def success_email_alert(context):
    dag_id = context.get("dag").dag_id
    run_id = context.get("run_id")
    send_email(
        to=["hoangbd1@viettel.com.vn"],
        subject=f"[Airflow Success] DAG {dag_id} completed successfully",
        html_content=f"<p>DAG <b>{dag_id}</b> đã hoàn thành thành công.</p><p>Run ID: {run_id}</p>",
        conn_id="smtp_default"
    )


####################
## DEFAULT ARGS
####################


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "end_date": None,
    "tags": ["full_load", "ETL"],

    ## email
    "email": ["hoangbd1@viettel.com.vn"],
    "email_on_failure": True,
    "on_failure_callback": safe_email_alert,
    "email_on_retry": False,

    ## Timezone
    "timezone": "Asia/Ho_Chi_Minh",   
    }



####################
## DAG DEFINITION
####################

with DAG("STAGING",
        default_args=default_args,
        schedule="0 6 * * *",
        catchup=False,
        tags=["ETL", "STAGING"],
        on_success_callback= success_email_alert,

         ) as dag:
    
    extract_tasks = {
    "E_c_tax": extract_c_tax,
    "E_c_submarket": extract_c_submarket,
    "E_c_market": extract_c_market,
    "E_m_product": extract_m_product,
    "E_c_invoice": extract_c_invoice,
    "E_c_bpartner": extract_c_bpartner,
    "E_c_bp_group": extract_c_bp_group,
    "E_ad_group": extract_ad_org,
    "E_c_uom": extract_c_uom,
    "E_c_producttype": extract_c_producttype,
    "E_c_invoiceline": extract_c_invoiceline,
    "E_c_doctype": extract_c_doctype,
    "E_c_currency": extract_c_currency,

    ## Kho
    "E_m_locator": extract_m_locator,
    "E_m_warehouse": extract_m_warehouse,
    "E_m_product_category": extract_m_product_category,
    "E_m_step": extract_m_step,
    "E_m_inout": extract_m_inout,
    "E_m_inoutline": extract_m_inoutline,
    "E_c_department":extract_c_department
    ##
}

    with TaskGroup("EXTRACT") as extract_group:
        create_table_stag = SQLExecuteQueryOperator(
            task_id="create_stag_tables",
            conn_id="STAGING_POSTGRES",
            sql="./CREATE_STAG.sql"
        )

        extract_task_list = []
        for task_id, fn in extract_tasks.items():
            extract_task_list.append(
                PythonOperator(
                    task_id=task_id,
                    python_callable=fn,
                    dag=dag
                )
            )  

        create_table_stag >> extract_task_list
