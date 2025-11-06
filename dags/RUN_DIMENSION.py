from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.utils.email import send_email


####
####
from transform.dim_c_tax import load_c_tax_full
from transform.dim_ad_org import load_ad_org_full
from transform.dim_c_bp_group import load_c_bp_group_full
from transform.dim_c_doctype import load_c_doctype_full
from transform.dim_c_market import load_c_market_full
from transform.dim_c_submarket import load_c_submarket_full
from transform.dim_c_uom import load_c_uom_full
from transform.dim_m_product import load_m_product_full
from transform.dim_c_bpartner import load_c_bpartner_full
from transform.dim_c_producttype import load_c_producttype_full
from transform.dim_c_invoice import load_c_invoice_full
from transform.dim_date import load_dim_date
from transform.dim_c_currency import load_c_currency_full

## dim Kho_test
from transform.dim_m_locator import load_m_locator_full
from transform.dim_m_step import load_m_step_full
from transform.dim_product_category import load_m_product_category_full
from transform.dim_m_warehouse import load_m_warehouse_full
from transform.dim_m_inout import load_m_inout_full
from transform.dim_c_department import load_c_department_full
## Invoiceline
from transform.fact_c_invoiceline import load_c_invoiceline_full

## Inoutline
from transform.fact_m_inoutline import load_m_inoutline_full

####


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

with DAG("DIMESION",
        default_args=default_args,
        schedule="15 6 * * *",
        catchup=False,
        tags=["ETL", "DIMENSION"]
        ) as dag:
    
    ###############################################
    ## Cần chia lại dim cha và dim con, dim con sẽ load trước
    ## 3 Level Dim, 1 là dim to nhất, 2, 3 bé dần
    ###############################################
    transform_dim_task = {

    # Dim level 3
    "TF_dim_m_step": load_m_step_full,
    "TF_dim_c_tax": load_c_tax_full,
    "TF_dim_ad_org": load_ad_org_full,
    "TF_dim_c_uom": load_c_uom_full,
    "TF_dim_date": load_dim_date,
    "TF_dim_c_doctype": load_c_doctype_full,


    "TF_dim_m_warehouse": load_m_warehouse_full,
    "TF_dim_m_locator": load_m_locator_full,
    "TF_dim_c_producttype": load_c_producttype_full,
    "TF_dim_product_category": load_m_product_category_full,
    "TF_dim_c_department":load_c_department_full,
    "TF_dim_c_bp_group": load_c_bp_group_full,
    "TF_dim_c_submarket": load_c_submarket_full,
    "TF_dim_c_currency": load_c_currency_full,



    # Dim level 2
    "TF_dim_c_market": load_c_market_full,
    "TF_dim_m_product": load_m_product_full,
    "TF_dim_c_bpartner": load_c_bpartner_full,

    ## dim level 1
    "TF_dim_c_invoice": load_c_invoice_full,
    "TF_dim_m_inout": load_m_inout_full
    }   
    


    with TaskGroup("TRANSFORM_DIMESION") as transform_dim_group:
        create_table_dimesion = SQLExecuteQueryOperator(
            task_id="create_dim_tables",
            conn_id="STAGING_POSTGRES",
            sql="./CREATE_DIM_XMCP.sql"
        )

        transform_dim_task_list = []
        for task_id, fn in transform_dim_task.items():
            transform_dim_task_list.append(
                PythonOperator(
                    task_id=task_id,
                    python_callable=fn,
                    dag=dag
                )
            )  

        create_table_dimesion >> transform_dim_task_list 
