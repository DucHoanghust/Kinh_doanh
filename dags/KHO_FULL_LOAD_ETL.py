from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.utils.email import send_email


####
from dags.extract.c_doctype import extract_c_doctype
from dags.extract.c_uom import extract_c_uom
from dags.extract.m_inout import extract_m_inout
from dags.extract.m_inoutline import extract_m_inoutline
from dags.extract.m_locator import extract_m_locator
from dags.extract.m_product import extract_m_product
from dags.extract.m_product_category import extract_m_product_category
from dags.extract.m_step import extract_m_step
from dags.extract.m_warehouse import extract_m_warehouse
from dags.extract.ad_org import extract_ad_org

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

with DAG("KHO_FULL_LOAD",
        default_args=default_args,
        schedule="0 7 * * *",
        catchup=False,
         ) as dag:
    
    with TaskGroup("EXTRACT") as extract_group:

        E_m_locator = PythonOperator(
            task_id="E_m_locator",
            python_callable=extract_m_locator,
            dag=dag
        )

        E_m_warehouse = PythonOperator(
            task_id="E_m_warehouse",
            python_callable=extract_m_warehouse,
            dag=dag
        )

        E_ad_org = PythonOperator(
            task_id="E_ad_org",
            python_callable=extract_ad_org,
            dag=dag
        )

        E_m_product = PythonOperator(
            task_id="E_m_product",
            python_callable=extract_m_product,
            dag=dag
        )

        E_m_product_category = PythonOperator(
            task_id="E_m_product_category",
            python_callable=extract_m_product_category,
            dag=dag
        )

        E_c_uom = PythonOperator(
            task_id="E_c_uom",
            python_callable=extract_c_uom,
            dag=dag
        )

        E_c_doctype = PythonOperator(
            task_id="E_c_doctype",
            python_callable=extract_c_doctype,
            dag=dag
        )

        E_m_step = PythonOperator(
            task_id="E_m_step",
            python_callable=extract_m_step,
            dag=dag
        )

        E_m_inout = PythonOperator(
            task_id="E_m_inout",
            python_callable=extract_m_inout,
            dag=dag
        )

        E_m_inoutline = PythonOperator(
            task_id="E_m_inoutline",
            python_callable=extract_m_inoutline,
            dag=dag
        )






extract_group 