from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.utils.email import send_email
from airflow.sensors.external_task import ExternalTaskSensor
####
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

with DAG("FACT",
        default_args=default_args,
        schedule="30 6 * * *",
        catchup=False,
        tags=["ETL", "FACT"]
        ) as dag:
    
 
    
    transform_fact_task={
    "L_fact_c_invoiceline": load_c_invoiceline_full,
    "L_fact_m_inoutline":load_m_inoutline_full
    }


    with TaskGroup("TRANSFORM_FACT") as transform_fact_group:
        # wait_for_dim = ExternalTaskSensor(
        #     task_id="wait_for_dim",
        #     external_dag_id="DIMESION",
        #     mode="poke",
        #     timeout=600,
        #     poke_interval=60
        # )

        create_table_fact = SQLExecuteQueryOperator(
            task_id="create_fact_tables",
            conn_id="DW_POSTGRES",
            sql="./CREATE_FACT_XMCP.sql"
        )

        transform_fact_task_list = []
        for task_id, fn in transform_fact_task.items():
            transform_fact_task_list.append(
                PythonOperator(
                    task_id=task_id,
                    python_callable=fn,
                    dag=dag
                )
            )

    with TaskGroup("CREATE_VIEW") as create_view:
        create_view = SQLExecuteQueryOperator(
            task_id="create_view",
            conn_id="DW_POSTGRES",
            sql="./CREATE_VIEW.sql"
        )
        
        # wait_for_dim >> 
        create_table_fact >> transform_fact_task_list >> create_view
