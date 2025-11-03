# from airflow import DAG
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from airflow.providers.standard.operators.python import PythonOperator
# from datetime import datetime
# from airflow.utils.task_group import TaskGroup

# ####
# from extract.c_tax import extract_c_tax ### Cần sửa lại
# from extract.c_submarket import extract_c_submarket
# from extract.c_market import extract_c_market
# from extract.m_product import extract_m_product
# from extract.c_invoice import extract_c_invoice
# from extract.c_bpartner import extract_c_bpartner
# from extract.c_bp_group import extract_c_bp_group
# from extract.ad_org import extract_ad_org
# from extract.c_uom import extract_c_uom
# from extract.c_producttype import extract_c_producttype
# from extract.c_invoiceline import extract_c_invoiceline

# ####
# from transform.dim_c_tax import load_c_tax_incremental
# from transform.dim_ad_org import load_ad_org_incremental
# from transform.dim_c_bp_group import load_c_bp_group_incremental
# from transform.dim_c_doctype import load_c_doctype_incremental
# from transform.dim_c_market import load_c_market_incremental
# from transform.dim_c_submarket import load_c_submarket_incremental
# from transform.dim_c_uom import load_c_uom_incremental
# from transform.dim_m_product import load_m_product_incremental
# from transform.dim_c_bpartner import load_c_bpartner_incremental
# from transform.dim_c_producttype import load_c_producttype_incremental
# from transform.dim_c_invoice import load_c_invoice_incremental
# from transform.dim_date import load_dim_date

# with DAG("KD_INCREMENTAL_LOAD",
#          start_date=datetime(2024, 1, 1),
#          schedule=None) as dag:
    
    
#     with TaskGroup("LOAD_DIM") as load_incr_dim:
#         TF_dim_c_tax = PythonOperator(
#             task_id="TF_dim_c_tax",
#             python_callable=load_c_tax_incremental,
#             dag=dag
#         )

#         TF_dim_ad_org = PythonOperator(
#             task_id="TF_dim_ad_org",
#             python_callable=load_ad_org_incremental,
#             dag=dag
#         )

#         TF_dim_c_bp_group = PythonOperator(
#             task_id="TF_dim_c_bp_group",
#             python_callable=load_c_bp_group_incremental,
#             dag=dag
#         )

#         TF_dim_c_doctype = PythonOperator(
#             task_id="TF_dim_c_doctype",
#             python_callable=load_c_doctype_incremental,
#             dag=dag
#         )

#         TF_dim_c_market = PythonOperator(
#             task_id="TF_dim_c_market",
#             python_callable=load_c_market_incremental,
#             dag=dag
#         )

#         TF_dim_c_submarket = PythonOperator(
#             task_id="TF_dim_c_submarket",
#             python_callable=load_c_submarket_incremental,
#             dag=dag
#         )

#         TF_dim_c_uom = PythonOperator(
#             task_id="TF_dim_c_uom",
#             python_callable=load_c_uom_incremental,
#             dag=dag
#         )

#         TF_dim_m_product = PythonOperator(
#             task_id="TF_dim_m_product",
#             python_callable=load_m_product_incremental,
#             dag=dag
#         )

#         TF_dim_c_bpartner = PythonOperator(
#             task_id="TF_dim_c_bpartner",
#             python_callable=load_c_bpartner_incremental,
#             dag=dag
#         )

#         TF_dim_c_producttype = PythonOperator(
#             task_id="TF_dim_c_producttype",
#             python_callable=load_c_producttype_incremental,
#             dag=dag
#         )

#         TF_dim_c_invoice = PythonOperator(
#             task_id="TF_dim_c_invoice",
#             python_callable=load_c_invoice_incremental,
#             dag=dag
#         )

#     with TaskGroup("LOAD_FACT") as load_incr_fact:
#         pass
