
from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from plugins.oracle_operators import OracleHookThick
from plugins.postgres_operators import PostgresOperators


def extract_hr_employee():
    source_operator = OracleHookThick(conn_id="SOURCE_ORACLE")
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    
    sql = """
    SELECT hr_employee_id,
        c_department_id,  
        name,
        value, 
        gender,
        date_birth,
        mobile, 
        isactive,
        created,
        updated  
        from HR_Employee
    """

    df = source_operator.get_data_to_pandas(sql)
    print(df.columns)
    print(df.shape)
    df.columns = [col.lower() for col in df.columns]
    staging_operator.save_data_to_postgres(
        df,
        table_name="hr_employee",
        schema="kd_stag",
        if_exists="replace"
    
    )