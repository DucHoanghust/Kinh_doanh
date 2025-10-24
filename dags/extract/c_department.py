from plugins.oracle_operators import OracleHookThick
from plugins.postgres_operators import PostgresOperators


def extract_c_department():
    source_operator = OracleHookThick(conn_id="SOURCE_ORACLE")
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    
    sql = """
    SELECT c_department_id,
           ad_org_id, 
           name, 
           value, 
           isactive, 
           created, 
           updated
    FROM c_department
    """

    df = source_operator.get_data_to_pandas(sql)
    print(df.columns)
    print(df.shape)
    df.columns = [col.lower() for col in df.columns]
    staging_operator.save_data_to_postgres(
        df,
        table_name="c_department",
        schema="xmcp_staging",
        if_exists="replace"
    
    )