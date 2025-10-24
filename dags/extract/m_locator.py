from plugins.oracle_operators import OracleHookThick
from plugins.postgres_operators import PostgresOperators

# M_LOCATOR: Vị trí kho hàng

def extract_m_locator():
    source_operator = OracleHookThick(conn_id="SOURCE_ORACLE")
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")

    sql="""
    
    select 
        m_locator_id,
        name, 
        value, 
        isactive,
        created,
        updated
    from m_locator;
"""    

    df = source_operator.get_data_to_pandas(sql)
    print(df.columns)
    print(df.shape)
    df.columns = [col.lower() for col in df.columns]
    staging_operator.save_data_to_postgres(
        df,
        table_name="m_locator",
        schema="xmcp_staging",
        if_exists="replace"
    
    )