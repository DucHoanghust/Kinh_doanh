from plugins.oracle_operators import OracleHookThick
from plugins.postgres_operators import PostgresOperators


def extract_m_warehouse():
    source_operator = OracleHookThick(conn_id="SOURCE_ORACLE")
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")

    sql="""
    select
        m_warehouse_id,
        name,
        value,
        isstocked,--Không tính tồn kho hàng hỏng 
        isactive, 
        created,
        updated
    from m_warehouse
"""    

    df = source_operator.get_data_to_pandas(sql)
    print(df.columns)
    print(df.shape)
    df.columns = [col.lower() for col in df.columns]
    staging_operator.save_data_to_postgres(
        df,
        table_name="m_warehouse",
        schema="xmcp_staging",
        if_exists="replace"
    
    )