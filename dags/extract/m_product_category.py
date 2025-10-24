from plugins.oracle_operators import OracleHookThick
from plugins.postgres_operators import PostgresOperators


def extract_m_product_category():
    source_operator = OracleHookThick(conn_id="SOURCE_ORACLE")
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")

    sql="""
        select
        m_product_category_id,
        
        name,
        value,
        
        isactive, 
        created,
        updated
    from m_product_category

"""

    df = source_operator.get_data_to_pandas(sql)
    print(df.columns)
    print(df.shape)
    df.columns = [col.lower() for col in df.columns]
    staging_operator.save_data_to_postgres(
        df,
        table_name="m_product_category",
        schema="xmcp_staging",
        if_exists="replace"
    
    )