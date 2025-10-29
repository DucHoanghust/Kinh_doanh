from datetime import datetime
from plugins.postgres_operators import PostgresOperators
import pandas as pd
import logging

def load_m_warehouse_full():

    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")

    

    df = staging_operator.get_data_to_pd("""
                                        SELECT
                                            m_warehouse_id,
                                            name, 
                                            value, 
                                            isstocked,
                                            isactive,
                                            created,
                                            updated
                                        from xmcp_staging.m_warehouse
                                         """)  

      
    logging.info(df.columns)
    # Chuẩn hóa lại is active sang boolean từ Y/N sang 1/0
    df['isactive'] = df['isactive'].map({'Y': 1, 'N': 0})
    
    # Thêm Surrogate Key  -- Bước này sẽ tự thêm trong Serial trong sql
    # df['c_tax_sk'] = df.index + 1

    # Xử lí SCD Type 2
    df['valid_from'] = pd.Timestamp.now()
    df['valid_to'] = pd.Timestamp("9999-12-31 23:59:59")
    df['is_current'] = 1
    

    dw_operator.save_data_to_postgres(
        df,
        table_name="dim_m_warehouse",
        schema="xmcp_dw",
        if_exists="append"
    
    )  