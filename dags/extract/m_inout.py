from plugins.oracle_operators import OracleHookThick
from plugins.postgres_operators import PostgresOperators


def extract_m_inout():
    source_operator = OracleHookThick(conn_id="SOURCE_ORACLE")
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    """
        currencyrate, -- tỷ giá
        docstatus, -- Trạng thái CO: Hoàn thành, DR: Đang nháp
        register_status, -- Trạng thái ký: K: Đã ký, N: Chưa trình ký, H: Hủy, TK: Đang trình kí
        isprinted,
        isactive,
        isinvoiced -- là hóa đơn hay không
    """
    sql="""
    select
        m_inout_id,
        c_department_create_id, -- Phòng ban YC
        m_warehouse_id, -- Kho
        c_submarket_id,
        

        documentno,
        currencyrate, -- tỷ giá
        
        
        
        docstatus, -- Trạng thái CO: Hoàn thành, DR: Đang nháp
        register_status, -- Trạng thái ký: K: Đã ký, N: Chưa trình ký, H: Hủy, TK: Đang trình kí
        isinvoiced,
        isprinted,
        isactive,  
        created,
        updated
    from m_inout

"""    

    df = source_operator.get_data_to_pandas(sql)
    print(df.columns)
    print(df.shape)
    df.columns = [col.lower() for col in df.columns]
    staging_operator.save_data_to_postgres(
        df,
        table_name="m_inout",
        schema="xmcp_staging",
        if_exists="replace"
    
    )