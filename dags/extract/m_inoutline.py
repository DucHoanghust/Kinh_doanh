from plugins.oracle_operators import OracleHookThick
from plugins.postgres_operators import PostgresOperators

# M_INOUTLINE: Chi tiết phiếu nhập xuất

# movementtype -> Phân loại thành Xuất/Nhập

def extract_m_inoutline():
    source_operator = OracleHookThick(conn_id="SOURCE_ORACLE")
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")

    sql="""
    
    select
        m_inoutline_id,
        
        m_inout_id,
        m_locator_id,
        m_product_id,
        ad_org_id,
        c_uom_id,
        m_step_id,
        m_warehouse_id,

        movementtype, 

        qtyrequiered,
        qty,
        
        qtyentered,
        movementqty,
        
        rateconverted,
        
        priceentered,
        pricecost,
        
        amountconvert,
        linenetamount,
        
        totaltaxamount,
        taxamountconvert,
        
        totallines,
        totallinesconvert,
        
        amountallocation,
        distributionamount,
        
        
        receiptdate,
        lifetime,
        dateexpiration,
        classification,
        timeused,
        timestock,
        
        
        qtyonhand,
        updated
    from m_inoutline 

"""    

    df = source_operator.get_data_to_pandas(sql)
    print(df.columns)
    print(df.shape)
    df.columns = [col.lower() for col in df.columns]
    staging_operator.save_data_to_postgres(
        df,
        table_name="m_inoutline",
        schema="xmcp_staging",
        if_exists="replace"
    
    )