from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from plugins.postgres_operators import PostgresOperators
import pandas as pd
import logging

def load_m_inoutline_full():
    
    staging_operator = PostgresOperators(conn_id="STAGING_POSTGRES")
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")
    # Sửa lại load sql
    

    df = staging_operator.get_data_to_pd("""SELECT * FROM xmcp_staging.m_inoutline""")
    logging.info(df.columns)
    logging.info("🔄 Start full load fact_m_inoutline")
    
    # Các trường thừa đã bỏ
    # qtyentered,
    # movementqty,

    sql="""
        INSERT INTO xmcp_dw.fact_m_inoutline (
            m_inoutline_id,

            m_inout_sk,
            m_step_sk,
            m_warehouse_sk,
            m_product_sk,
            ad_org_sk,
            m_locator_sk,
            c_uom_sk,
            date_sk,

            
            movementtype, 
            ---- PHIẾU NHẬP KHO
            -- SL yêu cầu/ SL yêu cầu quy đổi
            qtyrequiered,
            qty,

            -- Số lượng 
            qtyentered,

            -- Số lượng quy đổi
            movementqty,
            -- HS quy đổi
            rateconverted,
            
            -- giá gốc / giá quy đổi
            priceentered,
            pricecost,
            
            -- Thành tiền / thành tiền quy đổi
            amountconvert,
            linenetamount,
            
            -- Tổng thuế/ tổng thuế quy đổi
            totaltaxamount,
            taxamountconvert,
            
            -- Tổng tiền/ tổng tiền quy đổi
            totallines,
            totallinesconvert,
            
            -- Tiền phân bổ đích danh/ số tiền phân bổ
            amountallocation,
            distributionamount,
            
            -- Ngày nhập 
            receiptdate,

            -- Thời gian đưa vào sử dụng (Tháng)
            lifetime,

            -- Kế hoạch sử dụng
            dateexpiration,

            -- Loại hàng hóa
            classification,

            -- Thời gian bảo hành đưa vào sử dụng/ Thời gian bảo hành lưu kho
            timeused,
            timestock,
            
            -- PHIẾU XUẤT KHO
            -- Số lượng tồn kho
            Qtyonhand,
            
            updated

        )
        SELECT 
            mi.m_inoutline_id,

            COALESCE(m.m_inout_sk, -1) as m_inout_sk,
            COALESCE(s.m_step_sk,-1) as m_step_sk,
            COALESCE(w.m_warehouse_sk,-1) as m_warehouse_sk,
            COALESCE(p.m_product_sk, -1) as m_product_sk,
            COALESCE(a.ad_org_sk, -1) as ad_org_sk,
            COALESCE(l.m_locator_sk,-1) as m_locator_sk,
            COALESCE(c.c_uom_sk, -1) c_uom_sk,
            COALESCE(d.date_sk, -1) date_sk,


            
            COALESCE(m.movementtype, 'n/a') AS movementtype,
            COALESCE(mi.qtyrequiered, 0) AS qtyrequiered,
            COALESCE(mi.qty, 0) AS qty,
            COALESCE(mi.qtyentered, 0) AS qtyentered,
            COALESCE(mi.movementqty, 0) AS movementqty,
            COALESCE(mi.rateconverted, 0) AS rateconverted,
            COALESCE(mi.priceentered, 0) AS priceentered,
            COALESCE(mi.pricecost, 0) AS pricecost,
            COALESCE(mi.amountconvert, 0) AS amountconvert,
            COALESCE(mi.linenetamount, 0) AS linenetamount,
            COALESCE(mi.totaltaxamount, 0) AS totaltaxamount,
            COALESCE(mi.taxamountconvert, 0) AS taxamountconvert,
            COALESCE(mi.totallines, 0) AS totallines,
            COALESCE(mi.totallinesconvert, 0) AS totallinesconvert,
            COALESCE(mi.amountallocation, 0) AS amountallocation,
            COALESCE(mi.distributionamount, 0) AS distributionamount,

            mi.receiptdate AS receiptdate,
            COALESCE(mi.lifetime, 0) AS lifetime,
            
            mi.dateexpiration AS dateexpiration,
            COALESCE(mi.classification, 'n/a') AS classification,
            COALESCE(mi.timeused, 0) AS timeused,
            COALESCE(mi.timestock, 0) AS timestock,
            COALESCE(mi.qtyonhand, 0) AS qtyonhand,
            mi.updated

        FROM xmcp_staging.m_inoutline mi
        LEFT JOIN xmcp_dw.dim_m_inout m ON m.m_inout_id = mi.m_inout_id
        LEFT JOIN xmcp_dw.dim_c_uom c on c.c_uom_id=mi.c_uom_id
        LEFT JOIN xmcp_dw.dim_ad_org a on a.ad_org_id=mi.ad_org_id
        LEFT JOIN xmcp_dw.dim_m_product p on p.m_product_id=mi.m_product_id
        LEFT JOIN xmcp_dw.dim_m_warehouse w on w.m_warehouse_id=mi.m_warehouse_id
        LEFT JOIN xmcp_dw.dim_m_step s on s.m_step_id=mi.m_step_id
        LEFT JOIN xmcp_dw.dim_m_locator l on l.m_locator_id=mi.m_locator_id
        LEFT JOIN xmcp_dw.dim_date d ON receiptdate::DATE = d.full_date
    """

    dw_operator.run_sql(sql)
