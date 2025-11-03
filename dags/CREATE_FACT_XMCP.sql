DROP TABLE IF EXISTS xmcp_dw.fact_c_invoiceline CASCADE;
DROP TABLE  IF EXISTS xmcp_dw.fact_m_inoutline CASCADE;
-- FACT INOUTLINE
CREATE TABLE IF NOT EXISTS xmcp_dw.fact_m_inoutline (
    m_inoutline_sk SERIAL PRIMARY KEY,
    m_inoutline_id INT,
        
    -- dimesion surrougate key
    m_inout_sk INT NOT NULL,
    c_uom_sk INT NOT NULL,
    m_locator_sk INT NOT NULL,
    m_step_sk INT NOT NULL,
    m_warehouse_sk INT NOT NULL,
    m_product_sk INT NOT NULL,
    ad_org_sk INT NOT NULL,
    date_sk INT NOT NULL,
    -- c_doctype_sk INT NOT NULL,

    -- measures
    m_inout_id INT,
    m_locator_id INT,
    m_product_id INT,
    c_uom_id INT,
    m_step_id INT,
    ad_org_id INT,
    m_warehousehouse_id INT,

    movementtype VARCHAR(10), 
    ---- PHIẾU NHẬP KHO
    -- SL yêu cầu/ SL yêu cầu quy đổi
    qtyrequiered NUMERIC(10,2),
    qty NUMERIC(10,2),

    -- Số lượng 
    qtyentered NUMERIC(10,2),

    -- Số lượng quy đổi
    movementqty NUMERIC(10,2),
    -- HS quy đổi
    rateconverted NUMERIC(10,2),
    
    -- giá gốc / giá quy đổi
    priceentered NUMERIC(30,10),
    pricecost NUMERIC(30,10),
    
    -- Thành tiền / thành tiền quy đổi
    amountconvert NUMERIC(30,10),
    linenetamount NUMERIC(30,10),
    
    -- Tổng thuế/ tổng thuế quy đổi
    totaltaxamount NUMERIC(30,10),
    taxamountconvert NUMERIC(30,10),
    
    -- Tổng tiền/ tổng tiền quy đổi
    totallines NUMERIC(30,10),
    totallinesconvert NUMERIC(30,10),
    
    -- Tiền phân bổ đích danh/ số tiền phân bổ
    amountallocation NUMERIC(30,10),
    distributionamount NUMERIC(30,10),
    
    -- Ngày nhập 
    receiptdate TIMESTAMP,

    -- Thời gian đưa vào sử dụng (Tháng)
    lifetime INT,

    -- Kế hoạch sử dụng
    dateexpiration TIMESTAMP,

    -- Loại hàng hóa
    classification VARCHAR(255),

    -- Thời gian bảo hành đưa vào sử dụng/ Thời gian bảo hành lưu kho
    timeused INT,
    timestock INT,
    
    -- PHIẾU XUẤT KHO
    -- Số lượng tồn kho
    qtyonhand NUMERIC(10,2),
    
    updated TIMESTAMP
);

-- FACT INVOICELINE

CREATE TABLE IF NOT EXISTS xmcp_dw.fact_c_invoiceline (
    c_invoiceline_sk SERIAL PRIMARY KEY,
    c_invoiceline_id INT NOT NULL,

    -- dimesion surrougate key
    c_invoice_sk INT NOT NULL,
    c_submarket_sk INT NOT NULL,
    m_product_sk INT NOT NULL,
    ad_org_sk INT NOT NULL,
    c_uom_sk INT NOT NULL,
    c_tax_sk INT NOT NULL,
    date_sk INT NOT NULL,
    c_bpartner_sk INT,
    -- c_bp_group_sk INT NOT NULL,
    

    -- measures
    qtyinvoiced NUMERIC(30,2),  
    priceactual NUMERIC(30,10), 
    linenetamt NUMERIC(30,10), 
    linetotalamt NUMERIC(30,10), 
    linenetamtconvert NUMERIC(30,10), 


    discount NUMERIC(30,2), 
    discountamt NUMERIC(30,2), 
    discountamtconvert NUMERIC(30,2), 
    discount2amt NUMERIC(30,2), 
    discountamt2convert NUMERIC(30,2), 
    percenttax NUMERIC(30,2), 
    taxamount NUMERIC(30,10), 
    taxamountconvert NUMERIC(30,10), 
    grandtotal NUMERIC(30,10), 
    grandtotalconvert NUMERIC(30,10),


    qtyentered NUMERIC(30,2), 
    movementqty NUMERIC(30,2)


);



-- fact_c_invoiceline → dim_c_invoice
ALTER TABLE xmcp_dw.fact_c_invoiceline
ADD CONSTRAINT fk_fact_invoice
FOREIGN KEY (c_invoice_sk) REFERENCES xmcp_dw.dim_c_invoice(c_invoice_sk) NOT VALID;

-- -- fact_c_invoiceline → dim_c_submarket
-- ALTER TABLE xmcp_dw.fact_c_invoiceline
-- ADD CONSTRAINT fk_fact_submarket
-- FOREIGN KEY (c_submarket_sk) REFERENCES xmcp_dw.dim_c_submarket(c_submarket_sk) NOT VALID;

-- fact_c_invoiceline → dim_m_product
ALTER TABLE xmcp_dw.fact_c_invoiceline
ADD CONSTRAINT fk_fact_product
FOREIGN KEY (m_product_sk) REFERENCES xmcp_dw.dim_m_product(m_product_sk) NOT VALID;

-- fact_c_invoiceline → dim_ad_org
ALTER TABLE xmcp_dw.fact_c_invoiceline
ADD CONSTRAINT fk_fact_org
FOREIGN KEY (ad_org_sk) REFERENCES xmcp_dw.dim_ad_org(ad_org_sk) NOT VALID;

-- fact_c_invoiceline → dim_c_uom
ALTER TABLE xmcp_dw.fact_c_invoiceline
ADD CONSTRAINT fk_fact_uom
FOREIGN KEY (c_uom_sk) REFERENCES xmcp_dw.dim_c_uom(c_uom_sk) NOT VALID;

-- fact_c_invoiceline → dim_c_tax
ALTER TABLE xmcp_dw.fact_c_invoiceline
ADD CONSTRAINT fk_fact_tax
FOREIGN KEY (c_tax_sk) REFERENCES xmcp_dw.dim_c_tax(c_tax_sk) NOT VALID;

-- fact_c_invoiceline → dim_date
ALTER TABLE xmcp_dw.fact_c_invoiceline
ADD CONSTRAINT fk_fact_date
FOREIGN KEY (date_sk) REFERENCES xmcp_dw.dim_date(date_sk) NOT VALID;

-- fact_c_invoiceline → dim_c_bpartner (bạn có sử dụng c_bpartner_sk)
-- ALTER TABLE xmcp_dw.fact_c_invoiceline
-- ADD CONSTRAINT fk_fact_bpartner
-- FOREIGN KEY (c_bpartner_sk) REFERENCES xmcp_dw.dim_c_bpartner(c_bpartner_sk) NOT VALID;



-- fact_m_inoutline → dim_m_warehouse
ALTER TABLE xmcp_dw.fact_m_inoutline
ADD CONSTRAINT fk_fact_inoutline_warehouse
FOREIGN KEY (m_warehouse_sk) REFERENCES xmcp_dw.dim_m_warehouse(m_warehouse_sk) NOT VALID;

-- fact_m_inoutline → dim_m_inout
ALTER TABLE xmcp_dw.fact_m_inoutline
ADD CONSTRAINT fk_fact_inoutline_inout
FOREIGN KEY (m_inout_sk) REFERENCES xmcp_dw.dim_m_inout(m_inout_sk) NOT VALID;

-- fact_m_inoutline → dim_c_submarket
-- ALTER TABLE xmcp_dw.fact_m_inoutline
-- ADD CONSTRAINT fk_fact_inoutline_submarket
-- FOREIGN KEY (c_submarket_sk) REFERENCES xmcp_dw.dim_c_submarket(c_submarket_sk) NOT VALID;

-- fact_m_inoutline → dim_c_uom
ALTER TABLE xmcp_dw.fact_m_inoutline
ADD CONSTRAINT fk_fact_inoutline_uom
FOREIGN KEY (c_uom_sk) REFERENCES xmcp_dw.dim_c_uom(c_uom_sk) NOT VALID;

-- fact_m_inoutline → dim_m_locator
ALTER TABLE xmcp_dw.fact_m_inoutline
ADD CONSTRAINT fk_fact_inoutline_locator
FOREIGN KEY (m_locator_sk) REFERENCES xmcp_dw.dim_m_locator(m_locator_sk) NOT VALID;

-- fact_m_inoutline → dim_ad_org
ALTER TABLE xmcp_dw.fact_m_inoutline
ADD CONSTRAINT fk_fact_inoutline_org
FOREIGN KEY (ad_org_sk) REFERENCES xmcp_dw.dim_ad_org(ad_org_sk) NOT VALID;

-- fact_m_inoutline → dim_m_product
ALTER TABLE xmcp_dw.fact_m_inoutline
ADD CONSTRAINT fk_fact_inoutline_product
FOREIGN KEY (m_product_sk) REFERENCES xmcp_dw.dim_m_product(m_product_sk) NOT VALID;

-- fact_m_inoutline → dim_c_doctype
-- ALTER TABLE xmcp_dw.fact_m_inoutline
-- ADD CONSTRAINT fk_fact_inoutline_doctype
-- FOREIGN KEY (c_doctype_sk) REFERENCES xmcp_dw.dim_c_doctype(c_doctype_sk) NOT VALID;

-- fact_m_inoutline → dim_m_step
ALTER TABLE xmcp_dw.fact_m_inoutline
ADD CONSTRAINT fk_fact_inoutline_step
FOREIGN KEY (m_step_sk) REFERENCES xmcp_dw.dim_m_step(m_step_sk) NOT VALID;