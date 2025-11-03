DROP SCHEMA IF EXISTS xmcp_staging CASCADE;

CREATE SCHEMA IF NOT EXISTS xmcp_staging;

CREATE TABLE IF NOT EXISTS xmcp_staging.c_tax (
    c_tax_id INT,
    c_taxcategory_id INT,
    name VARCHAR(255),
    rate INT,
    value VARCHAR(10),
    isactive VARCHAR(2),
    created TIMESTAMP,
    updated TIMESTAMP
);




CREATE TABLE IF NOT EXISTS xmcp_staging.c_submarket (
    c_submarket_id INT,
    c_market_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive INT,
    created TIMESTAMP,  
    updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS xmcp_staging.c_market (
    c_market_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    created TIMESTAMP,
    updated TIMESTAMP
);


CREATE TABLE IF NOT EXISTS xmcp_staging.m_product (
    m_product_id INT,
    c_uom_id INT,
    c_producttype_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive INT,
    created TIMESTAMP,
    updated TIMESTAMP
);


CREATE TABLE IF NOT EXISTS xmcp_staging.c_invoice (
    c_invoice_id INT,
    c_bpartner_id INT,
    m_product_id INT,
    c_doctype_id INT,
    c_currency_id INT,
    ad_org_id INT,
    documentno VARCHAR(255),
    currency_rate INT,
    isactive INT,
    created TIMESTAMP,
    updated TIMESTAMP,

    dateinvoice TIMESTAMP
);

CREATE TABLE IF NOT EXISTS xmcp_staging.c_currency (
    c_currency_id INT,
    name VARCHAR(255),
    isactive INT,
    created TIMESTAMP,
    updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS xmcp_staging.c_bpartner (
    c_bpartner_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive INT,
    created TIMESTAMP,
    updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS xmcp_staging.c_bp_group (
    c_bp_group_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive INT,
    created TIMESTAMP,
    updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS xmcp_staging.ad_org (
    ad_org_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive INT,
    created TIMESTAMP,
    updated TIMESTAMP
);


CREATE TABLE IF NOT EXISTS xmcp_staging.c_uom (
    c_uom_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    uomtype VARCHAR(10),
    isactive INT,
    created TIMESTAMP,
    updated TIMESTAMP
);


CREATE TABLE IF NOT EXISTS xmcp_staging.c_producttype (
    c_producttype_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive VARCHAR(2),
    created TIMESTAMP,
    updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS xmcp_staging.c_invoiceline (
    c_invoiceline_id INT,
    c_invoice_id INT,
    c_submarket_id INT,
    m_product_id INT,
    ad_org_id INT,
    c_uom_id INT,
    c_tax_id INT,

    c_bpartner_id INT,

    qtyinvoiced NUMERIC(10,2),  
    priceactual NUMERIC(20,10), 
    linenetamt NUMERIC(20,10), 
    linetotalamt NUMERIC(20,10), 
    linenetamtconvert NUMERIC(20,10), 


    discount NUMERIC(10,2), 
    discountamt NUMERIC(10,2), 
    discountamtconvert NUMERIC(10,2), 
    discount2amt NUMERIC(10,2), 
    discountamt2convert NUMERIC(10,2), 
    percenttax NUMERIC(10,2), 
    taxamount NUMERIC(20,10), 
    taxamountconvert NUMERIC(20,10), 
    grandtotal NUMERIC(20,10), 
    grandtotalconvert NUMERIC(20,10),


    qtyentered NUMERIC(10,2), 
    movementqty NUMERIC(10,2),

    isactive VARCHAR(2),
    created TIMESTAMP,
    updated TIMESTAMP

    
);

CREATE TABLE IF NOT EXISTS xmcp_staging.c_doctype (
    c_doctype_id INT,
    name VARCHAR(255),
    printname VARCHAR(255),
    docbasetype VARCHAR(10),
    issotrx VARCHAR(2),
    isactive VARCHAR(2),
    created TIMESTAMP,
    updated TIMESTAMP
);


CREATE TABLE IF NOT EXISTS xmcp_staging.c_department (
    c_department_id INT,
    ad_org_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive VARCHAR(2),
    created TIMESTAMP,
    updated TIMESTAMP
);


CREATE TABLE IF NOT EXISTS xmcp_staging.hr_employee (
    hr_employee_id INT,
    c_department_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    gender VARCHAR(10),
    date_birth TIMESTAMP,
    mobile VARCHAR(20),
    isactive VARCHAR(2),
    created TIMESTAMP,
    updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS xmcp_staging.m_locator (
    m_locator_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive VARCHAR(2),
    created TIMESTAMP,
    updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS xmcp_staging.m_warehouse (
    m_warehouse_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isstocked VARCHAR(10), --Không tính tồn kho hàng hỏng 
    isactive VARCHAR(2),
    created TIMESTAMP,
    updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS xmcp_staging.m_product_category (
    m_product_category_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive VARCHAR(2),
    created TIMESTAMP,
    updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS xmcp_staging.m_inout (
    m_inout_id INT,
    c_department_create_id INT, -- Phòng ban YC
    m_warehouse_id INT, 
    c_department_id INT,
    c_doctype_id INT,
    c_bpartner_id INT,

    
    documentno VARCHAR(255),
    currencyrate NUMERIC(20,10), -- tỷ giá
    movementtype VARCHAR(10),
    
    docstatus VARCHAR(10), -- Trạng thái CO: Hoàn thành, DR: Đang nháp
    register_status VARCHAR(10), -- Trạng thái ký: K: Đã ký, N: Chưa trình ký, H: Hủy, TK: Đang trình kí
    isinvoiced INT,
    isprinted INT,
    isactive VARCHAR(2),  
    created TIMESTAMP,
    updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS xmcp_staging.m_inoutline (
    m_inoutline_id INT,

    m_inout_id INT,
    m_locator_id INT,
    m_product_id INT,
    ad_org_id INT,
    c_uom_id INT,
    m_step_id INT,
    m_warehouse_id INT,

    movementtype VARCHAR(10),
    
    qtyrequiered NUMERIC(10,2),  
    qty NUMERIC(10,2),  
    
    qtyentered NUMERIC(10,2), 
    movementqty NUMERIC(10,2), 
    
    
    rateconverted NUMERIC(10,2), 
    
    priceentered NUMERIC(30,10), 
    pricecost NUMERIC(30,10), 
    
    amountconvert NUMERIC(30,10), 
    linenetamount NUMERIC(30,10), 
    
    totaltaxamount NUMERIC(30,10), 
    taxamountconvert NUMERIC(30,10), 
    
    totallines NUMERIC(30,10), 
    totallinesconvert NUMERIC(30,10), 
    
    amountallocation NUMERIC(30,10), 
    distributionamount NUMERIC(30,10), 
    
    
    receiptdate TIMESTAMP,
    lifetime INT,
    dateexpiration TIMESTAMP,
    classification VARCHAR(255),
    timeused INT,
    timestock INT,
    
    
    qtyonhand NUMERIC(10,2),
    updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS xmcp_staging.m_step (
    m_step_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive VARCHAR(2),
    created TIMESTAMP,
    updated TIMESTAMP
);

