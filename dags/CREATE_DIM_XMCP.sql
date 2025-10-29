DROP SCHEMA IF EXISTS xmcp_dw CASCADE;

CREATE SCHEMA IF NOT EXISTS xmcp_dw;

-- CREATE TABLE IF NOT EXISTS xmcp_dw.dim_location (
--     location_sk SERIAL PRIMARY KEY,
--     location_id INT,
--     c_market_sk INT,
--     c_market_id INT,
--     name VARCHAR(255),
--     name_chuan VARCHAR(255),
--     lat DOUBLE PRECISION,
--     long DOUBLE PRECISION

-- );

-- INSERT INTO xmcp_dw.dim_location
-- (location_sk,location_id,c_market_sk, c_market_id, name, name_chuan, lat, long)
-- VALUES
-- (-1,NULL, -1, NULL, 'n/a', 'n/a', NULL, NULL)
-- ON CONFLICT DO NOTHING;




CREATE TABLE IF NOT EXISTS xmcp_dw.dim_c_tax (
    c_tax_sk SERIAL PRIMARY KEY,
    c_tax_id INT,
    c_taxcategory_id INT NOT NULL, -- Hơi trùng, cân nhắc bỏ
    name VARCHAR(255),
    rate INT,
    value VARCHAR(255),
    isactive VARCHAR(2) NOT NULL,
    created TIMESTAMP,
    updated TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current INT

);

INSERT INTO xmcp_dw.dim_c_tax (c_tax_sk, c_tax_id, c_taxcategory_id,name,rate,value,isactive,created,updated,valid_from,valid_to,is_current)
VALUES (-1, NULL, -1,'n/a',0,'n/a',1,'2025-01-01 00:00:00', '2025-01-01 00:00:00','1900-01-01 00:00:00','9999-12-31 23:59:59',1)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS xmcp_dw.dim_ad_org (
    ad_org_sk SERIAL PRIMARY KEY,
    ad_org_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive VARCHAR(2) NOT NULL,
    created TIMESTAMP,
    updated TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current INT

);

INSERT INTO xmcp_dw.dim_ad_org(ad_org_sk, ad_org_id, name, value, isactive, created, updated,valid_from,valid_to,is_current)
VALUES (-1, NULL, 'n/a', 'n/a', 1, '2025-01-01 00:00:00', '2025-01-01 00:00:00','1900-01-01 00:00:00','9999-12-31 23:59:59',1)
ON CONFLICT DO NOTHING;


CREATE TABLE IF NOT EXISTS xmcp_dw.dim_c_market (
    c_market_sk SERIAL PRIMARY KEY,
    c_market_id INT,
    location_sk INT,
    name VARCHAR(255),
    name_chuan VARCHAR(255),
    lat DOUBLE PRECISION,
    long DOUBLE PRECISION,
    value VARCHAR(255),
    created TIMESTAMP,
    updated TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current INT
);

INSERT INTO xmcp_dw.dim_c_market (c_market_sk, c_market_id,location_sk, name,name_chuan,lat,long, value, created, updated, valid_from, valid_to, is_current)
VALUES (-1, NULL,-1, 'n/a','n/a',NULL,NULL,'n/a','2025-01-01 00:00:00', '2025-01-01 00:00:00','1900-01-01 00:00:00','9999-12-31 23:59:59',1)
ON CONFLICT DO NOTHING;


CREATE TABLE IF NOT EXISTS xmcp_dw.dim_c_submarket (
    c_submarket_sk SERIAL PRIMARY KEY,
    c_submarket_id INT,
    -- c_market_id INT NOT NULL,
    c_market_sk INT NOT NULL,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive VARCHAR(2) NOT NULL,
    created TIMESTAMP,
    updated TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current INT
    -- CONSTRAINT fk_submarket_market FOREIGN KEY (c_market_sk)
    -- REFERENCES xmcp_dw.dim_c_market(c_market_sk)
);
INSERT INTO xmcp_dw.dim_c_submarket (c_submarket_sk, c_submarket_id, c_market_sk, name, value, isactive, created, updated, valid_from, valid_to, is_current)
VALUES (-1, NULL, -1,'n/a','n/a',1,'2025-01-01 00:00:00', '2025-01-01 00:00:00','1900-01-01 00:00:00','9999-12-31 23:59:59',1)
ON CONFLICT DO NOTHING;




CREATE TABLE IF NOT EXISTS xmcp_dw.dim_c_uom (
    c_uom_sk SERIAL PRIMARY KEY,
    c_uom_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    uomtype VARCHAR(10),
    isactive VARCHAR(2) NOT NULL,
    created TIMESTAMP,
    updated TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current INT
);
INSERT INTO xmcp_dw.dim_c_uom (c_uom_sk, c_uom_id, name, value, uomtype, isactive, created, updated, valid_from, valid_to, is_current)
VALUES (-1, NULL,'n/a','n/a','n/a',1,'2025-01-01 00:00:00', '2025-01-01 00:00:00','1900-01-01 00:00:00','9999-12-31 23:59:59',1)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS xmcp_dw.dim_c_producttype (
    c_producttype_sk SERIAL PRIMARY KEY,
    c_producttype_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive VARCHAR(2) NOT NULL,
    created TIMESTAMP,
    updated TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current INT
);
INSERT INTO xmcp_dw.dim_c_producttype (c_producttype_sk, c_producttype_id, name, value, isactive, created, updated, valid_from, valid_to, is_current)
VALUES (-1, NULL, 'n/a', 'n/a', 1, '2025-01-01 00:00:00', '2025-01-01 00:00:00','1900-01-01 00:00:00','9999-12-31 23:59:59',1)
ON CONFLICT DO NOTHING;


CREATE TABLE IF NOT EXISTS xmcp_dw.dim_m_product (
    m_product_sk SERIAL PRIMARY KEY,
    m_product_id INT,
    c_uom_sk INT,
    c_producttype_sk INT,
    m_product_category_sk INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive VARCHAR(2) NOT NULL,
    created TIMESTAMP,
    updated TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current INT
    -- CONSTRAINT fk_producttype FOREIGN KEY (c_producttype_sk)
    -- REFERENCES xmcp_dw.dim_c_producttype (c_producttype_sk)

    -- CONSTRAINT fk_uom_product FOREIGN KEY (c_uom_sk)
    -- REFERENCES xmcp_dw.dim_c_uom (c_uom_sk)
);
INSERT INTO xmcp_dw.dim_m_product (m_product_sk, m_product_id, c_uom_sk, c_producttype_sk, m_product_category_sk, name, value, isactive, created, updated, valid_from, valid_to, is_current)
VALUES (-1, NULL,-1,-1,-1,'n/a','n/a',1,'2025-01-01 00:00:00', '2025-01-01 00:00:00','1900-01-01 00:00:00','9999-12-31 23:59:59',1)
ON CONFLICT DO NOTHING;




CREATE TABLE IF NOT EXISTS xmcp_dw.dim_c_bp_group (
    c_bp_group_sk SERIAL PRIMARY KEY,
    c_bp_group_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive VARCHAR(2) NOT NULL,
    created TIMESTAMP,
    updated TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current INT
);
INSERT INTO xmcp_dw.dim_c_bp_group (c_bp_group_sk, c_bp_group_id, name, value, isactive, created, updated, valid_from, valid_to, is_current)
VALUES (-1, NULL, 'n/a', 'n/a', 1, '2025-01-01 00:00:00', '2025-01-01 00:00:00','1900-01-01 00:00:00','9999-12-31 23:59:59',1)
ON CONFLICT DO NOTHING;



CREATE TABLE IF NOT EXISTS xmcp_dw.dim_c_bpartner (
    c_bpartner_sk SERIAL PRIMARY KEY,
    c_bpartner_id INT,
    -- c_bp_group_id INT NOT NULL,
    c_bp_group_sk INT NOT NULL,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive VARCHAR(2) NOT NULL,
    created TIMESTAMP,
    updated TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current INT
    -- CONSTRAINT fk_bpartner_bp_group FOREIGN KEY (c_bp_group_sk)
    -- REFERENCES xmcp_dw.dim_c_bp_group(c_bp_group_sk)
);

INSERT INTO xmcp_dw.dim_c_bpartner (c_bpartner_sk, c_bpartner_id, c_bp_group_sk, name, value, isactive, created, updated, valid_from, valid_to, is_current)
VALUES (-1, NULL, -1,'n/a','n/a',1,'2025-01-01 00:00:00', '2025-01-01 00:00:00','1900-01-01 00:00:00','9999-12-31 23:59:59',1)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS xmcp_dw.dim_c_doctype (
    c_doctype_sk SERIAL PRIMARY KEY,
    c_doctype_id INT,
    name VARCHAR(255),
    printname VARCHAR(255),
    docbasetype VARCHAR(255),
    issotrx VARCHAR(2),
    isactive VARCHAR(2) NOT NULL,
    created TIMESTAMP,
    updated TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current INT
);
INSERT INTO xmcp_dw.dim_c_doctype (c_doctype_sk, c_doctype_id, name, printname, docbasetype, issotrx, isactive, created, updated, valid_from, valid_to, is_current)
VALUES (-1, NULL, 'n/a', 'n/a', 'n/a', 0, 1, '2025-01-01 00:00:00', '2025-01-01 00:00:00','1900-01-01 00:00:00','9999-12-31 23:59:59',1)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS xmcp_dw.dim_c_currency (
    c_currency_sk SERIAL PRIMARY KEY,
    c_currency_id INT,
    name VARCHAR(255),
    isactive VARCHAR(2) NOT NULL,
    created TIMESTAMP,
    updated TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current INT
);


INSERT INTO xmcp_dw.dim_c_currency (c_currency_sk, c_currency_id, name, isactive, created, updated, valid_from, valid_to, is_current)
VALUES (-1, NULL, 'n/a', '1', '2025-01-01 00:00:00', '2025-01-01 00:00:00', '1900-01-01 00:00:00', '9999-12-31 23:59:59', 1)
ON CONFLICT DO NOTHING;



CREATE TABLE IF NOT EXISTS xmcp_dw.dim_c_invoice (
    c_invoice_sk SERIAL PRIMARY KEY,
    c_invoice_id INT,
    c_bpartner_sk INT,
    m_product_id INT,
    c_doctype_sk INT,
    c_currency_sk INT,
    ad_org_id INT,
    documentno VARCHAR(255),
    currency_rate INT,
    isactive VARCHAR(2) NOT NULL,
    created TIMESTAMP,
    updated TIMESTAMP,
    dateinvoice TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current INT
    -- CONSTRAINT fk_invoice_doctype FOREIGN KEY (c_doctype_sk)
    -- REFERENCES xmcp_dw.dim_c_doctype(c_doctype_sk),

    -- CONSTRAINT fk_invoice_c_bpartner FOREIGN KEY (c_bpartner_sk)
    -- REFERENCES xmcp_dw.dim_c_bpartner(c_bpartner_sk),

    -- CONSTRAINT fk_invoice_c_currency FOREIGN KEY (c_currency_sk)
    -- REFERENCES xmcp_dw.dim_c_currency(c_currency_sk)
);
INSERT INTO xmcp_dw.dim_c_invoice (c_invoice_sk, c_invoice_id, c_bpartner_sk, m_product_id, c_doctype_sk,c_currency_sk, ad_org_id, documentno,currency_rate, isactive, created, updated,dateinvoice,valid_from,valid_to,is_current)
VALUES (-1, NULL, -1, -1, -1, -1,-1, 'n/a',0, 1, '2025-01-01 00:00:00', '2025-01-01 00:00:00', '2025-01-01 00:00:00','1900-01-01 00:00:00','9999-12-31 23:59:59',1)
ON CONFLICT DO NOTHING;








CREATE TABLE IF NOT EXISTS xmcp_dw.dim_date (
    date_sk        INT PRIMARY KEY,
    full_date      DATE,
    day            INT,
    month          INT,
    month_name     VARCHAR(20),
    quarter        INT,
    year           INT,
    day_of_week    INT,
    day_name       VARCHAR(20),
    week_of_year   INT,
    is_weekend     INT
);
INSERT INTO xmcp_dw.dim_date (
    date_sk, full_date, day, month, month_name, quarter,
    year, day_of_week, day_name, week_of_year, is_weekend
) VALUES (
    -1, NULL, 0, 0, 'n/a', 0,
    0, 0, 'n/a', 0, -1
)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS xmcp_dw.dim_c_department (
    c_department_sk SERIAL PRIMARY KEY,
    c_department_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive VARCHAR(2) NOT NULL,
    created TIMESTAMP,
    updated TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current INT
);
INSERT INTO xmcp_dw.dim_c_department (c_department_sk, c_department_id, name, value, isactive, created, updated, valid_from, valid_to, is_current)
VALUES (-1, NULL, 'n/a', 'n/a', 1, '2025-01-01 00:00:00', '2025-01-01 00:00:00','1900-01-01 00:00:00','9999-12-31 23:59:59',1)
ON CONFLICT DO NOTHING;

-- CREATE TABLE IF NOT EXISTS xmcp_dw.dim_hr_employee (
--     hr_employee_sk SERIAL PRIMARY KEY,
--     hr_employee_id INT,
--     c_department_sk INT,
--     name VARCHAR(255),
--     value VARCHAR(255),
--     gender VARCHAR(10),
--     date_birth TIMESTAMP,
--     mobile VARCHAR(255),
--     isactive VARCHAR(2) NOT NULL,
--     created TIMESTAMP,
--     updated TIMESTAMP,
--     valid_from TIMESTAMP,
--     valid_to TIMESTAMP,
--     is_current INT,

--     -- CONSTRAINT fk_hr_employee_department FOREIGN KEY (c_department_sk)
--     -- REFERENCES xmcp_dw.dim_c_department (c_department_sk)
-- );
-- INSERT INTO xmcp_dw.dim_hr_employee (hr_employee_sk, hr_employee_id,c_department_sk, name, value, gender, date_birth, mobile, isactive, created, updated, valid_from, valid_to, is_current)
-- VALUES (-1, NULL, -1, 'n/a', 'n/a', 'n/a', '1900-01-01 00:00:00', 'n/a', 1, '2025-01-01 00:00:00', '2025-01-01 00:00:00','1900-01-01 00:00:00','9999-12-31 23:59:59',1)
-- ON CONFLICT DO NOTHING;




-- Dim m_locator
CREATE TABLE IF NOT EXISTS xmcp_dw.dim_m_locator(
    m_locator_sk SERIAL PRIMARY KEY,
    m_locator_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive VARCHAR(2) NOT NULL,
    created TIMESTAMP,
    updated TIMESTAMP
);



-- Dim m_step
CREATE TABLE IF NOT EXISTS xmcp_dw.dim_m_step(
    m_step_sk SERIAL PRIMARY KEY,
    m_step_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive VARCHAR(2) NOT NULL,
    created TIMESTAMP,
    updated TIMESTAMP
);

-- Dim m_warehouse
CREATE TABLE IF NOT EXISTS xmcp_dw.dim_m_warehouse(
    m_warehouse_sk SERIAL PRIMARY KEY,
    m_warehouse_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isstocked VARCHAR(2), --Không tính tồn kho hàng hỏng 
    isactive VARCHAR(2),
    created TIMESTAMP,
    updated TIMESTAMP
);


-- Dim m_product_category
CREATE TABLE IF NOT EXISTS xmcp_dw.dim_m_product_category(
    m_product_category_sk SERIAL PRIMARY KEY,
    m_product_category_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive VARCHAR(2),
    created TIMESTAMP,
    updated TIMESTAMP
);

-- Dim m_inout
CREATE TABLE IF NOT EXISTS xmcp_dw.dim_m_inout(
    m_inout_sk SERIAL PRIMARY KEY,
    m_inout_id INT,
    c_department_create_id INT, -- Phòng ban YC
    -- dimension surrougate key
    c_department_sk INT,

    --     
    documentno VARCHAR(255),
    currencyrate NUMERIC(20,10), -- tỷ giá
    docstatus VARCHAR(10), -- Trạng thái CO: Hoàn thành, DR: Đang nháp
    register_status VARCHAR(10), -- Trạng thái ký: K: Đã ký, N: Chưa trình ký, H: Hủy, TK: Đang trình kí
    isprinted VARCHAR(2),
    isactive VARCHAR(2),  
    created TIMESTAMP,
    updated TIMESTAMP
);


-- FACT INOUTLINE
CREATE TABLE IF NOT EXISTS xmcp_dw.fact_m_inoutline (
    m_inoutline_sk SERIAL PRIMARY KEY,
    m_inoutline_id INT,
        
    -- dimesion surrougate key
    m_inout_sk INT NOT NULL,
    c_submarket_sk INT NOT NULL,
    c_uom_sk INT NOT NULL,
    m_locator_sk INT NOT NULL,
    m_step_sk INT NOT NULL,
    m_warehouse_sk INT NOT NULL,
    m_product_sk INT NOT NULL,
    ad_org_sk INT NOT NULL,
    date_sk INT NOT NULL,
    c_doctype_sk INT NOT NULL,

    -- measures
    m_locator_id INT,
    m_product_id INT,
    c_uom_id INT,
    m_step_id INT,

    movementtype VARCHAR(255), 

    qtyrequiered NUMERIC(20,10),
    qty NUMERIC(20,10),
    
    qtyentered NUMERIC(20,10),
    movementqty NUMERIC(20,10),
    
    rateconverted NUMERIC(20,10),
    
    priceentered NUMERIC(20,10),
    pricecost NUMERIC(20,10),
    
    amountconvert NUMERIC(20,10),
    linenetamount NUMERIC(20,10),
    
    totaltaxamount NUMERIC(20,10),
    taxamountconvert NUMERIC(20,10),
    
    totallines NUMERIC(20,10),
    totallinesconvert NUMERIC(20,10),
    
    amountallocation NUMERIC(20,10),
    distributionamount NUMERIC(20,10),
    
    
    receiptdate TIMESTAMP,
    lifetime TIMESTAMP,
    dateexpiration TIMESTAMP,
    classification VARCHAR(255),
    timeused TIMESTAMP,
    timestock TIMESTAMP,
    
    
    
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

    -- foreign key
    -- CONSTRAINT fk_fact_invoice   FOREIGN KEY (c_invoice_sk)   REFERENCES xmcp_dw.dim_c_invoice (c_invoice_sk),
    -- CONSTRAINT fk_fact_submarket    FOREIGN KEY (c_submarket_sk)    REFERENCES xmcp_dw.dim_c_submarket (c_submarket_sk),
    -- CONSTRAINT fk_fact_product   FOREIGN KEY (m_product_sk)   REFERENCES xmcp_dw.dim_m_product (m_product_sk),
    -- CONSTRAINT fk_fact_org       FOREIGN KEY (ad_org_sk)      REFERENCES xmcp_dw.dim_ad_org (ad_org_sk),
    -- CONSTRAINT fk_fact_uom       FOREIGN KEY (c_uom_sk)       REFERENCES xmcp_dw.dim_c_uom (c_uom_sk),
    -- CONSTRAINT fk_fact_tax       FOREIGN KEY (c_tax_sk)       REFERENCES xmcp_dw.dim_c_tax (c_tax_sk),
    -- CONSTRAINT fk_fact_date      FOREIGN KEY (date_sk)        REFERENCES xmcp_dw.dim_date (date_sk)


    -- CONSTRAINT fk_fact_bp_group  FOREIGN KEY (c_bp_group_sk)  REFERENCES xmcp_dw.dim_c_bp_group (c_bp_group_sk)

-- dim c_invoice
ALTER TABLE xmcp_dw.dim_c_invoice
ADD CONSTRAINT fk_invoice_doctype
FOREIGN KEY (c_doctype_sk) REFERENCES xmcp_dw.dim_c_doctype(c_doctype_sk) NOT VALID;

ALTER TABLE xmcp_dw.dim_c_invoice
ADD CONSTRAINT fk_invoice_c_bpartner
FOREIGN KEY (c_bpartner_sk) REFERENCES xmcp_dw.dim_c_bpartner(c_bpartner_sk) NOT VALID;

ALTER TABLE xmcp_dw.dim_c_invoice
ADD CONSTRAINT fk_invoice_c_currency
FOREIGN KEY (c_currency_sk) REFERENCES xmcp_dw.dim_c_currency(c_currency_sk) NOT VALID;

-- dim_c_producttype -> dim_m_product
ALTER TABLE xmcp_dw.dim_m_product
ADD CONSTRAINT fk_producttype
FOREIGN KEY (c_producttype_sk) REFERENCES xmcp_dw.dim_c_producttype(c_producttype_sk) NOT VALID;

-- dim_c_submarket -> dim_c_market
ALTER TABLE xmcp_dw.dim_c_submarket
ADD CONSTRAINT fk_submarket_market
FOREIGN KEY (c_market_sk) REFERENCES xmcp_dw.dim_c_market(c_market_sk) NOT VALID;

-- dim_c_bpartner -> dim_c_bp_group
ALTER TABLE xmcp_dw.dim_c_bpartner
ADD CONSTRAINT fk_bpartner_bp_group
FOREIGN KEY (c_bp_group_sk) REFERENCES xmcp_dw.dim_c_bp_group(c_bp_group_sk) NOT VALID;



-- fact_c_invoiceline → dim_c_invoice
ALTER TABLE xmcp_dw.fact_c_invoiceline
ADD CONSTRAINT fk_fact_invoice
FOREIGN KEY (c_invoice_sk) REFERENCES xmcp_dw.dim_c_invoice(c_invoice_sk) NOT VALID;

-- fact_c_invoiceline → dim_c_submarket
ALTER TABLE xmcp_dw.fact_c_invoiceline
ADD CONSTRAINT fk_fact_submarket
FOREIGN KEY (c_submarket_sk) REFERENCES xmcp_dw.dim_c_submarket(c_submarket_sk) NOT VALID;

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


-- KHO

-- dim_m_product → dim_m_product_category
ALTER TABLE xmcp_dw.dim_m_product
ADD CONSTRAINT fk_product_category
FOREIGN KEY (m_product_category_sk) REFERENCES xmcp_dw.dim_m_product_category(m_product_category_sk) NOT VALID;

-- fact_m_inoutline → dim_m_warehouse
ALTER TABLE xmcp_dw.fact_m_inoutline
ADD CONSTRAINT fk_fact_inoutline_warehouse
FOREIGN KEY (m_warehouse_sk) REFERENCES xmcp_dw.dim_m_warehouse(m_warehouse_sk) NOT VALID;

-- fact_m_inoutline → dim_m_inout
ALTER TABLE xmcp_dw.fact_m_inoutline
ADD CONSTRAINT fk_fact_inoutline_inout
FOREIGN KEY (m_inout_sk) REFERENCES xmcp_dw.dim_m_inout(m_inout_sk) NOT VALID;

-- fact_m_inoutline → dim_c_submarket
ALTER TABLE xmcp_dw.fact_m_inoutline
ADD CONSTRAINT fk_fact_inoutline_submarket
FOREIGN KEY (c_submarket_sk) REFERENCES xmcp_dw.dim_c_submarket(c_submarket_sk) NOT VALID;

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
ALTER TABLE xmcp_dw.fact_m_inoutline
ADD CONSTRAINT fk_fact_inoutline_doctype
FOREIGN KEY (c_doctype_sk) REFERENCES xmcp_dw.dim_c_doctype(c_doctype_sk) NOT VALID;

-- fact_m_inoutline → dim_m_step
ALTER TABLE xmcp_dw.fact_m_inoutline
ADD CONSTRAINT fk_fact_inoutline_step
FOREIGN KEY (m_step_sk) REFERENCES xmcp_dw.dim_m_step(m_step_sk) NOT VALID;

-- dim_inout → dim_c_department (c_department_create)
ALTER TABLE xmcp_dw.dim_m_inout
ADD CONSTRAINT fk_fact_inout_department
FOREIGN KEY (c_department_sk) REFERENCES xmcp_dw.dim_c_department(c_department_sk) NOT VALID;