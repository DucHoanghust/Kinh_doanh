DROP SCHEMA IF EXISTS kd_stag CASCADE;

CREATE SCHEMA IF NOT EXISTS kd_stag;

CREATE TABLE IF NOT EXISTS kd_stag.c_tax (
    c_tax_id INT,
    c_taxcategory_id INT,
    name VARCHAR(255),
    rate INT,
    value VARCHAR(10),
    isactive VARCHAR(2),
    created TIMESTAMP,
    updated TIMESTAMP
);




CREATE TABLE IF NOT EXISTS kd_stag.c_submarket (
    c_submarket_id INT,
    c_market_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive INT,
    created TIMESTAMP,  
    updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS kd_stag.c_market (
    c_market_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    created TIMESTAMP,
    updated TIMESTAMP
);


CREATE TABLE IF NOT EXISTS kd_stag.m_product (
    m_product_id INT,
    c_uom_id INT,
    c_producttype_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive INT,
    created TIMESTAMP,
    updated TIMESTAMP
);


CREATE TABLE IF NOT EXISTS kd_stag.c_invoice (
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

CREATE TABLE IF NOT EXISTS kd_stag.c_currency (
    c_currency_id INT,
    name VARCHAR(255),
    isactive INT,
    created TIMESTAMP,
    updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS kd_stag.c_bpartner (
    c_bpartner_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive INT,
    created TIMESTAMP,
    updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS kd_stag.c_bp_group (
    c_bp_group_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive INT,
    created TIMESTAMP,
    updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS kd_stag.ad_org (
    ad_org_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive INT,
    created TIMESTAMP,
    updated TIMESTAMP
);


CREATE TABLE IF NOT EXISTS kd_stag.c_uom (
    c_uom_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    uomtype VARCHAR(10),
    isactive INT,
    created TIMESTAMP,
    updated TIMESTAMP
);


CREATE TABLE IF NOT EXISTS kd_stag.c_producttype (
    c_producttype_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive VARCHAR(2),
    created TIMESTAMP,
    updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS kd_stag.c_invoiceline (
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

CREATE TABLE IF NOT EXISTS kd_stag.c_doctype (
    c_doctype_id INT,
    name VARCHAR(255),
    printname VARCHAR(255),
    docbasetype VARCHAR(10),
    issotrx VARCHAR(2),
    isactive VARCHAR(2),
    created TIMESTAMP,
    updated TIMESTAMP
);


CREATE TABLE IF NOT EXISTS kd_stag.c_department (
    c_department_id INT,
    ad_org_id INT,
    name VARCHAR(255),
    value VARCHAR(255),
    isactive VARCHAR(2),
    created TIMESTAMP,
    updated TIMESTAMP
);


CREATE TABLE IF NOT EXISTS kd_stag.hr_employee (
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
