-- VW: vw_c_invoice_full
CREATE OR REPLACE VIEW kd_dw.vw_c_invoice_full AS
SELECT
    

    i.c_invoice_sk as c_invoice_sk,
    i.documentno as documentno,
    i.currency_rate as currency_rate,
    
    p.name as partner_name,
    p.value as partner_value,

    pg.name as group_name,
    pg.value as group_value,

    dt.name as doctype_name,
    dt.docbasetype as doctype_base_type,

    c.name as currency_name
    
    
    
    
FROM kd_dw.dim_c_invoice i
LEFT JOIN kd_dw.dim_c_bpartner p on p.c_bpartner_sk=i.c_bpartner_sk
LEFT JOIN kd_dw.dim_c_bp_group pg on pg.c_bp_group_sk = p.c_bp_group_sk
LEFT JOIN kd_dw.dim_c_doctype dt on dt.c_doctype_sk = i.c_doctype_sk
LEFT JOIN kd_dw.dim_c_currency c on c.c_currency_sk=i.c_currency_sk;


-- VW: vw_c_submarket_full
CREATE OR REPLACE VIEW kd_dw.vw_c_submarket_full as
select 
    sm.c_submarket_sk as c_submarket_sk,
    sm.name as submarket_name,
    sm.value as submarket_value,
    m.name as market_name,
    m.name_chuan as market_name_chuan,
    m.value as market_value,
    m.lat as vi_do,
    m.long as kinh_do
    
from kd_dw.dim_c_submarket sm   
LEFT JOIN kd_dw.dim_c_market m on sm.c_market_sk = m.c_market_sk;


-- VW: vw_m_product_full
CREATE OR REPLACE VIEW kd_dw.vw_m_product_full as
select 
    p.m_product_sk,
    p.name as product_name,
    p.value as product_value,
    pt.c_producttype_sk,
    pt.name as producttype_name,
    pt.value as producttype_value
    
from kd_dw.dim_m_product p
LEFT JOIN kd_dw.dim_c_producttype pt on p.c_producttype_sk = pt.c_producttype_sk
where p.is_current=1;


-- VW: vw_sale_summary
CREATE OR REPLACE VIEW kd_dw.vw_sales_summary AS
SELECT
--fact
    f.c_invoiceline_sk,
    f.qtyinvoiced,
    f.priceactual,
    
    f.linenetamt,
    f.linenetamtconvert,
    
    f.discount,
    f.discountamt,
    f.discountamtconvert,

    f.discount2amt,
    f.discountamt2convert,

    f.percenttax,
    f.taxamount,
    f.taxamountconvert,
    
    f.grandtotal,
    f.grandtotalconvert,


-- dim product
    p.product_name,
    p.product_value,
    p.producttype_name,
    
-- dim invoice
    inv.documentno      AS invoice_no,
    inv.partner_name   AS parner_name,
    inv.group_name   AS group_name,
    inv.doctype_name    AS doctype_name,

-- market & submarket
    sm.submarket_name as submarket_name,
    sm.market_name as market_name,

-- dim date
    d.full_date as sales_date,
    d.day as sales_day,
    d.month AS sales_month,
    d.month_name AS sales_month_name,
    d.quarter AS sales_quarter,
    d.year AS sales_year,
    d.day_of_week,
    d.week_of_year,
    
-- dim khác
    org.name as ad_org_name,
    t.name as tax_name,
    u.name as uom_don_vi
    

FROM kd_dw.fact_c_invoiceline f
LEFT JOIN kd_dw.vw_c_invoice_full inv ON f.c_invoice_sk = inv.c_invoice_sk
LEFT JOIN kd_dw.vw_c_submarket_full sm ON f.c_submarket_sk = sm.c_submarket_sk
LEFT JOIN kd_dw.vw_m_product_full p ON f.m_product_sk = p.m_product_sk
LEFT JOIN kd_dw.dim_date d ON f.date_sk = d.date_sk
LEFT JOIN kd_dw.dim_ad_org org ON f.ad_org_sk = org.ad_org_sk
LEFT JOIN kd_dw.dim_c_tax t on t.c_tax_sk=f.c_tax_sk
LEFT JOIN kd_dw.dim_c_uom u on u.c_uom_sk=f.c_uom_sk;

