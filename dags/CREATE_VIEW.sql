-- VW: vw_c_invoice_full
CREATE OR REPLACE VIEW xmcp_dw.vw_c_invoice_full AS
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
    
    
    
    
FROM xmcp_dw.dim_c_invoice i
LEFT JOIN xmcp_dw.dim_c_bpartner p on p.c_bpartner_sk=i.c_bpartner_sk
LEFT JOIN xmcp_dw.dim_c_bp_group pg on pg.c_bp_group_sk = p.c_bp_group_sk
LEFT JOIN xmcp_dw.dim_c_doctype dt on dt.c_doctype_sk = i.c_doctype_sk
LEFT JOIN xmcp_dw.dim_c_currency c on c.c_currency_sk=i.c_currency_sk;


-- VW: vw_c_submarket_full
CREATE OR REPLACE VIEW xmcp_dw.vw_c_submarket_full as
select 
    sm.c_submarket_sk as c_submarket_sk,
    sm.name as submarket_name,
    sm.value as submarket_value,
    m.name as market_name,
    m.name_chuan as market_name_chuan,
    m.value as market_value,
    m.lat as vi_do,
    m.long as kinh_do
    
from xmcp_dw.dim_c_submarket sm   
LEFT JOIN xmcp_dw.dim_c_market m on sm.c_market_sk = m.c_market_sk;


-- VW: vw_m_product_producttype
CREATE OR REPLACE VIEW xmcp_dw.vw_m_product_producttype as
select 
    p.m_product_sk,
    p.name as product_name,
    p.value as product_value,
    pt.name as producttype_name,
    pt.value as producttype_value
    
from xmcp_dw.dim_m_product p
LEFT JOIN xmcp_dw.dim_c_producttype pt on p.c_producttype_sk = pt.c_producttype_sk
where p.is_current=1;


-- VW: vw_sale_summary
CREATE OR REPLACE VIEW xmcp_dw.vw_sales_summary AS
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
    

FROM xmcp_dw.fact_c_invoiceline f
LEFT JOIN xmcp_dw.vw_c_invoice_full inv ON f.c_invoice_sk = inv.c_invoice_sk
LEFT JOIN xmcp_dw.vw_c_submarket_full sm ON f.c_submarket_sk = sm.c_submarket_sk
LEFT JOIN xmcp_dw.vw_m_product_producttype p ON f.m_product_sk = p.m_product_sk
LEFT JOIN xmcp_dw.dim_date d ON f.date_sk = d.date_sk
LEFT JOIN xmcp_dw.dim_ad_org org ON f.ad_org_sk = org.ad_org_sk
LEFT JOIN xmcp_dw.dim_c_tax t on t.c_tax_sk=f.c_tax_sk
LEFT JOIN xmcp_dw.dim_c_uom u on u.c_uom_sk=f.c_uom_sk;

-- VW: vw_m_inout_full
CREATE OR REPLACE VIEW xmcp_dw.vw_m_inout_full as
SELECT 

    i.m_inout_sk as m_inout_sk,

    i.inout_type as inout_type,
    i.docstatus as docstatus,
    i.register_status as register_status,
    i.isinvoiced as isinvoiced,
    i.isprinted as isprinted,
    i.currencyrate as currencyrate,
    
    p.name as partner_name,
    p.value as partner_value,

    pg.name as group_name,
    pg.value as group_value,

    dt.name as doctype_name,
    dt.docbasetype as doctype_base_type,

    dp.name as department_name,
    dp.value as department_value
    
from xmcp_dw.dim_m_inout i
LEFT JOIN xmcp_dw.dim_c_department dp on dp.c_department_sk=i.c_department_sk
LEFT JOIN xmcp_dw.dim_c_doctype dt on dt.c_doctype_sk=i.c_doctype_sk
LEFT JOIN xmcp_dw.dim_c_bpartner p on p.c_bpartner_sk=i.c_bpartner_sk
LEFT JOIN xmcp_dw.dim_c_bp_group pg on pg.c_bp_group_sk = p.c_bp_group_sk;



-- VW: vw_m_product_product_category
CREATE OR REPLACE VIEW xmcp_dw.vw_m_product_product_category as
select 
    p.m_product_sk,
    p.name as product_name,
    p.value as product_value,

    pc.name as product_category_name,
    pc.value as product_category_value

from xmcp_dw.dim_m_product p
LEFT JOIN xmcp_dw.dim_m_product_category pc on p.m_product_category_sk = pc.m_product_category_sk
where p.is_current=1;


-- VW: vw_m_inoutline_full
CREATE OR REPLACE VIEW xmcp_dw.vw_m_inoutline_full as
SELECT 
-- fact
    im.m_inoutline_sk,
    im.movementtype,

    im.qtyrequiered,
    im.qty,
    
    im.qtyentered,

    im.movementqty,
    -- HS quy đổi
    im.rateconverted,
    
    im.priceentered,
    im.pricecost,
    
    im.amountconvert,
    im.linenetamount,
    
    im.totaltaxamount,
    im.taxamountconvert,
    
    im.totallines,
    im.totallinesconvert,
    
    im.amountallocation,
    im.distributionamount,
    
    im.receiptdate,

    im.lifetime,

    im.dateexpiration,

    im.classification,

    im.timeused,
    im.timestock,
    im.qtyonhand,


-- dim locator
    l.name as locator_name,
    l.value as locator_value,

-- dim step
    s.name as step_name,
    s.value as step_value,

-- dim warehouse
    w.name as warehouse_name,
    w.value as warehouse_value,

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
    u.name as uom_don_vi

FROM xmcp_dw.fact_m_inoutline im 
LEFT JOIN xmcp_dw.vw_m_inout_full m on m.m_inout_sk=im.m_inout_sk
LEFT JOIN xmcp_dw.dim_c_uom u on u.c_uom_sk=im.c_uom_sk
LEFT JOIN xmcp_dw.dim_m_locator l on l.m_locator_sk=im.m_locator_sk
LEFT JOIN xmcp_dw.dim_ad_org org ON org.ad_org_sk = im.ad_org_sk
LEFT JOIN xmcp_dw.dim_m_step s ON s.m_step_sk = im.m_step_sk
LEFT JOIN xmcp_dw.dim_m_warehouse w ON w.m_warehouse_sk = im.m_warehouse_sk
LEFT JOIN xmcp_dw.vw_m_product_product_category pc ON pc.m_product_sk = im.m_product_sk

LEFT JOIN xmcp_dw.dim_date d ON im.date_sk = d.date_sk



