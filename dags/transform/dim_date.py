import pandas as pd
from plugins.postgres_operators import PostgresOperators
import pandas as pd
from datetime import datetime, timedelta
import logging


# thời gian trong invoice line và invoice là như nhau
# min = (created)
# max = max(updated)

def load_dim_date():
    dw_operator = PostgresOperators(conn_id="DW_POSTGRES")
    

    sql="""
        INSERT INTO xmcp_dw.dim_date (
                date_sk, full_date, day, month, month_name,
                quarter, year, day_of_week, day_name, week_of_year, is_weekend
            )
        SELECT 
            TO_CHAR(d, 'YYYYMMDD')::INT AS date_sk,
            d AS full_date,
            EXTRACT(DAY FROM d)::INT,
            EXTRACT(MONTH FROM d)::INT,
            TO_CHAR(d, 'Month'),
            EXTRACT(QUARTER FROM d)::INT,
            EXTRACT(YEAR FROM d)::INT,
            EXTRACT(ISODOW FROM d)::INT,
            TO_CHAR(d, 'Day'),
            EXTRACT(WEEK FROM d)::INT,
            CASE WHEN EXTRACT(ISODOW FROM d) IN (6,7) THEN 1 ELSE 0 END
        FROM generate_series(
            (SELECT MIN(DATE(created)) FROM xmcp_staging.c_invoiceline),
            (SELECT MAX(DATE(updated)) FROM xmcp_staging.c_invoiceline),
            interval '1 day'
        ) d;

    """

    dw_operator.run_sql(sql)
