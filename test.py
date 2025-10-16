from sqlalchemy import create_engine
import pandas as pd
import oracledb
import cx_Oracle

# Khởi động thick mode (trỏ tới thư mục Instant Client)
oracledb.init_oracle_client(lib_dir=r"C:\Users\hoangbd1\oracle\instantclient_19_28")

# Service name
# engine = create_engine("oracle+oracledb://xmcp:Admin123@10.0.0.10:1521/?service_name=orcl.ximangcampha.net")
engine = create_engine(
    "oracle+cx_oracle://xmcp:Admin123@10.0.0.10:1521/?service_name=orcl.ximangcampha.net"
)


# Ví dụ query
sql = "select * from M_Product where M_Product_ID=10602082"

# Đọc dữ liệu vào pandas DataFrame
df = pd.read_sql(sql, engine)

print(df.head())