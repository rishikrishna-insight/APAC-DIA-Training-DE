import pyarrow.parquet as pq
import sys,os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#table = pq.read_table("lake/bronze/parquet/products/part-0.parquet")
#print(table.schema)

import duckdb

duckdb.sql("SELECT * FROM 'data_raw/sample/returns/part-00001-8d225d03-3f37-4017-ba6b-d836a945b96a-c000.snappy.parquet' LIMIT 20").show()


#conn = duckdb.connect("duckdb/warehouse.duckdb")
#conn.execute("SELECT * FROM 'lake/bronze/parquet/returns/part-0.parquet' LIMIT 20")