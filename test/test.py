from pathlib import Path
from src.core_eda.eda import EDA
import duckdb


file = Path().home() / 'Downloads/Data/fss_item_sample/fss_items_clean.parquet'
e = EDA(file)
dict_ = e.analyze()
# query = f"""
# (query)
# query = f"""
# CREATE OR REPLACE MACRO discretize(v, l) AS (
# 	WITH t1 AS (
# 		SELECT unnest(list_distinct(l)) as j
# 	), t2 AS (
# 		SELECT COUNT(*) + 1 c FROM t1
# 	  WHERE try_cast(j AS float) <= v
# 	) FROM t2
# 	SELECT IF(v IS NULL, NULL, c)
# ) ;
#
# --Usage
# FROM 'https://raw.githubusercontent.com/thewiremonkey/factbook.csv/master/data/c2127.csv'
# SELECT name, value, discretize(value, [2,3,4,5]) AS class ;
# """
# c = duckdb.sql(query).pl()
