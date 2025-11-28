from pyspark.sql.types import *

stocks_list_schema = StructType([
    StructField("company_name", StringType(), True),
    StructField("symbol", StringType(), True),
])

snapshot_schema = StructType([
    StructField("open", StringType(), True),
    StructField("high", StringType(), True),
    StructField("low", StringType(), True),
    StructField("close", StringType(), True),
    StructField("volume", StringType(), True),
    StructField("vwap", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("transactions", StringType(), True),
    StructField("otc", StringType(), True),
])