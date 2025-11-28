from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from functools import reduce
import logging

# Set up logger for this module
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create a Spark session for the ETL job"""
    return SparkSession.builder \
        .appName("Stocks_Analysis_ETL") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def extract_csv_data(
        spark: SparkSession, 
        input_path: str,
        schema: StructType
):
    """Function for reading CSV data"""

    logger.info(f"Reading data from {input_path}")

    #Read data using spark
    df = spark.read.csv(input_path, header=True, schema=schema, mode="PERMISSIVE")

    total_records = df.count()
    logger.info(f"Loaded {total_records} total records")

    return df

def create_fact_table(
        spark: SparkSession,
        raw_df: DataFrame,
        raw_list: list,
        path:str):

    stocks = []

    logger.info(f"Reading all stock data")

    for symbol in raw_list:
        df = spark.read.parquet(path+f'{symbol}/')
        df = df.select('close','timestamp')\
            .withColumn('timestamp', f.from_utc_timestamp(f.to_timestamp(f.col('timestamp').cast("long")/1000),"US/Eastern")) \
            .withColumn('close', f.col('close').cast('double'))\
            .withColumn('symbol', f.lit(symbol))\
            .withColumn("day", f.day(f.col("timestamp")))\
            .withColumn("week", f.weekofyear(f.col("timestamp")))\
            .withColumn("month", f.month(f.col("timestamp")))\
            .withColumn("year", f.year(f.col("timestamp")))\

        stocks.append(df)

    stocks = reduce(DataFrame.unionByName,stocks)
    stocks = stocks.join(raw_df,on='symbol',how='left')

    return stocks

def calculate_relative_increase(df: DataFrame):
        
    df_relative_increase = df.groupBy("symbol","company_name").agg(
        f.first("close").alias("first_price"),
        f.last("close").alias("last_price"))
    df_relative_increase = df_relative_increase.withColumn("relative_increase", 
                                                           (f.col("last_price") - f.col("first_price")) / f.col("first_price"))
    
    return df_relative_increase

def calculate_portofolio_value(
        df:DataFrame, 
        investment_per_stock: int
        ):
    
    df_first_last = df.groupBy("symbol","company_name").agg(
        f.first("close").alias("first_price"),
        f.last("close").alias("last_price"))
    
    df_returns = df_first_last.withColumn("return_ratio", f.col("last_price") / f.col("first_price"))\
                .withColumn("investment_value", f.lit(investment_per_stock) * f.col("return_ratio"))

    portfolio_value = df_returns.agg(f.sum("investment_value").alias("total_value")).collect()[0]["total_value"]
    
    return portfolio_value

def calculate_monthly_cagr(df:DataFrame, start_month:int, end_month:int):

    df_cagr = df.filter(f.col("month").isin([start_month,end_month]))
    df_cagr = df_cagr.groupBy("symbol","company_name").pivot("month").agg(f.first("close"))
    df_cagr = df_cagr.withColumnRenamed(str(start_month), "start_price")\
                    .withColumnRenamed(str(end_month), "end_price")

    df_cagr = df_cagr.withColumn("monthly_cagr", pow(f.col("end_price")/f.col("start_price"), 1/(end_month-start_month+1)) - 1)

    return df_cagr

def calculate_weekly_delta(df:DataFrame):

    df_weekly = df.groupBy("symbol","company_name", "week").agg(
        f.max("close").alias("max_price"), 
        f.min("close").alias("min_price"))

    # Calculate weekly difference
    df_weekly = df_weekly.withColumn("weekly_delta", -(f.col("max_price") - f.col("min_price")))

    return df_weekly

def run_analysis(df:DataFrame):

    answers = []

    #Question 1
    question_1_str = "Which stock has had the greatest relative increase in price in this period?"
    answer_1 = calculate_relative_increase(df).orderBy(f.desc("relative_increase")).limit(1).collect()

    answer_entry = {
        'question_id': 1,
        'question': question_1_str,
        'answer': f"'{answer_1[0]['company_name']}' with an increase of {round(answer_1[0]['relative_increase'],2)*100}%",
        'symbol': answer_1[0]['symbol'],
        'value': round(answer_1[0]['relative_increase'],2)
    }
    answers.append(answer_entry)

    #Question 2
    question_2_str = "If you had invested $1 million at the beginning of this period by purchasing $10,000 worth of shares in every company in the list equally, how much would you have today?"
    answer_2 = calculate_portofolio_value(df,10000)

    answer_entry = {
        'question_id': 2,
        'question': question_2_str,
        'answer': f"You would have approximately {round(answer_2)}.",
        'symbol': "",
        'value': round(answer_2)
    }
    answers.append(answer_entry)

    #Question 3
    question_3_str = "Which stock had the greatest value in monthly CAGR between January and June?"
    answer_3 = calculate_monthly_cagr(df,1,6).orderBy(f.col("monthly_cagr").desc()).limit(1).collect()

    answer_entry = {
        'question_id': 3,
        'question': question_3_str,
        'answer': f"'{answer_3[0]['company_name']}' with a CAGR of {round(answer_3[0]['monthly_cagr'],2)}.",
        'symbol': answer_3[0]['symbol'],
        'value': round(answer_3[0]['monthly_cagr'],2)
    }
    answers.append(answer_entry)

    #Question 4
    question_4_str = "During the year, which stock had the greatest decrease in value within a single week and which week was this?"
    answer_4 = calculate_weekly_delta(df).orderBy(f.asc("weekly_delta")).limit(1).collect()

    answer_entry = {
        'question_id': 4,
        'question': question_4_str,
        'answer': f"'{answer_4[0]['company_name']}' with a decrease of {round(answer_4[0]['weekly_delta'],2)} in week {answer_4[0]['week']}.",
        'symbol': answer_4[0]['symbol'],
        'value': answer_4[0]['week']
    }
    answers.append(answer_entry)

    return answers