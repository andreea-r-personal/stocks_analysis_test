from dotenv import load_dotenv
import logging
import sys
import os
import time
import json
from massive import RESTClient
from datetime import datetime
import traceback

# Import defined functions
from src.etl_pipeline import *
from src.schema_definition import stocks_list_schema, snapshot_schema

def setup_logging():
    """Basic logging setup"""

    t = datetime.now().strftime("%Y%m%d")

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'logs/etl_run_{t}.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def main():

    load_dotenv()
    #Initialise logging
    os.makedirs('logs', exist_ok=True)

    logger = setup_logging()
    logger.info("Starting ETL Pipeline")

    #Track runtime
    start_time = datetime.now()
    t = start_time.strftime("%Y%m%d")

    #Get environment variables
    start_date = os.environ['START_DATE']
    end_date = os.environ['END_DATE']
    local_raw = os.environ['LOCAL_RAW']
    local_processed = os.environ['LOCAL_PROCESSED']
    local_outputs = os.environ['LOCAL_OUTPUTS']
    api_key = os.environ['MASSIVE_API_KEY']
    reload = True if os.environ["RELOAD"] == "True" else False

    os.makedirs(local_processed, exist_ok=True)
    os.makedirs(local_outputs, exist_ok=True)

    #API has a 5 requests per minute limit (approx 12 seconds, chose 15 to be safe)
    wait = 15

    try:
        spark = create_spark_session()
        logger.info("Spark session created")

        #Step 1: Read CSV to DataFrame and get a list of all 
        symbols_df = extract_csv_data(spark,local_raw + "stocks.csv",stocks_list_schema)
        symbols_list = symbols_df.select('symbol').rdd.flatMap(lambda x: x).collect()

        #Step 2: Extract snapshots for each symbol from Massive REST API
        client = RESTClient(api_key = api_key, trace=True)

        if(reload):
            for symbol in symbols_list:
                #Connection to the REST API
                logger.info(f"Extracting data for {symbol}")
                snapshots = client.get_aggs(symbol, 1, "day", start_date, end_date)

                #Read to dataframe
                logger.info(f"Writing data for {symbol}")
                df = spark.createDataFrame(snapshots, schema=snapshot_schema)
                df.coalesce(1).write.mode("overwrite").parquet(local_raw + 'stocks/' + f'{symbol}')

                #Added delay to avoid 429 error on number and frequency of requests (rate limiting)
                #Avoided threading requests as this compounds rate limiting issue
                #TODO: Investigate if batching is an option
                time.sleep(wait)

        #Step 3: Consolidate all snapshots into one fact table
        stocks_df = create_fact_table(spark,symbols_df,symbols_list,local_raw + 'stocks/')
        logger.info(f"Writing fact data")
        stocks_df.repartition("symbol").write.mode("overwrite").parquet(local_processed + 'fact')

        #Step 4: Perform calculations
        logger.info(f"Performing calculations")
        answers = run_analysis(stocks_df)

        #Step 5: Save to JSON
        logger.info(f"Producing final output")
        with open(local_outputs + f"analysis_{t}.json", "w") as f:
            json.dump(answers, f, indent=4)
    
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    
    finally:
        spark.stop()
        logger.info("Spark session closed")

if __name__ == "__main__":
    main()