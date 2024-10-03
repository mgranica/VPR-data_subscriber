import sys
import configparser
import argparse
import os
import time
import logging

from spark_session import create_spark_session
from schemas import orders_schema
from functions import KinesisDataProcessor, OrdersItemsProcessor, EventsStreamProcessor, OrdersStreamProcessor, PackagesStreamProcessor


def setup_logging():
    """
    Set up logging configuration.
    """
    logging.basicConfig(
        level=logging.INFO,  # Log level can be adjusted (DEBUG, INFO, WARNING, ERROR)
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),  # Log to console
            logging.FileHandler("app.log", mode='a')  # Optionally log to a file
        ]
    )  

def load_aws_credentials(profile_name="default"):
    # Check the environment flag
    environment = os.getenv('ENVIRONMENT', 'LOCAL')

    if environment == 'GITHUB_ACTIONS':
        # Load credentials from environment variables set in GitHub Actions
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

        if not aws_access_key_id or not aws_secret_access_key:
            logging.error("AWS credentials not found in GitHub Actions environment variables.")
            sys.exit(1)

        logging.info("Successfully loaded credentials from GitHub Actions environment.")
    else:
        # Load credentials from the .aws/credentials file (local development)
        try:
            credentials = configparser.ConfigParser()
            credentials.read(os.path.join(os.path.dirname(__file__), '..', '.aws', 'credentials'))
            
            logging.info("Successfully loaded credentials variables from .aws file.")
        except Exception as e:
            logging.error(f"Error loading .aws file: {e}")
            sys.exit(1)

        aws_access_key_id = credentials[profile_name]["aws_access_key_id"]
        aws_secret_access_key = credentials[profile_name]["aws_secret_access_key"]

        if not aws_access_key_id or not aws_secret_access_key:
            logging.error("AWS credentials not found.")
            sys.exit(1)

    return aws_access_key_id, aws_secret_access_key

def load_aws_config():
    """
    Loads AWS configuration settings from the .aws/config file.

    :param profile_name: The profile name in the AWS config file (default: "default").
    :return: The region_name as a string.
    """
    try:
        config = configparser.ConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), '..', '.aws', 'config'))
        logging.info("Successfully loaded config variables from .aws file.")

        return config
    except Exception as e:
        logging.error(f"Error loading .aws file: {e}")
        sys.exit(1)
        
        
        
def main():
    # Load credentials and configuration
    aws_access_key_id, aws_secret_access_key = load_aws_credentials()
    aws_config = load_aws_config()
    bucket_name = aws_config["paths"]["BUCKET_NAME"]
    region_name = aws_config["default"]["REGION"]
    stream_name = aws_config["default"]["STREAM_NAME"]
    # Paths
    orders_wa_stream_path = "s3a://vproptimiserplatform/orders/delta/bronze/orders_stream_5"
    orders_path = 's3a://vproptimiserplatform/orders/delta/bronze/orders'
    orders_items_path = "s3a://vproptimiserplatform/orders/delta/bronze/orders_items"
    events_path = 's3a://vproptimiserplatform/orders/delta/bronze/events'
    packages_path = "s3a://vproptimiserplatform/orders/gold/package_table"
    # CHECKPOINTS
    events_checkpoint = 's3a://vproptimiserplatform/orders/delta/checkpoints/events'
    orders_checkpoint = 's3a://vproptimiserplatform/orders/delta/checkpoints/orders'
    orders_items_checkpoint = "s3a://vproptimiserplatform/orders/delta/checkpoints/orders_items"
    orders_items_update_checkpoint = "s3a://vproptimiserplatform/orders/delta/checkpoints/orders_items_update"
    package_update_checkpoint_location = "s3a://vproptimiserplatform/orders/delta/checkpoints/package_update"
    
    # Create Spark session
    spark = create_spark_session(aws_access_key_id, aws_secret_access_key)
    # Kinesis Workaround
    kinesis_processor = KinesisDataProcessor(aws_access_key_id, aws_secret_access_key, region_name, spark)
    # Fetch data from Kinesis stream
    df = kinesis_processor.fetch_kinesis_data(stream_name, orders_schema)
    # Save the DataFrame to Delta
    kinesis_processor.save_df_as_delta(df, orders_wa_stream_path)
    # Read Delta table as a stream
    df_order_stream = kinesis_processor.read_delta_table_as_stream(orders_wa_stream_path)
    
    ## Events stream processor
    events_stream_processor = EventsStreamProcessor(orders_path, events_path, events_checkpoint)
    ## Orders stream processor
    orders_stream_processor = OrdersStreamProcessor(spark, orders_path, orders_checkpoint)
    ## Orders Items processor
    orders_items_stream_processor = OrdersItemsProcessor(spark, orders_items_path, orders_items_checkpoint)
    ## Packages Stream
    package_stream_processor = PackagesStreamProcessor(spark, packages_path, package_update_checkpoint_location)
    
    # Process the events stream
    events_stream = events_stream_processor.process_events_stream(df_order_stream)
    # Process the orders stream
    orders_stream = orders_stream_processor.process_orders_stream(df_order_stream)    
    # Process the orders items stream
    df_orders_items_stream = orders_items_stream_processor.process_orders_items_stream(df_order_stream)
    # Process the package update stream
    package_stream = package_stream_processor.update_packages_stream(df_orders_items_stream)
    # Process the orders items update stream
    orders_items_processing_stream = orders_items_stream_processor.update_orders_items_stream(df_orders_items_stream)
    # Process the orders update stream
    orders_processing_stream = orders_stream_processor.update_orders_stream(df_orders_items_stream)
    # Process the inventory events append stream
    events_processing_stream = events_stream_processor.append_events_stream(spark)
    
    

    
    
    
    
    
    
    
if __name__ == "__main__":
    setup_logging()
    main()