import json
import logging
import os
import boto3
from delta import *
from uuid import uuid4
from datetime import datetime
from datetime import timedelta
import random
import uuid
import logging
import numpy as np
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.types as t
import pyspark.sql.functions as f


def stop_all_streams(spark):
    """
    Stops all active structured streams in the given Spark session.
    
    Parameters:
    spark (SparkSession): The Spark session with active structured streams.
    
    Returns:
    None
    """
    # Get the list of active streams
    active_streams = spark.streams.active
    
    # Stop each active stream
    for stream in active_streams:
        stream.stop()
    
    # Optionally, print a message confirming that all streams have been stopped
    print(f"Stopped {len(active_streams)} active stream(s).")

    
class KinesisDataProcessor:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, spark: SparkSession):
        """
        Initialize the KinesisDataProcessor with AWS credentials and a Spark session.

        Parameters:
        - aws_access_key_id: AWS access key ID.
        - aws_secret_access_key: AWS secret access key.
        - region_name: AWS region name.
        - spark: SparkSession instance.
        """
        self.kinesis_client = self.get_kinesis_client(aws_access_key_id, aws_secret_access_key, region_name)
        self.spark = spark

    def get_kinesis_client(self, aws_access_key_id, aws_secret_access_key, region_name):
        """Initialize the Kinesis Boto3 client with error handling."""
        try:
            kinesis_client = boto3.client(
                'kinesis',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name
            )
            print("Kinesis client initialized successfully.")
            return kinesis_client
        except Exception as e:
            print(f"Failed to initialize Kinesis client: {e}")
            return None

    def fetch_kinesis_data(self, stream_name, orders_schema, shard_iterator_type="TRIM_HORIZON"):
        """
        Fetch records from a Kinesis stream and convert them into a Spark DataFrame.

        Parameters:
        - stream_name: Name of the Kinesis stream to read from.
        - orders_schema: Spark DataFrame schema to apply to the incoming records.
        - shard_iterator_type: Use 'LATEST' for most recent records.

        Returns:
        - Spark DataFrame containing the fetched records.
        """
        try:
            # Get the shard iterator for the stream
            response = self.kinesis_client.describe_stream(StreamName=stream_name)
            shard_id = response['StreamDescription']['Shards'][0]['ShardId']

            shard_iterator = self.kinesis_client.get_shard_iterator(
                StreamName=stream_name,
                ShardId=shard_id,
                ShardIteratorType=shard_iterator_type
            )['ShardIterator']

            response = self.kinesis_client.get_records(ShardIterator=shard_iterator)
            shard_iterator = response["NextShardIterator"]
            response = self.kinesis_client.get_records(ShardIterator=shard_iterator, Limit=100)
            records = response['Records']

            # Convert the Kinesis records into a list of JSON strings
            records_data = [json.loads(record['Data']) for record in records]

            # Create a Spark DataFrame from the fetched records
            df = self.spark.createDataFrame(records_data, schema=orders_schema)

            return df

        except self.kinesis_client.exceptions.ResourceNotFoundException:
            print(f"Stream {stream_name} not found.")
        except self.kinesis_client.exceptions.ProvisionedThroughputExceededException:
            print("Throughput limit exceeded, please try again later.")
        except self.kinesis_client.exceptions.InvalidArgumentException as e:
            print(f"Invalid argument: {e}")
        except Exception as e:
            print(f"An error occurred while fetching data from Kinesis: {e}")
            raise

    def save_df_as_delta(self, df, table_path):
        """
        Saves a PySpark DataFrame as a Delta table.

        Parameters:
        - df: The DataFrame to be saved.
        - table_path: The path for the Delta table.

        Raises:
        - Exception: If the saving process fails.
        """
        try:
            # Save the DataFrame to a Delta table at the specified path
            (
                df
                .write
                .format("delta")
                .mode("append")
                .save(table_path)
            )
            print(f"DataFrame successfully saved as Delta table at: {table_path}")
        except Exception as e:
            raise Exception(f"Failed to save DataFrame as Delta table: {str(e)}")

    def read_delta_table_as_stream(self, delta_table_path):
        """
        Reads a Delta table as a streaming DataFrame.

        Parameters:
        - delta_table_path: The path to the Delta table.

        Returns:
        - A streaming DataFrame representing the Delta table.

        Raises:
        - Exception: If the streaming read process fails.
        """
        try:
            # Read the Delta table as a streaming DataFrame
            streaming_df = (
                self.spark.readStream
                .format("delta")
                .load(delta_table_path)
            )
            return streaming_df  # Returning the streaming DataFrame
        except Exception as e:
            raise Exception(f"Failed to read Delta table as stream: {str(e)}")


class OrdersItemsProcessor:
    def __init__(self, spark, orders_items_path, orders_items_checkpoint_location):
        """
        Initialize the OrdersItemsProcessor with Delta table paths.

        Parameters:
        - spark: Spark Session
        - orders_items_path: Path to the Delta table for orders items.
        - orders_items_checkpoint_location: Checkpoint location for streaming.
        """
        self.spark = spark
        self.orders_items_path = orders_items_path
        self.orders_items_checkpoint_location = orders_items_checkpoint_location

    def process_orders_items_stream(self, df_order_stream: DataFrame) -> DataFrame:
        """
        Transforms the incoming order stream DataFrame by exploding order items,
        generating inventory IDs, and adding a status column.

        Parameters:
        - df_order_stream (DataFrame): Input DataFrame representing the order stream.

        Returns:
        - DataFrame: Transformed streaming DataFrame with the required columns.

        Raises:
        - ValueError: If the input DataFrame is empty or has unexpected schema.
        """
        # Validate input streaming DataFrame
        if df_order_stream is None or len(df_order_stream.columns) == 0:
            raise ValueError("Input DataFrame is empty or not provided")

        # Apply the transformation logic for the streaming DataFrame
        df_orders_items_stream = (
            df_order_stream
            .withColumn("order_exploded", f.explode(f.col("order_details.items")))
            .withColumn("package_exploded", f.explode(f.col("order_exploded.packages")))
            .withColumn("inventory_id", f.concat(f.lit("inv-"), f.expr("uuid()")))
            .withColumn("items_quantity", f.col("order_exploded.quantity") * f.col("package_exploded.quantity"))
            .withColumn("items_weight", f.col("items_quantity") * f.col("package_exploded.weight"))
            .withColumn("items_volume", f.col("items_quantity") * f.col("package_exploded.volume"))
            .withColumn("status", f.lit("PENDING"))
            .select(
                f.col("inventory_id"),
                f.col("order_id"),
                f.col("order_exploded.product_id").alias("product_id"),
                f.col("order_exploded.product_name").alias("product_name"),
                f.col("order_exploded.price").alias("order_price"),
                f.col("package_exploded.package_id").alias("package_id"),
                f.col("package_exploded.subpackage_id").alias("subpackage_id"),
                f.col("items_quantity"),
                f.col("items_weight"),
                f.col("items_volume"),
                f.col("order_details.order_timestamp").alias("order_timestamp"),
                f.col("status")
            )
        )
        # Log success message
        logging.info("Orders Stream Transformation applied successfully to the streaming DataFrame.")
        return df_orders_items_stream

    def upsert_to_orders_items(self, microBatchDF, batchId):
        """
        Upserts the incoming micro-batch DataFrame into the Delta table.

        Parameters:
        - microBatchDF: The incoming micro-batch DataFrame.
        - batchId: The unique ID of the current micro-batch.
        """
        deltaTableOrdersItems = DeltaTable.forPath(self.spark, self.orders_items_path)
        (
            deltaTableOrdersItems.alias("t")
            .merge(
                microBatchDF.alias("s"),
                "s.order_id = t.order_id AND s.inventory_id = t.inventory_id"
            )
            .whenMatchedUpdate(
                set={
                    "status": f.lit("PROCESSING"),
                    "order_timestamp": f.current_timestamp()
                },
            )
            .execute()
        )
    
    def update_orders_items_stream(self, df_orders_items_stream) -> None:
        """
        Function to process the orders items stream, filtering by PENDING status and upserting into the Delta Lake table.
        """
        # Apply transformations and define the streaming write operation
        processing_orders_items_stream = (
            df_orders_items_stream
            .filter(f.col("status") == f.lit("PENDING"))  # Filter by status
            .select(
                f.col("inventory_id"),
                f.col("order_id")
            )
            .writeStream
            .format("delta")  # Write to Delta format
            .outputMode("update")  # Use 'update' mode for micro-batch processing
            .foreachBatch(self.upsert_to_orders_items)  # Use the upsert method defined in the class
            .trigger(once=True)  # Trigger the stream to process once
            .option("path", self.orders_items_path)
            .option("checkpointLocation", f"{self.orders_items_checkpoint_location}_processing")
            .start()
        )
        # Log the status of the streaming process
        logging.info(f"Processing Orders Items stream successfully written to {self.orders_items_path}")
        
        return processing_orders_items_stream


class EventsStreamProcessor:
    def __init__(self, orders_path, events_path, events_checkpoint_location):
        """
        Initialize the EventsStreamProcessor with paths for Delta tables and checkpointing.

        Parameters:
        - orders_path: Path to the Delta table storing order data.
        - events_path: Path to the Delta table for events.
        - events_checkpoint_location: Checkpoint location for streaming.
        """
        self.orders_path = orders_path
        self.events_path = events_path
        self.events_checkpoint_location = events_checkpoint_location

    def process_events_stream(self, df_order_stream) -> None:
        """
        Processes the order stream by selecting event-specific columns and writing them 
        as a stream into a Delta table.

        Parameters:
        - df_order_stream: The input PySpark DataFrame containing order stream data.

        Raises:
        - Exception: If the streaming process fails or encounters an error.
        """
        # Define the stream transformation and writing process
        events_stream = (
            df_order_stream
            .select(
                f.col("event_id"),
                f.col("event_type"),
                f.col("event_timestamp"),
                f.col("order_id")
            )
            .writeStream
            .format("delta")
            .outputMode("append")
            .trigger(once=True)  # Using trigger(once=True) as per requirement
            .option("path", self.events_path)
            .option("checkpointLocation", self.events_checkpoint_location)
            .start()
        )
        
        # Log the status of the streaming process
        logging.info(f"Events stream successfully written to {self.events_path}")

        return events_stream

    def append_events_stream(self, spark: SparkSession) -> None:
        """
        Function to process the events stream by reading from the Delta table,
        filtering for records with status 'PROCESSING', generating event metadata, 
        and writing the stream to another Delta table.

        Parameters:
        - spark: SparkSession object
        """
        # Define the streaming read and transformations
        processing_events_stream = (
            spark
            .readStream
            .format("delta")  # Reading from Delta format
            .load(self.orders_path)  # Load from the specified Delta table path
            .filter(f.col("status") == f.lit("PROCESSING"))  # Filter for processing orders
            .withColumn(
                "event_id", f.concat(f.lit("ev-"), f.expr("uuid()"))  # Generate unique event ID
            )
            .withColumn(
                "event_type", f.lit("INVENTORY_UPDATED")  # Set the event type
            )
            .withColumn(
                "event_timestamp", f.current_timestamp()  # Add the current timestamp
            )
            .select(
                f.col("event_id"),
                f.col("event_type"),
                f.col("event_timestamp"),
                f.col("order_id")  # Select relevant columns
            )
            .writeStream
            .format("delta")  # Write the output in Delta format
            .outputMode("append")  # Append new data as it arrives
            .trigger(once=True)  # Trigger the stream to process once
            .option("path", self.events_path)  # Specify the path to write the output
            .option("checkpointLocation", f"{self.events_checkpoint_location}_processing")  # Set checkpoint location
            .start()  # Start the streaming query
        )
        
        logging.info(f"Processing events stream successfully written to {self.events_path}")
        return processing_events_stream

class OrdersStreamProcessor:
    def __init__(self, spark, orders_path, checkpoint_location):
        """
        Initialize the OrdersStreamProcessor with paths for the Delta table and checkpoint location.

        Parameters:
        - orders_path: Path to the Delta table storing orders data.
        - checkpoint_location: Location for checkpoint data to ensure fault-tolerance in streaming.
        """
        self.spark = spark
        self.orders_path = orders_path
        self.checkpoint_location = checkpoint_location

    def process_orders_stream(self, df_order_stream):
        """
        Processes the order stream by selecting specific columns related to orders and writing them 
        as a stream into a Delta table.

        Parameters:
        - df_order_stream: The input PySpark DataFrame containing order stream data.

        Raises:
        - Exception: If the streaming process fails or encounters an error.
        """
        # Define the stream transformation and writing process
        orders_stream = (
            df_order_stream
            .select(
                f.col("order_id"),
                f.col("order_details.customer_id").alias("customer_id"),
                f.col("order_details.total_weight").alias("total_weight"),
                f.col("order_details.total_volume").alias("total_volume"),
                f.col("order_details.total_amount").alias("total_price"),
                f.col("order_details.order_timestamp").alias("order_timestamp"),
                f.col("order_details.status").alias("status"),
                f.col("order_details.destination_address.lat").alias("lat"),
                f.col("order_details.destination_address.lon").alias("lon")
            )
            .writeStream
            .format("delta")
            .outputMode("append")
            .trigger(once=True)  # Trigger once to process the stream and stop
            .option("path", self.orders_path)
            .option("checkpointLocation", self.checkpoint_location)
            .start()
        )
        
        logging.info(f"Orders stream successfully written to {self.orders_path}")

        return orders_stream

    def upsert_to_orders(self, microBatchDF, batchId):
        """
        Function to upsert records into Delta Lake table for orders.
        Removes duplicate order_id records, updates status to PROCESSING, and sets the current order timestamp for matched records.

        Parameters:
        - microBatchDF: The micro-batch DataFrame containing new or updated order data.
        - batchId: The batch identifier for the micro-batch.
        - spark: The active SparkSession.
        - orders_path: Path to the Delta table storing orders data.
        """
        # Load the existing Delta table
        deltaTableOrders = DeltaTable.forPath(self.spark, self.orders_path)
        
        # Perform the merge (upsert) operation
        deltaTableOrders.alias("t").merge(
            microBatchDF.alias("s"),
            "s.order_id = t.order_id"
        ).whenMatchedUpdate(
            set={
                "status": f.lit("PROCESSING"),
                "order_timestamp": f.current_timestamp()
            }
        ).execute()

    def update_orders_stream(self, df_orders_items_stream):
        """
        Processes the orders stream, filtering by RECEIVED status and upserting into the Delta Lake table.

        Parameters:
        - df_orders_items_stream: The input PySpark DataFrame containing order stream data.
        - spark: The active SparkSession for accessing Delta Lake tables.
        """
        # Define the streaming write operation with upsert logic
        processing_orders_stream = (
            df_orders_items_stream
            .filter(f.col("status") == f.lit("RECEIVED"))  # Filter by status
            .select(f.col("order_id"))  # Select order_id for upsert
            .writeStream
            .format("delta")  # Write to Delta format
            .outputMode("update")  # Use 'update' mode for micro-batch processing
            .foreachBatch(self.upsert_to_orders)
            .trigger(once=True)  # Trigger once to process the stream and stop
            .option("path", self.orders_path)
            .option("checkpointLocation", f"{self.checkpoint_location}_processing")
            .start()
        )
        
        (f"Processing Orders stream successfully written to {self.orders_path}")
        
        return processing_orders_stream


class PackagesStreamProcessor:
    def __init__(self, spark, packages_path, checkpoint_location):
        """
        Initialize the PackagesStreamProcessor with necessary paths and configurations.

        Parameters:
        - spark: The SparkSession object.
        - packages_path: Path to the Delta table where the package data will be stored.
        - checkpoint_location: Location for checkpoint data to ensure fault-tolerance in streaming.
        """
        self.spark = spark
        self.packages_path = packages_path
        self.checkpoint_location = checkpoint_location

    def upsert_to_package(self, microBatchDF: DataFrame, batchId: int):
        """
        Upserts the incoming micro-batch DataFrame into the Delta table for packages.

        Parameters:
        - microBatchDF: The micro-batch DataFrame from the streaming source.
        - batchId: The unique identifier for the micro-batch.
        """
        # Define the Delta table path
        deltaTableProducts = DeltaTable.forPath(
            self.spark, self.packages_path
        )

        # Deduplicate and aggregate the micro-batch DataFrame
        deduplicatedBatchDF = (
            microBatchDF
            .filter(f.col("status") == f.lit("PENDING"))
            .groupBy("package_id", "subpackage_id")
            .agg(
                f.sum("items_quantity").alias("items_quantity"),
            )
        )
        
        # Perform the upsert operation (merge)
        deltaTableProducts.alias("t").merge(
            deduplicatedBatchDF.alias("s"),
            "s.package_id = t.package_id AND s.subpackage_id = t.subpackage_id"
        ).whenMatchedUpdate(
            condition=f.col("t.stock_quantity") >= f.col("s.items_quantity"),
            set={
                "stock_quantity": f.col("t.stock_quantity") - f.col("s.items_quantity"),
            }
        ).execute()

        logging.info(f"Batch {batchId} upserted successfully into package table.")

    def update_packages_stream(self, df_orders_items_stream: DataFrame):
        """
        Initializes and starts a streaming query to update the packages table using the upsert function.

        Parameters:
        - df_orders_items_stream (DataFrame): The input streaming DataFrame containing orders items data.

        Returns:
        - StreamingQuery: The StreamingQuery object representing the started stream.
        """
        # Start the streaming process with upsert logic using foreachBatch
        update_products_stream = (
            df_orders_items_stream
            .select(
                f.col("package_id"),
                f.col("subpackage_id"),
                f.col("items_quantity"),
                f.col("status")
            )
            .writeStream
            .format("delta")
            .outputMode("update")
            .foreachBatch(self.upsert_to_package)  # Use the class method for upserting
            .option("path", self.packages_path)
            .option("checkpointLocation", self.checkpoint_location)
            .trigger(once=True)  # Process the stream once for batch processing
            .start()
        )

        logging.info(f"Packages stream successfully written to {self.packages_path}")
        return update_products_stream
