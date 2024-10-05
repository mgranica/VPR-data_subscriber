from delta import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

def create_spark_session(aws_access_key_id, aws_secret_access_key, cores_number="2"):
    """
    Create and configure a Spark session with AWS credentials.
    
    :param aws_access_key_id: AWS access key ID.
    :param aws_secret_access_key: AWS secret access key.
    :param cores_number: Number of cores to use for the Spark session (default is 2).
    :return: SparkSession
    """
    try:
        # Configure the Spark session
        conf = (
            SparkConf()
            .setAppName("VPR-data_landing")
            .set("spark.hadoop.fs.s3a.endpoint", "s3.eu-south-2.amazonaws.com")
            .set("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2")
            .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
            .set("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
            .setMaster(f"local[{cores_number}]")  # replace the * with your desired number of cores. * for use all.
        )
        
        # Build the Spark session
        builder = SparkSession.builder.config(conf=conf)
        my_packages = ["org.apache.hadoop:hadoop-aws:3.3.1"]
        spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()

        return spark

    except Exception as e:
        print(f"An error occurred while creating the Spark session: {str(e)}")
        raise  # Re-raise the exception after logging or handling