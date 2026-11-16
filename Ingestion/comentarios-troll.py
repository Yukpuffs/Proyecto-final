import os
from pyspark.sql import SparkSession, functions as F # Import SparkSession and functions from PySpark
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType # Import data types from PySpark

spark = (SparkSession.builder # Create Spark session
         .appName("TrollsTwitter")
         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.5.0")
         .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION"))
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

rutas = [ # Use EMRFS S3 for read and use the data in bucket
    "s3://analisis-tweets/IRAhandle_tweets_2.csv",
    "s3://analisis-tweets/IRAhandle_tweets_3.csv",
]



