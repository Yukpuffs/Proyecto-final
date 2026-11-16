import os
from pyspark.sql import SparkSession, functions as F # Import SparkSession and functions from PySpark
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType # Import data types from PySpark

spark = (SparkSession.builder # Create Spark session
         .appName("TrollsTwitter")
         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2")
         .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION"))
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

rutas = [ # Use EMRFS S3 for read and use the data in bucket
    "s3a://analisis-tweets/raw/IRAhandle_tweets_2.csv",
    "s3a://analisis-tweets/raw/IRAhandle_tweets_3.csv",
]

class leer_datos_s3:
    def __init__(self, spark):
        self.spark = spark

    def leer_datos(self):
        schema = StructType([  # Define the schema for the CSV columns (all as text, converted below)
            StructField("external_author_id", StringType()),
            StructField("author", StringType()),
            StructField("content", StringType()),
            StructField("region", StringType()),
            StructField("language", StringType()),
            StructField("publish_date", StringType()),
            StructField("harvested_date", StringType()),
            StructField("following", StringType()),
            StructField("followers", StringType()),
            StructField("updates", StringType()),
            StructField("post_type", StringType()),
            StructField("account_type", StringType()),
            StructField("new_june_2018", StringType()),
            StructField("retweet", StringType()),
            StructField("account_category", StringType()),
        ])

        df = (self.spark.read  # Read the CSV files from the bucket
              .schema(schema)
              .option("header", True)
              .option("multiLine", True)
              .option("quote", '"')
              .option("escape", '"')
              .csv(rutas))


        print("Datos originales cargados desde S3")

        return df

if __name__ == "__main__":
    lector = leer_datos_s3(spark)
    lector.leer_datos()





