import os # Import os module for environment variable access
from pyspark.sql import SparkSession # Import SparkSession from PySpark for creating a Spark session
from Ingestion.readS3 import leer_datos_s3 # Import module for reading data from S3
from Processing.transformData import transformar_datos # Import module for transforming data
from Processing.saveparquet import GuardarParquet # Import module for saving DataFrame to Parquet format
from Processing.createnodes import CargadorNeo4j # Import module for loading data into Neo4j
from Processing.bigquery import CargadorBigQuery


RUTA_PARQUET = ("s3a://analisis-tweets/processed/comentarios_parquet/") # Define the S3 path for storing processed Parquet files

def main():

    spark = ( # Create a Spark session 
        SparkSession.builder
        .appName("AnalisisDesinformacion")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        if os.environ.get("SKIP_ETL") == "1": # Check if the environment variable SKIP_ETL is set to "1" to skip the ETL process
            print("Reutilizando Parquet procesado desde S3...")
            df = spark.read.parquet(RUTA_PARQUET) # Read the processed Parquet file from S3 into a DataFrame

        else:

            print("Iniciando ingesta...")

            lector = leer_datos_s3(spark)
            df_crudo = lector.leer_datos()

            print("Iniciando transformación...")

            transformador = transformar_datos(df_crudo)
            df_procesado = transformador.transformar()

            print("Estructura de los datos procesados:")
            df_procesado.printSchema()

            print("Guardando datos procesados en Parquet...")

            guardar = GuardarParquet(df_procesado, RUTA_PARQUET)
            guardar.guardar_en_parquet()

            print("Leyendo Parquet desde S3...")

            df = spark.read.parquet(RUTA_PARQUET)

        ruta_muestra = os.environ.get("SAMPLE_PARQUET_PATH","s3a://analisis-tweets/processed/prueba_500k_checkpoint/") # Define the S3 path for storing a sample of the Parquet file

        Path = spark._jvm.org.apache.hadoop.fs.Path

        ruta = Path(ruta_muestra)
        fs = ruta.getFileSystem(spark._jsc.hadoopConfiguration())

        if fs.exists(ruta):

            print("Reutilizando muestra fija de S3...")
            df_muestra = spark.read.parquet(ruta_muestra)

        else:

            print("Creando muestra de 500.000 registros...")
            (
                df.limit(500000) # Create a sample of 500,000 records from the DataFrame
                .write
                .mode("errorifexists")
                .parquet(ruta_muestra)
            )

            df_muestra = spark.read.parquet(ruta_muestra)

        total = df_muestra.count()

        print(f"Registros de entrada de la muestra: {total}")

        if total == 0:
            raise ValueError("La muestra está vacía.")

        if total < 500000:
            print(
                "Advertencia: el Parquet contiene menos de "
                "500.000 registros."
            )

        print("Iniciando carga del grafo en Neo4j...")

        cargador = CargadorNeo4j( # Create an instance of the CargadorNeo4j class to load data into Neo4j
            df_muestra,
            batch_size=int(os.environ.get("NEO4J_BATCH_SIZE", 5000)
            ),
            lote_size=int(os.environ.get("NEO4J_LOTE_SIZE", 5000)
            ),
            particiones=int(os.environ.get("NEO4J_PARTITIONS", 4)
            )
        )

        cargador.cargar()
        if os.environ.get("SKIP_BQ") != "1":
            bq = CargadorBigQuery(df_muestra)
            bq.cargar()
            if os.environ.get("EXPORTAR_GDS") == "1":
                bq.cargar_resultados_gds(
                    spark,
                    url=os.environ.get("NEO4J_URI", "bolt://neo4j:7687"),
                    usuario=os.environ.get("NEO4J_USER", "neo4j"),
                    password=os.environ["NEO4J_PASSWORD"],
                )

        print("Proceso finalizado correctamente.")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
