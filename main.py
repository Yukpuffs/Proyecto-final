from pyspark.sql import SparkSession

from Ingestion.readS3 import leer_datos_s3
from Processing.transformData import transformar_datos
from Processing.saveparquet import GuardarParquet


spark = (
    SparkSession.builder
    .appName("AnalisisDesinformacion")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


try:

    print("Iniciando ingesta...")

    lector = leer_datos_s3(spark)
    df = lector.leer_datos()

    print("Iniciando transformacion...")

    transformador = transformar_datos(df)
    df_procesado = transformador.transformar()


    print("Estructura de los datos procesados:")

    df_procesado.printSchema()
    df_procesado.show(10, truncate=False)

    print("Guardando datos en Parquet...")

    guardar = GuardarParquet(df_procesado)
    guardar.guardar_en_parquet()

    print("Proceso finalizado correctamente")


finally:

    spark.stop()
