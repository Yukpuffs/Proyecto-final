
import os

from pyspark.sql import SparkSession

from Ingestion.readS3 import leer_datos_s3
from Processing.transformData import transformar_datos
from Processing.saveparquet import GuardarParquet
from Processing.neo import CargadorNeo4j


RUTA_PARQUET = (
    "s3a://analisis-tweets/processed/comentarios_parquet/"
)


def main():

    spark = (
        SparkSession.builder
        .appName("AnalisisDesinformacion")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:

        if os.environ.get("SKIP_ETL") == "1":

            print("Reutilizando Parquet procesado desde S3...")

            df = spark.read.parquet(RUTA_PARQUET)

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

            guardar = GuardarParquet(df_procesado)
            guardar.guardar_en_parquet()

            print("Leyendo Parquet desde S3...")

            df = spark.read.parquet(RUTA_PARQUET)

        # ------------------------------------------
        # MUESTRA FIJA DE 500.000 REGISTROS
        # ------------------------------------------

        ruta_muestra = os.environ.get(
            "SAMPLE_PARQUET_PATH",
            "s3a://analisis-tweets/processed/prueba_500k_checkpoint/"
        )

        Path = spark._jvm.org.apache.hadoop.fs.Path

        ruta = Path(ruta_muestra)
        fs = ruta.getFileSystem(
            spark._jsc.hadoopConfiguration()
        )

        if fs.exists(ruta):

            print("Reutilizando muestra fija de S3...")

            df_muestra = spark.read.parquet(ruta_muestra)

        else:

            print("Creando muestra de hasta 500.000 registros...")

            (
                df.limit(500000)
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

        # ------------------------------------------
        # CARGA EN NEO4J
        # ------------------------------------------

        print("Iniciando carga del grafo en Neo4j...")

        cargador = CargadorNeo4j(
            df_muestra,
            batch_size=int(
                os.environ.get("NEO4J_BATCH_SIZE", 5000)
            ),
            lote_size=int(
                os.environ.get("NEO4J_LOTE_SIZE", 5000)
            ),
            particiones=int(
                os.environ.get("NEO4J_PARTITIONS", 4)
            )
        )

        cargador.cargar()

        print("Proceso finalizado correctamente.")

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
