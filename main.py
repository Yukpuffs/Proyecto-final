import os
from ingestion.readS3 import spark, leer_datos_s3
from Processing.transformData import transformar_datos
from Processing.saveparquet import GuardarParquet
from Processing.neo4j_graph import CargadorNeo4j

RUTA_PARQUET = "s3a://analisis-tweets/processed/comentarios_parquet/"

if __name__ == "__main__":
    if os.environ.get("SKIP_ETL") == "1":
        # Reutiliza el Parquet ya procesado en S3
        df = spark.read.parquet(RUTA_PARQUET)
    else:
        crudo = leer_datos_s3(spark).leer_datos()
        df = transformar_datos(crudo).transformar()
        GuardarParquet(df).guardar_en_parquet()
        df = spark.read.parquet(RUTA_PARQUET)   # el grafo se construye desde S3/Parquet

    CargadorNeo4j(df).cargar()
    spark.stop()
