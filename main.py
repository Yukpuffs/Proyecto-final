import os
from ingestion.readS3 import spark, leer_datos_s3
from Processing.transformData import transformar_datos
from Processing.saveparquet import GuardarParquet
from Processing.neo4j_graph import CargadorNeo4j
from Processing.bigquery import CargadorBigQuery

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

    if os.environ.get("SKIP_NEO4J") != "1":
        CargadorNeo4j(df.limit(50000)).cargar()

    if os.environ.get("SKIP_BQ") != "1":
        bq = CargadorBigQuery(df.limit(10000))
        bq.cargar()
        if os.environ.get("EXPORTAR_GDS") == "1":
            bq.cargar_resultados_gds(
                spark,
                url=os.environ.get("NEO4J_URI", "bolt://neo4j:7687"),
                usuario=os.environ.get("NEO4J_USER", "neo4j"),
                password=os.environ["NEO4J_PASSWORD"],
            )

    spark.stop()
