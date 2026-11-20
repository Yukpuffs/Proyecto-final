import os
from pyspark.sql import functions as F, Window

FORMATO_BQ = "bigquery"
FORMATO_NEO4J = "org.neo4j.spark.DataSource"


class CargadorBigQuery:
    """Carga a BigQuery los datos ya procesados (el mismo DataFrame que usa Neo4j)."""

    def __init__(self, df, proyecto=None, dataset=None, credenciales=None):
        self.df = df
        self.proyecto = proyecto or os.environ["BQ_PROJECT_ID"]
        self.dataset = dataset or os.environ.get("BQ_DATASET", "analisis_tweets")
        self.credenciales = credenciales or os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

    # Preparación: una tabla analítica por cada "vista" de los datos
    def preparar(self):
        # Misma limpieza y misma llave que usa Neo4j
        base = (
            self.df
            .filter(F.col("external_author_id").isNotNull() & F.col("fecha").isNotNull())
            .withColumn(
                "tweet_id",
                F.concat_ws(
                    "_",
                    F.col("external_author_id"),
                    F.date_format("fecha", "yyyyMMddHHmm"),
                    F.coalesce(F.col("updates"), F.lit("0")),
                ),
            )
            .dropDuplicates(["tweet_id"])
            .cache()
        )

        # 1) Tabla de hechos: un registro por tweet (hashtags y menciones como arreglos)
        tweets = base.select(
            "tweet_id",
            F.col("external_author_id").alias("usuario_id"),
            F.col("author").alias("username"),
            F.col("content").alias("texto"),
            "fecha", "region", "language", "post_type",
            "es_retuit", "es_respuesta", "usuario_retuiteado",
            "hashtags", "menciones", "n_hashtags", "n_menciones",
        )

        # 2) Un registro por usuario (el más reciente) + su actividad total
        w = Window.partitionBy("external_author_id").orderBy(F.col("fecha").desc())
        usuarios = (
            base.withColumn("rn", F.row_number().over(w))
            .withColumn("total_tweets", F.count("*").over(Window.partitionBy("external_author_id")))
            .filter("rn = 1")
            .select(
                F.col("external_author_id").alias("usuario_id"),
                F.col("author").alias("username"),
                "followers", "following", "account_type", "account_category",
                "total_tweets",
            )
        )

        # 3) Frecuencia de hashtags
        hashtags = (
            base.select("tweet_id", "external_author_id", "fecha",
                        F.explode("hashtags").alias("hashtag"))
            .filter(F.col("hashtag") != "")
            .groupBy("hashtag")
            .agg(
                F.count("*").alias("veces_usado"),
                F.countDistinct("external_author_id").alias("usuarios_distintos"),
                F.min("fecha").alias("primer_uso"),
                F.max("fecha").alias("ultimo_uso"),
            )
        )

        # 4) Actividad por día
        actividad = (
            base.groupBy(F.to_date("fecha").alias("dia"))
            .agg(
                F.count("*").alias("tweets"),
                F.sum(F.col("es_retuit").cast("int")).alias("retuits"),
                F.sum(F.col("es_respuesta").cast("int")).alias("respuestas"),
                F.countDistinct("external_author_id").alias("usuarios_activos"),
            )
        )

        return {
            "tweets": tweets,
            "usuarios": usuarios,
            "hashtags_frecuencia": hashtags,
            "actividad_diaria": actividad,
        }

    def _escribir(self, df, tabla):
        destino = f"{self.proyecto}.{self.dataset}.{tabla}"
        (
            df.write.format(FORMATO_BQ)
            .mode("overwrite")                      # reemplaza la tabla en cada corrida
            .option("writeMethod", "direct")        # Storage Write API: no necesita bucket de GCS
            .option("credentialsFile", self.credenciales)
            .option("parentProject", self.proyecto)
            .save(destino)
        )
        print(f"  tabla {tabla} cargada en BigQuery")

    def cargar(self):
        print("Cargando tablas en BigQuery...")
        for nombre, df in self.preparar().items():
            self._escribir(df, nombre)
        print("BigQuery listo")

    def cargar_resultados_gds(self, spark, url, usuario, password):
        def leer(etiqueta):
            return (
                spark.read.format(FORMATO_NEO4J)
                .option("url", url)
                .option("authentication.basic.username", usuario)
                .option("authentication.basic.password", password)
                .option("labels", f":{etiqueta}")
                .load()
            )

        cols = ["pagerank", "comunidad", "betweenness"]
        usuarios = leer("Usuario")
        if not all(c in usuarios.columns for c in cols):
            print("Aún no hay métricas GDS en Neo4j; se omite este paso.")
            return

        influyentes = (
            usuarios.filter(F.col("pagerank").isNotNull())
            .select(F.col("id").alias("usuario_id"), "username", *cols)
        )
        self._escribir(influyentes, "usuarios_influyentes")