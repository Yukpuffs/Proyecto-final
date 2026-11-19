
import os
from pyspark.sql import functions as F, Window

FORMATO = "org.neo4j.spark.DataSource"


class CargadorNeo4j:

    def __init__(self, df, url=None, usuario=None, password=None, batch_size=5000):
        self.df = df
        self.url = url or os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
        self.usuario = usuario or os.environ.get("NEO4J_USER", "neo4j")
        self.password = password or os.environ["NEO4J_PASSWORD"]
        self.batch_size = batch_size

    # Preparación de DataFrames (uno por tipo de nodo / relación)
    def preparar(self):  
        base = (
            self.df
            .filter(F.col("external_author_id").isNotNull() & F.col("fecha").isNotNull())
            # Llave del Tweet: external_author_id + fecha + updates
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

        # --- Usuario: un registro por autor (el más reciente, para followers/following)
        w = Window.partitionBy("external_author_id").orderBy(F.col("fecha").desc())
        usuarios = (
            base.withColumn("rn", F.row_number().over(w))
            .filter("rn = 1")
            .select(
                F.col("external_author_id").alias("id"),
                F.col("author").alias("username"),
                F.col("followers"),
                F.col("following"),
                F.col("account_type"),
                F.col("account_category"),
            )
        )

        # --- Tweet
        tweets = base.select(
            F.col("tweet_id").alias("id"),
            F.col("content").alias("texto"),
            F.col("fecha"),
            F.col("region"),
            F.col("language"),
            F.col("post_type"),
            F.col("es_retuit"),
            F.col("es_respuesta"),
        )

        # --- Hashtag
        tweet_hashtag = (
            base.select("tweet_id", F.explode("hashtags").alias("nombre"))
            .filter(F.col("nombre") != "")
            .distinct()
        )
        hashtags = tweet_hashtag.select("nombre").distinct()

        # --- Mencion
        tweet_mencion = (
            base.select("tweet_id", F.explode("menciones").alias("username"))
            .filter(F.col("username") != "")
            .distinct()
        )
        menciones = tweet_mencion.select("username").distinct()

        # --- Relación PUBLICÓ
        publico = base.select(
            F.col("external_author_id").alias("usuario_id"), "tweet_id"
        )

        # --- Relación RETWITEO_A (solo si usuario_retuiteado no está vacío)
        retuiteo = (
            base.filter(F.col("usuario_retuiteado").isNotNull() & (F.col("usuario_retuiteado") != ""))
            .select(F.col("external_author_id").alias("usuario_id"), "tweet_id", "usuario_retuiteado")
        )

        return {
            "usuarios": usuarios, "tweets": tweets, "hashtags": hashtags,
            "menciones": menciones, "publico": publico,
            "tweet_hashtag": tweet_hashtag, "tweet_mencion": tweet_mencion,
            "retuiteo": retuiteo,
        }
    # Escritura de nodos y relaciones en Neo4j
    def _writer(self, df):
        return (
            df.coalesce(1).write.format(FORMATO)
            .mode("Overwrite")  # Overwrite = MERGE (idempotente)
            .option("url", self.url)
            .option("authentication.basic.username", self.usuario)
            .option("authentication.basic.password", self.password)
            .option("batch.size", self.batch_size)
        )

    def _nodos(self, df, etiqueta, llave):
        (
            self._writer(df)
            .option("labels", f":{etiqueta}")
            .option("node.keys", llave)
            .option("schema.optimization.type", "NODE_CONSTRAINTS")  # crea UNIQUE sobre la llave
            .save()
        )
        print(f"  nodos {etiqueta} cargados")

    def _relacion(self, df, tipo, src, src_col, src_key, dst, dst_col, dst_key, props=None):
        w = (
            self._writer(df)
            .option("relationship", tipo)
            .option("relationship.save.strategy", "keys")
            .option("relationship.source.labels", f":{src}")
            .option("relationship.source.save.mode", "Match")
            .option("relationship.source.node.keys", f"{src_col}:{src_key}")
            .option("relationship.target.labels", f":{dst}")
            .option("relationship.target.save.mode", "Match")
            .option("relationship.target.node.keys", f"{dst_col}:{dst_key}")
        )
        if props:
            w = w.option("relationship.properties", props)
        w.save()
        print(f"  relación {tipo} cargada")

    def cargar(self):
        d = self.preparar()

        print("Cargando nodos en Neo4j...")
        self._nodos(d["usuarios"], "Usuario", "id")
        self._nodos(d["tweets"], "Tweet", "id")
        self._nodos(d["hashtags"], "Hashtag", "nombre")
        self._nodos(d["menciones"], "Mencion", "username")

        print("Cargando relaciones en Neo4j...")
        self._relacion(d["publico"], "PUBLICÓ",
                       "Usuario", "usuario_id", "id", "Tweet", "tweet_id", "id")
        self._relacion(d["tweet_hashtag"], "TIENE_HASHTAG",
                       "Tweet", "tweet_id", "id", "Hashtag", "nombre", "nombre")
        self._relacion(d["tweet_mencion"], "MENCIONA_A",
                       "Tweet", "tweet_id", "id", "Mencion", "username", "username")
        self._relacion(d["retuiteo"], "RETWITEO_A",
                       "Usuario", "usuario_id", "id", "Tweet", "tweet_id", "id",
                       props="usuario_retuiteado:usuario_retuiteado")

        print("Grafo cargado en Neo4j")
