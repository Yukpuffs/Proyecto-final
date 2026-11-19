import os
import math

from pyspark.sql import functions as F, Window
from pyspark import StorageLevel

from Processing.checkpoint import CheckpointS3

FORMATO = "org.neo4j.spark.DataSource"


class CargadorNeo4j:

    def __init__(self, df, url=None, usuario=None, password=None,
                 batch_size=5000, lote_size=5000, particiones=4):
        self.df = df
        self.url = url or os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
        self.usuario = usuario or os.environ.get("NEO4J_USER", "neo4j")
        self.password = password or os.environ["NEO4J_PASSWORD"]
        self.batch_size = batch_size
        self.lote_size = lote_size
        self.particiones = particiones

        ruta_checkpoint = os.environ.get("S3_CHECKPOINT_PATH", "s3a://analisis-tweets/checkpoints/neo4j/")
        version = os.environ.get("NEO4J_LOAD_VERSION", "produccion-500k-v1")
        self.checkpoints = CheckpointS3(self.df.sparkSession, f"{ruta_checkpoint.rstrip('/')}/{version}/")

        self.base = None


    def _agregar_id_relacion(self, df, *columnas): # Generate a unique relationship ID based on specified columns
        valores = [F.coalesce(F.col(columna).cast("string"), F.lit("")) for columna in columnas]
        return df.withColumn("rel_id", F.sha2(F.concat_ws("||", *valores), 256))

    def preparar(self): # Prepare DataFrames for Neo4j nodes and relationships
        print("Preparando DataFrames para Neo4j...")

        base = ( # Create a base DataFrame with necessary transformations
            self.df
            .filter(F.col("external_author_id").isNotNull() & F.col("fecha").isNotNull())
            .withColumn("tweet_id", F.sha2(F.concat_ws(
                "||",
                F.col("external_author_id").cast("string"),
                F.col("fecha").cast("string"),
                F.coalesce(F.col("updates").cast("string"), F.lit("")),
                F.coalesce(F.col("content").cast("string"), F.lit(""))
            ), 256))
            .dropDuplicates(["tweet_id"])
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        self.base = base

        total = base.count()
        print(f"Tweets disponibles: {total}")


        ventana = Window.partitionBy("external_author_id").orderBy(F.col("fecha").desc()) # Create a window specification to get the latest record for each user based on the 'fecha' column

        usuarios = ( # Create a DataFrame for users with the latest record for each user
            base
            .withColumn("rn", F.row_number().over(ventana))
            .filter(F.col("rn") == 1)
            .select(
                F.col("external_author_id").alias("id"),
                F.col("author").alias("username"),
                "followers",
                "following",
                "account_type",
                "account_category"
            )
            .filter(F.col("id").isNotNull())
        )

        tweets = base.select( # Create a DataFrame for tweets with necessary columns
            F.col("tweet_id").alias("id"),
            F.col("content").alias("texto"),
            "fecha", "region", "language", "post_type", "es_retuit", "es_respuesta"
        )

        tweet_hashtag = ( # Create a DataFrame for tweet-hashtag relationships by exploding the 'hashtags' array and filtering out null or empty values
            base
            .select("tweet_id", F.explode("hashtags").alias("nombre"))
            .filter(F.col("nombre").isNotNull() & (F.col("nombre") != ""))
            .distinct()
        )
        hashtags = tweet_hashtag.select("nombre").distinct()
        tweet_hashtag = self._agregar_id_relacion(tweet_hashtag, "tweet_id", "nombre")

        tweet_mencion = ( # Create a DataFrame for tweet-mention relationships by exploding the 'menciones' array and filtering out null or empty values
            base
            .select("tweet_id", F.explode("menciones").alias("username"))
            .filter(F.col("username").isNotNull() & (F.col("username") != ""))
            .distinct()
        )
        menciones = tweet_mencion.select("username").distinct() # Create a DataFrame for mentions with distinct usernames
        tweet_mencion = self._agregar_id_relacion(tweet_mencion, "tweet_id", "username")

        publico = base.select(F.col("external_author_id").alias("usuario_id"), "tweet_id") # Create a DataFrame for user-tweet relationships (who posted which tweet)
        publico = self._agregar_id_relacion(publico, "usuario_id", "tweet_id")

        if "usuario_retuiteado" in base.columns: # Create a DataFrame for retweet relationships if the 'usuario_retuiteado' column exists in the base DataFrame
            retuiteo = (
                base
                .filter(F.col("usuario_retuiteado").isNotNull() & (F.col("usuario_retuiteado") != ""))
                .select(F.col("external_author_id").alias("usuario_id"), "tweet_id", "usuario_retuiteado")
                .distinct()
            )
        else: # If the 'usuario_retuiteado' column does not exist, create an empty DataFrame with the necessary schema for retweet relationships
            print("Advertencia: no existe usuario_retuiteado. RETWITEO_A quedará vacía.") 
            retuiteo = (
                base.limit(0)
                .select(F.col("external_author_id").alias("usuario_id"), "tweet_id")
                .withColumn("usuario_retuiteado", F.lit(None).cast("string"))
            )

        retuiteo = self._agregar_id_relacion(retuiteo, "usuario_id", "tweet_id", "usuario_retuiteado")

        print("DataFrames preparados correctamente.")

        return { # Return a dictionary containing all the prepared DataFrames for nodes and relationships
            "usuarios": usuarios,
            "tweets": tweets,
            "hashtags": hashtags,
            "menciones": menciones,
            "publico": publico,
            "tweet_hashtag": tweet_hashtag,
            "tweet_mencion": tweet_mencion,
            "retuiteo": retuiteo
        }

    def _writer(self, df): # Create a DataFrameWriter for writing data to Neo4j with the specified format and options
        return (
            df.write
            .format(FORMATO)
            .mode("Overwrite")
            .option("url", self.url)
            .option("authentication.basic.username", self.usuario)
            .option("authentication.basic.password", self.password)
            .option("batch.size", str(self.batch_size))
            .option("transaction.retries", "10")
            .option("transaction.retry.timeout", "500")
        )

    def _nodos(self, df, etiqueta, llave): # Write nodes to Neo4j with the specified label and key column
        (
            self._writer(df.repartition(self.particiones, F.col(llave)))
            .option("labels", f":{etiqueta}")
            .option("node.keys", llave)
            .option("schema.optimization.node.keys", "UNIQUE")
            .save()
        )
        print(f"Nodos {etiqueta} cargados.")

    def _relacion(self, df, tipo, src, src_col, src_key, dst, dst_col, dst_key, propiedades="rel_id"): # Write relationships to Neo4j with the specified type, source and target labels, keys, and properties
        (
            self._writer(df.coalesce(1))
            .option("relationship", tipo)
            .option("relationship.save.strategy", "keys")
            .option("relationship.keys", "rel_id")
            .option("schema.optimization.relationship.keys", "UNIQUE")
            .option("relationship.properties", propiedades)
            .option("relationship.source.labels", f":{src}")
            .option("relationship.source.save.mode", "Match")
            .option("relationship.source.node.keys", f"{src_col}:{src_key}")
            .option("relationship.target.labels", f":{dst}")
            .option("relationship.target.save.mode", "Match")
            .option("relationship.target.node.keys", f"{dst_col}:{dst_key}")
            .save()
        )
        print(f"Relación {tipo} cargada.")

    def _cargar_lotes(self, etapa, df, llave, funcion_carga): # Load data in batches for a specific stage, filtering out null values and processing each batch with the provided loading function.
        print(f"\nIniciando etapa: {etapa}")

        df = df.filter(F.col(llave).isNotNull())
        total = df.count()

        if total == 0:
            print(f"{etapa}: no hay registros.")
            return

        numero_lotes = max(1, math.ceil(total / self.lote_size)) # Calculate the number of batches based on the total number of records and the specified batch size
        print(f"{etapa}: {total} registros, {numero_lotes} lotes aproximados.")

        df_lotes = (
            df
            .withColumn("_lote", F.pmod(F.xxhash64(F.col(llave)), F.lit(numero_lotes)))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        try:
            for numero in range(numero_lotes): # Iterate through each batch number and process the corresponding batch of data
                if self.checkpoints.esta_completado(etapa, numero):
                    print(f"{etapa}: lote {numero} ya completado. Se omite.")
                    continue

                lote = ( # Filter the DataFrame to get the current batch based on the batch number and persist it in memory and disk
                    df_lotes
                    .filter(F.col("_lote") == numero)
                    .drop("_lote")
                    .persist(StorageLevel.MEMORY_AND_DISK)
                )

                try:
                    cantidad = lote.count()

                    if cantidad == 0:
                        self.checkpoints.marcar_completado(etapa, numero)
                        continue

                    print(f"{etapa}: procesando lote {numero + 1}/{numero_lotes} ({cantidad} registros).")
                    funcion_carga(lote)
                    self.checkpoints.marcar_completado(etapa, numero)

                finally:
                    lote.unpersist()

        finally:
            df_lotes.unpersist()

        print(f"Etapa {etapa} finalizada.")

    def cargar(self): # Load all nodes and relationships into Neo4j by preparing the DataFrames and processing each stage in batches, while managing Spark caching and checkpoints.
        try:
            d = self.preparar()

            trabajos_nodos = [ # Define a list of node loading tasks, each containing the stage name, DataFrame, key column, and loading function for nodes
                ("usuarios", d["usuarios"], "id", lambda lote: self._nodos(lote, "Usuario", "id")),
                ("tweets", d["tweets"], "id", lambda lote: self._nodos(lote, "Tweet", "id")),
                ("hashtags", d["hashtags"], "nombre", lambda lote: self._nodos(lote, "Hashtag", "nombre")),
                ("menciones", d["menciones"], "username", lambda lote: self._nodos(lote, "Mencion", "username"))
            ]

            print("\n========== CARGA DE NODOS ==========")

            for etapa, dataframe, llave, carga in trabajos_nodos:
                self._cargar_lotes(etapa, dataframe, llave, carga)

            trabajos_relaciones = [ # Define a list of relationship loading tasks, each containing the stage name, DataFrame, key column, and loading function for relationships
                (
                    "publico", d["publico"], "rel_id",
                    lambda lote: self._relacion(
                        lote, "PUBLICÓ",
                        "Usuario", "usuario_id", "id",
                        "Tweet", "tweet_id", "id",
                        "rel_id"
                    )
                ),
                (
                    "tweet_hashtag", d["tweet_hashtag"], "rel_id",
                    lambda lote: self._relacion(
                        lote, "TIENE_HASHTAG",
                        "Tweet", "tweet_id", "id",
                        "Hashtag", "nombre", "nombre",
                        "rel_id"
                    )
                ),
                (
                    "tweet_mencion", d["tweet_mencion"], "rel_id",
                    lambda lote: self._relacion(
                        lote, "MENCIONA_A",
                        "Tweet", "tweet_id", "id",
                        "Mencion", "username", "username",
                        "rel_id"
                    )
                ),
                (
                    "retuiteo", d["retuiteo"], "rel_id",
                    lambda lote: self._relacion(
                        lote, "RETWITEO_A",
                        "Usuario", "usuario_id", "id",
                        "Tweet", "tweet_id", "id",
                        "rel_id,usuario_retuiteado"
                    )
                )
            ]

            print("\n========== CARGA DE RELACIONES ==========")

            for etapa, dataframe, llave, carga in trabajos_relaciones: # Process each relationship loading task in batches, filtering out null values
                self._cargar_lotes(etapa, dataframe, llave, carga)

            print("CARGA FINALIZADA CORRECTAMENTE")

        finally:
            if self.base is not None:
                self.base.unpersist()
                print("Caché de Spark liberada.")