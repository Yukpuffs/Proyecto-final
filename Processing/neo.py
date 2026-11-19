
import os
import math

from pyspark.sql import functions as F, Window
from pyspark import StorageLevel

from Processing.checkpoint import CheckpointS3


FORMATO = "org.neo4j.spark.DataSource"


class CargadorNeo4j:

    def __init__(
        self,
        df,
        url=None,
        usuario=None,
        password=None,
        batch_size=5000,
        lote_size=5000,
        particiones=4
    ):
        self.df = df

        self.url = url or os.environ.get(
            "NEO4J_URI",
            "bolt://neo4j:7687"
        )

        self.usuario = usuario or os.environ.get(
            "NEO4J_USER",
            "neo4j"
        )

        self.password = password or os.environ["NEO4J_PASSWORD"]

        self.batch_size = batch_size
        self.lote_size = lote_size
        self.particiones = particiones

        ruta_checkpoint = os.environ.get(
            "S3_CHECKPOINT_PATH",
            "s3a://analisis-tweets/checkpoints/neo4j/"
        )

        version = os.environ.get(
            "NEO4J_LOAD_VERSION",
            "produccion-500k-v1"
        )

        self.checkpoints = CheckpointS3(
            self.df.sparkSession,
            f"{ruta_checkpoint.rstrip('/')}/{version}/"
        )

        self.base = None

    # ------------------------------------------------------
    # IDENTIFICADOR DE RELACIONES
    # ------------------------------------------------------

    def _agregar_id_relacion(self, df, *columnas):

        valores = [
            F.coalesce(
                F.col(columna).cast("string"),
                F.lit("")
            )
            for columna in columnas
        ]

        identificador = F.sha2(
            F.concat_ws("||", *valores),
            256
        )

        return df.withColumn("rel_id", identificador)

    # ------------------------------------------------------
    # PREPARACIÓN DE DATAFRAMES
    # ------------------------------------------------------

    def preparar(self):

        print("Preparando DataFrames para Neo4j...")

        base = (
            self.df
            .filter(
                F.col("external_author_id").isNotNull()
                & F.col("fecha").isNotNull()
            )
            .withColumn(
                "tweet_id",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col("external_author_id").cast("string"),
                        F.col("fecha").cast("string"),
                        F.coalesce(
                            F.col("updates").cast("string"),
                            F.lit("")
                        ),
                        F.coalesce(
                            F.col("content").cast("string"),
                            F.lit("")
                        )
                    ),
                    256
                )
            )
            .dropDuplicates(["tweet_id"])
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        self.base = base

        total = base.count()

        print(f"Tweets disponibles: {total}")

        # USUARIOS

        ventana = (
            Window
            .partitionBy("external_author_id")
            .orderBy(F.col("fecha").desc())
        )

        usuarios = (
            base
            .withColumn(
                "rn",
                F.row_number().over(ventana)
            )
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

        # TWEETS

        tweets = (
            base.select(
                F.col("tweet_id").alias("id"),
                F.col("content").alias("texto"),
                F.col("fecha"),
                F.col("region"),
                F.col("language"),
                F.col("post_type"),
                F.col("es_retuit"),
                F.col("es_respuesta")
            )
        )

        # HASHTAGS

        tweet_hashtag = (
            base
            .select(
                "tweet_id",
                F.explode("hashtags").alias("nombre")
            )
            .filter(
                F.col("nombre").isNotNull()
                & (F.col("nombre") != "")
            )
            .distinct()
        )

        hashtags = (
            tweet_hashtag
            .select("nombre")
            .distinct()
        )

        tweet_hashtag = self._agregar_id_relacion(
            tweet_hashtag,
            "tweet_id",
            "nombre"
        )

        # MENCIONES

        tweet_mencion = (
            base
            .select(
                "tweet_id",
                F.explode("menciones").alias("username")
            )
            .filter(
                F.col("username").isNotNull()
                & (F.col("username") != "")
            )
            .distinct()
        )

        menciones = (
            tweet_mencion
            .select("username")
            .distinct()
        )

        tweet_mencion = self._agregar_id_relacion(
            tweet_mencion,
            "tweet_id",
            "username"
        )

        # PUBLICÓ

        publico = (
            base.select(
                F.col("external_author_id").alias("usuario_id"),
                "tweet_id"
            )
        )

        publico = self._agregar_id_relacion(
            publico,
            "usuario_id",
            "tweet_id"
        )

        # RETWITEO_A

        if "usuario_retuiteado" in base.columns:

            retuiteo = (
                base
                .filter(
                    F.col("usuario_retuiteado").isNotNull()
                    & (F.col("usuario_retuiteado") != "")
                )
                .select(
                    F.col("external_author_id").alias("usuario_id"),
                    "tweet_id",
                    "usuario_retuiteado"
                )
                .distinct()
            )

            retuiteo = self._agregar_id_relacion(
                retuiteo,
                "usuario_id",
                "tweet_id",
                "usuario_retuiteado"
            )

        else:

            print(
                "Advertencia: no existe usuario_retuiteado. "
                "RETWITEO_A quedará vacía."
            )

            retuiteo = (
                base.limit(0)
                .select(
                    F.col("external_author_id").alias("usuario_id"),
                    "tweet_id"
                )
                .withColumn(
                    "usuario_retuiteado",
                    F.lit(None).cast("string")
                )
            )

            retuiteo = self._agregar_id_relacion(
                retuiteo,
                "usuario_id",
                "tweet_id",
                "usuario_retuiteado"
            )

        print("DataFrames preparados correctamente.")

        return {
            "usuarios": usuarios,
            "tweets": tweets,
            "hashtags": hashtags,
            "menciones": menciones,
            "publico": publico,
            "tweet_hashtag": tweet_hashtag,
            "tweet_mencion": tweet_mencion,
            "retuiteo": retuiteo
        }

    # ------------------------------------------------------
    # WRITER
    # ------------------------------------------------------

    def _writer(self, df):

        return (
            df.write
            .format(FORMATO)
            .mode("Overwrite")
            .option("url", self.url)
            .option(
                "authentication.basic.username",
                self.usuario
            )
            .option(
                "authentication.basic.password",
                self.password
            )
            .option("batch.size", str(self.batch_size))
            .option("transaction.retries", "10")
            .option("transaction.retry.timeout", "500")
        )

    # ------------------------------------------------------
    # NODOS
    # ------------------------------------------------------

    def _nodos(self, df, etiqueta, llave):

        (
            self._writer(
                df.repartition(
                    self.particiones,
                    F.col(llave)
                )
            )
            .option("labels", f":{etiqueta}")
            .option("node.keys", llave)
            .option(
                "schema.optimization.node.keys",
                "UNIQUE"
            )
            .save()
        )

        print(f"Nodos {etiqueta} cargados.")

    # ------------------------------------------------------
    # RELACIONES
    # ------------------------------------------------------

    def _relacion(
        self,
        df,
        tipo,
        src,
        src_col,
        src_key,
        dst,
        dst_col,
        dst_key,
        propiedades="rel_id"
    ):

        (
            self._writer(df.coalesce(1))
            .option("relationship", tipo)
            .option("relationship.save.strategy", "keys")
            .option("relationship.keys", "rel_id")
            .option(
                "schema.optimization.relationship.keys",
                "UNIQUE"
            )
            .option("relationship.properties", propiedades)

            .option(
                "relationship.source.labels",
                f":{src}"
            )
            .option(
                "relationship.source.save.mode",
                "Match"
            )
            .option(
                "relationship.source.node.keys",
                f"{src_col}:{src_key}"
            )

            .option(
                "relationship.target.labels",
                f":{dst}"
            )
            .option(
                "relationship.target.save.mode",
                "Match"
            )
            .option(
                "relationship.target.node.keys",
                f"{dst_col}:{dst_key}"
            )

            .save()
        )

        print(f"Relación {tipo} cargada.")

    # ------------------------------------------------------
    # CARGA POR LOTES
    # ------------------------------------------------------

    def _cargar_lotes(
        self,
        etapa,
        df,
        llave,
        funcion_carga
    ):

        print(f"\nIniciando etapa: {etapa}")

        df = df.filter(F.col(llave).isNotNull())

        total = df.count()

        if total == 0:
            print(f"{etapa}: no hay registros.")
            return

        numero_lotes = max(
            1,
            math.ceil(total / self.lote_size)
        )

        print(
            f"{etapa}: {total} registros, "
            f"{numero_lotes} lotes aproximados."
        )

        df_lotes = (
            df
            .withColumn(
                "_lote",
                F.pmod(
                    F.xxhash64(F.col(llave)),
                    F.lit(numero_lotes)
                )
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        try:

            for numero in range(numero_lotes):

                if self.checkpoints.esta_completado(
                    etapa,
                    numero
                ):

                    print(
                        f"{etapa}: lote {numero} "
                        "ya completado. Se omite."
                    )

                    continue

                lote = (
                    df_lotes
                    .filter(F.col("_lote") == numero)
                    .drop("_lote")
                    .persist(StorageLevel.MEMORY_AND_DISK)
                )

                try:

                    cantidad = lote.count()

                    if cantidad == 0:

                        self.checkpoints.marcar_completado(
                            etapa,
                            numero
                        )

                        continue

                    print(
                        f"{etapa}: procesando lote "
                        f"{numero + 1}/{numero_lotes} "
                        f"({cantidad} registros)."
                    )

                    funcion_carga(lote)

                    self.checkpoints.marcar_completado(
                        etapa,
                        numero
                    )

                finally:

                    lote.unpersist()

        finally:

            df_lotes.unpersist()

        print(f"Etapa {etapa} finalizada.")

    # ------------------------------------------------------
    # ORQUESTACIÓN
    # ------------------------------------------------------

    def cargar(self):

        try:

            d = self.preparar()

            trabajos_nodos = [

                (
                    "usuarios",
                    d["usuarios"],
                    "id",
                    lambda lote: self._nodos(
                        lote, "Usuario", "id"
                    )
                ),

                (
                    "tweets",
                    d["tweets"],
                    "id",
                    lambda lote: self._nodos(
                        lote, "Tweet", "id"
                    )
                ),

                (
                    "hashtags",
                    d["hashtags"],
                    "nombre",
                    lambda lote: self._nodos(
                        lote, "Hashtag", "nombre"
                    )
                ),

                (
                    "menciones",
                    d["menciones"],
                    "username",
                    lambda lote: self._nodos(
                        lote, "Mencion", "username"
                    )
                )
            ]

            print("\n========== CARGA DE NODOS ==========")

            for etapa, dataframe, llave, carga in trabajos_nodos:

                self._cargar_lotes(
                    etapa,
                    dataframe,
                    llave,
                    carga
                )

            trabajos_relaciones = [

                (
                    "publico",
                    d["publico"],
                    "rel_id",
                    lambda lote: self._relacion(
                        lote,
                        "PUBLICÓ",
                        "Usuario", "usuario_id", "id",
                        "Tweet", "tweet_id", "id",
                        "rel_id"
                    )
                ),

                (
                    "tweet_hashtag",
                    d["tweet_hashtag"],
                    "rel_id",
                    lambda lote: self._relacion(
                        lote,
                        "TIENE_HASHTAG",
                        "Tweet", "tweet_id", "id",
                        "Hashtag", "nombre", "nombre",
                        "rel_id"
                    )
                ),

                (
                    "tweet_mencion",
                    d["tweet_mencion"],
                    "rel_id",
                    lambda lote: self._relacion(
                        lote,
                        "MENCIONA_A",
                        "Tweet", "tweet_id", "id",
                        "Mencion", "username", "username",
                        "rel_id"
                    )
                ),

                (
                    "retuiteo",
                    d["retuiteo"],
                    "rel_id",
                    lambda lote: self._relacion(
                        lote,
                        "RETWITEO_A",
                        "Usuario", "usuario_id", "id",
                        "Tweet", "tweet_id", "id",
                        "rel_id,usuario_retuiteado"
                    )
                )
            ]

            print("\n========== CARGA DE RELACIONES ==========")

            for etapa, dataframe, llave, carga in trabajos_relaciones:

                self._cargar_lotes(
                    etapa,
                    dataframe,
                    llave,
                    carga
                )

            print("\n====================================")
            print("CARGA FINALIZADA CORRECTAMENTE")
            print("====================================")

        finally:

            if self.base is not None:
                self.base.unpersist()
                print("Caché de Spark liberada.")
