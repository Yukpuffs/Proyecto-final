from pyspark.sql import functions as F # Import necessary functions from PySpark SQL module

class transformar_datos: # Define a class for data transformation

    def __init__(self, df):
        self.df = df

    def transformar(self):

        df = ( # Filter out rows with null content, trim whitespace, and cast columns to appropriate data types
            self.df
            .filter(F.col("content").isNotNull())
            .withColumn("content", F.trim(F.col("content")))
            .withColumn("retweet", F.col("retweet").cast("int"))
            .withColumn("followers", F.col("followers").cast("long"))
            .withColumn("following", F.col("following").cast("long"))
            .withColumn("fecha", F.to_timestamp(
                "publish_date", "M/d/yyyy H:mm"
            ))
        )

        texto = F.col("content")

        df = ( # Extract hashtags and mentions, determine if it's a retweet or reply, and count them
            df
            .withColumn("hashtags",F.transform(F.regexp_extract_all(texto,F.lit(r"(?U)#(\w+)"),1),lambda h: F.lower(h)))
            .withColumn("menciones",F.transform(    F.regexp_extract_all(texto,F.lit(r"@(\w{1,15})"), 1 ),lambda m: F.lower(m)))
            .withColumn("es_retuit_texto",     texto.rlike(r"^RT @") ) .withColumn("usuario_retuiteado",F.lower(F.regexp_extract(texto, r"^RT @(\w+)", 1)))
            .withColumn("es_respuesta", texto.rlike(r"^@") & ~texto.rlike(r"^RT @")).withColumn("es_retuit",(F.col("retweet") == 1) | F.col("es_retuit_texto"))
            .withColumn("n_hashtags", F.size("hashtags"))
            .withColumn("n_menciones", F.size("menciones"))
        )

        print("Transformacion completada")

        return df
