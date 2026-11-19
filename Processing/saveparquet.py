
class GuardarParquet: # Create a class to handle saving DataFrame to Parquet

    def __init__(self, dataframe, ruta_salida):
        self.df = dataframe
        self.ruta_salida = ruta_salida # Define the output path for the Parquet file

    def guardar_en_parquet(self):

        ruta_salida = ( # Define the s3 output path for the Parquet file
            "s3a://analisis-tweets/processed/comentarios_parquet/"
        )

        (
            self.df.write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(ruta_salida)
        )

        print("====================================")
        print("Datos guardados correctamente en S3")
        print(f"Ubicacion: {ruta_salida}")
        print("Formato: Parquet")
