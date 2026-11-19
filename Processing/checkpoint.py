
from pyspark.sql import SparkSession

class CheckpointS3:

    def __init__(self, spark, ruta_base):
        self.jvm = spark._jvm
        self.config = spark._jsc.hadoopConfiguration()

        self.Path = self.jvm.org.apache.hadoop.fs.Path
        self.ruta_base = self.Path(ruta_base)
        self.fs = self.ruta_base.getFileSystem(self.config)

    def _ruta_lote(self, etapa, lote):
        return self.Path(
            self.ruta_base,
            f"{etapa}/lote_{lote:05d}.done"
        )

    def esta_completado(self, etapa, lote):
        return self.fs.exists(
            self._ruta_lote(etapa, lote)
        )

    def marcar_completado(self, etapa, lote):
        ruta = self._ruta_lote(etapa, lote)

        self.fs.mkdirs(ruta.getParent())

        if not self.fs.exists(ruta):
            archivo = self.fs.create(ruta, False)
            archivo.close()

        print(
            f"Checkpoint guardado: {etapa} - lote {lote}"
        )
