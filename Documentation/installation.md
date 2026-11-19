# Instalación y ejecución

## 1. Requisitos de instalación

- Docker Desktop (con al menos 4 GB de RAM asignados).
- Cuenta de AWS y crear el bucket `analisis-tweets`.
- Credenciales propias de AWS para cada integrante.

## 2. Configuración e instalación

1. Clonar el repositorio.
2. Crear el archivo `.env`.
3. Completar las siguientes variables de entorno:

```env
S3_BUCKET=
S3_CHECKPOINT_PATH=
NEO4J_LOAD_VERSION=
SAMPLE_PARQUET_PATH=

AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=

SPARK_TAG=
SPARK_PACKAGES=

NEO4J_URI=
NEO4J_USER=
NEO4J_PASSWORD=
```

4. Verificar que el bucket de Amazon S3 contenga los archivos CSV originales dentro de la carpeta `raw/`. Además, crear una carpeta `processed/` para guardar archivos en formato parquet.
Adicionalmente, crear carpeta `checkpoints/` para guardar el proceso de creado de nodos.

**Importante:** El archivo `.env` contiene credenciales personales y no debe compartirse ni subirse al repositorio.

## 3. Ejecución del proyecto

1. Abrir Docker Desktop

2. Ejecutar el procesamiento para ingesta, transformación y creación de nodos:

```bash
docker compose run --rm spark
```

3. Acceder a Neo4j desde el navegador:

[http://localhost:7474](http://localhost:7474)

Iniciar sesión utilizando el usuario `neo4j` y la contraseña configurada en el archivo `.env`.

4. Ejecutar las consultas Cypher disponibles en la carpeta `Queries/`. <br>

    ```bash
    Get-Content Queries\ccommon_hashtags.cypher | docker exec -i neo4j cypher-shell -u neo4j -p (contraseña neo4j)
    ```
 - <b> Hastags mas usados: </b>
 ```cypher
    MATCH (:Tweet)-[:TIENE_HASHTAG]->(h:Hashtag)
    RETURN h.nombre AS hashtag, count(*) AS usos
    ORDER BY usos DESC LIMIT 10;
```



