
<div align="center">

# 🕸️ ANALISIS INFLUENCIA Y DESINFORMACIÓN EN REDES SOCIALES

### BIG DATA · ANALISIS GRAFOS

<p>
  Análisis de interacciones y patrones de difusión
  en redes sociales mediante procesamiento distribuido.
</p>

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![AWS](https://img.shields.io/badge/Amazon_S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white)
![Neo4j](https://img.shields.io/badge/Neo4j-008CC1?style=for-the-badge&logo=neo4j&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![BigQuery](https://img.shields.io/badge/Google_BigQuery-669DF6?style=for-the-badge&logo=googlebigquery&logoColor=white)
</div>

---

## 01. Descripción del problema

El problema que se busca abordar consiste en analizar el comportamiento y las interacciones que se generan en las redes sociales a partir de publicaciones y comentarios relacionados con temas específicos. Se pretende comprender cómo estas interacciones pueden contribuir a la difusión de información, influir en la percepción de los usuarios y generar patrones de comunicación entre diferentes cuentas.

## 02. Arquitectura

```mermaid
flowchart LR
    A["📁 CSV originales<br/>Kaggle"]
    B["☁️ Amazon S3<br/>raw/"]
    C["⚙️ Apache Spark<br/>Docker"]
    D["📦 Amazon S3<br/>Parquet"]
    E["🕸️ Neo4j<br/>Docker"]
    F["☁️ BigQuery <br/> Consultas"]

    A --> B --> C --> D --> E --> F

    style A fill:#211A35,color:#fff,stroke:#A78BFA
    style B fill:#302447,color:#fff,stroke:#A78BFA
    style C fill:#49315F,color:#fff,stroke:#C4A7FF
    style D fill:#302447,color:#fff,stroke:#A78BFA
    style E fill:#211A35,color:#fff,stroke:#A78BFA
    style F fill:#211A35,color:#fff,stroke:#A78BFA
```

## 03. Integrantes

- Daiana Valentina Bandama Castro
- Karol Johana Barrero Garzón