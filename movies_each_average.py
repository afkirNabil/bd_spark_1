#!/usr/bin/env python
# movies_each_average.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, avg

spark = (SparkSession.builder
         .appName("MoviesEachAverage")
         .getOrCreate())

# Ruta del fichero de entrada (ajústala a tu sistema de ficheros)
INPUT = "ratings.txt"
OUTPUT = "output/movies_each_average"

# 1) Leer como texto y trocear por '::'
raw = spark.read.text(INPUT)

ratings = (raw
           .select(split(col("value"), "::").alias("cols"))
           .select(
               col("cols")[0].cast("int").alias("user_id"),
               col("cols")[1].cast("int").alias("movie_id"),
               col("cols")[2].cast("double").alias("rating"),
               col("cols")[3].cast("long").alias("ts"))
          )

# 2) Media por película
movies_avg = (ratings
              .groupBy("movie_id")
              .agg(avg("rating").alias("avg_rating")))

# 3) Guardar resultado (parquet, csv, delta, …). Ejemplo: CSV con cabecera
(movies_avg
 .write
 .mode("overwrite")
 .option("header", True)
 .csv(OUTPUT))

spark.stop()
