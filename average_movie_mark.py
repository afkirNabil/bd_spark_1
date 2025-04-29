from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, avg

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("AverageMovieMark>3") \
    .getOrCreate()

# Ruta local del fichero ratings.txt (ajústala si está en otro sitio)
INPUT = "ratings.txt"
OUTPUT = "output/average_movie_mark"

# 1) Leer como texto plano
raw = spark.read.text(INPUT)

# 2) Dividir cada línea y extraer columnas
ratings = raw.select(split(col("value"), "::").alias("cols")).select(
    col("cols")[0].cast("int").alias("user_id"),
    col("cols")[1].cast("int").alias("movie_id"),
    col("cols")[2].cast("double").alias("rating"),
    col("cols")[3].cast("long").alias("timestamp")
)

# 3) Calcular la media por película
movies_avg = ratings.groupBy("movie_id").agg(avg("rating").alias("avg_rating"))

# 4) Filtrar películas con nota media > 3
movies_above_3 = movies_avg.filter(col("avg_rating") > 3)

# 5) Mostrar los primeros resultados por consola
print("Películas con media > 3:")
movies_above_3.show(10)

# 6) Guardar el resultado en formato CSV local
movies_above_3.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(OUTPUT)

# Finalizar la sesión de Spark
spark.stop()
