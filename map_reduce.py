from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

# Crear una SparkSession
spark = SparkSession.builder \
    .appName("Dias calurosos y frios") \
    .getOrCreate()

# Cargar el CSV en un DataFrame
df = spark.read.csv("./Algiers_Weather_Data.csv", header=True, inferSchema=True)

# Mostrar las primeras filas para entender la estructura del dataset
df.show(5)

# Definimos las condiciones para dias calurosos y frios
# Asumimos que la columna relevante es 'Max Temperature'

# Añadir una columna que categorice los dias
df_with_category = df.withColumn(
    "day_category",
    when(col("temperature_2m_max (°C)") > 30, "Caluroso")
    .when(col("temperature_2m_max (°C)") < 15, "Frio")
    .otherwise("Templado")
)

# Contar los dias calurosos y frios
result = df_with_category.groupBy("day_category").agg(count("day_category").alias("count"))

# Mostrar el resultado
result.show()

# Parar la SparkSession
spark.stop()
