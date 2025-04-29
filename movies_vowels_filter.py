from pyspark.sql import SparkSession
import re

# Crear sesión Spark
spark = SparkSession.builder.appName("MoviesWithVowels").getOrCreate()
sc = spark.sparkContext

# Leer el archivo (local)
lines = sc.textFile("peliculas.txt")

# Separar campos de cada línea por comas respetando comillas (CSV semiestructurado)
def parse_line(line):
    fields = re.findall(r'"([^"]*)"', line)  # Extrae solo lo que va entre comillas
    return fields  # Devuelve lista: [id, titulo, director, duración, ...]

# Contar vocales en una cadena
def contar_vocales(cadena):
    return sum(1 for c in cadena.lower() if c in "aeiouáéíóú")

# Crear RDD con líneas ya parseadas
peliculas_rdd = lines.map(parse_line)

# Filtrar películas con 4 o más vocales en el título
peliculas_con_vocales = peliculas_rdd.filter(lambda campos: contar_vocales(campos[1]) >= 4)

# Mostrar resultados
print("\n🎬 Películas con 4 o más vocales en el título:\n")
for p in peliculas_con_vocales.collect():
    print(f"- {p[1]}")

spark.stop()
