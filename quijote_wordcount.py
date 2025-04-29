from pyspark.sql import SparkSession
import sys

# Parámetros
PALABRA_ELEGIDA = "dios"  # palabra a buscar (sin espacios ni tildes)
HDFS_OUTPUT_PATH = "output/quijote_palabras"

# ───────────────────────────────
# 1. Sesión de Spark
# ───────────────────────────────
spark = SparkSession.builder.appName("QuijoteWordCount").getOrCreate()
sc = spark.sparkContext

# ───────────────────────────────
# 2. Leer el texto de El Quijote
# ───────────────────────────────
# Puedes cambiar esta ruta por una local si no tienes HDFS:
# Ej: "quijote.txt"
rdd_texto = sc.textFile("quijote.txt")

# ───────────────────────────────
# 3. Limpiar y dividir en palabras
# ───────────────────────────────
import re
def limpiar(linea):
    return re.findall(r'\b\w+\b', linea.lower(), flags=re.UNICODE)

palabras_rdd = rdd_texto.flatMap(limpiar)

# ───────────────────────────────
# 4. Crear pares (palabra, 1) y contar ocurrencias
# ───────────────────────────────
palabra_counts = palabras_rdd.map(lambda w: (w, 1)) \
                              .reduceByKey(lambda a, b: a + b)

# ───────────────────────────────
# 5. Mostrar cuántas veces aparece una palabra
# ───────────────────────────────
palabra_elegida_limpia = PALABRA_ELEGIDA.lower()
ocurrencias = palabra_counts.filter(lambda x: x[0] == palabra_elegida_limpia).collect()

if ocurrencias:
    print(f"\n📌 La palabra '{PALABRA_ELEGIDA}' aparece {ocurrencias[0][1]} veces.\n")
else:
    print(f"\n📌 La palabra '{PALABRA_ELEGIDA}' aparece 0 veces.\n")


# ───────────────────────────────
# 6. Ordenar palabras por frecuencia (mayor a menor)
# ───────────────────────────────
ordenado = palabra_counts.map(lambda x: (x[1], x[0])) \
                         .sortByKey(ascending=False) \
                         .map(lambda x: f"{x[1]}\t{x[0]}")

# ───────────────────────────────
# 7. Guardar en HDFS
# ───────────────────────────────
ordenado.saveAsTextFile(HDFS_OUTPUT_PATH)

print(f"✅ Resultado guardado en: {HDFS_OUTPUT_PATH}")

# ───────────────────────────────
# Fin
# ───────────────────────────────
spark.stop()
