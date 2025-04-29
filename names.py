from pyspark.sql import SparkSession
import random

# 1. Crear sesión de Spark
spark = SparkSession.builder.appName("NamesGroupingAndSampling").getOrCreate()
sc = spark.sparkContext

# 2. Cadena de nombres original
cadena = "Juan, Jimena, Luis, Cristian, Laura, Lorena, Cristina, Jacobo, Jorge"

# 3. Transformar la cadena en lista
names_list = [name.strip() for name in cadena.split(",")]

# 4. Crear RDD a partir de la lista
names_rdd = sc.parallelize(names_list)

# 5. Agrupar por inicial
# Crear tuplas (inicial, nombre)
pairs_rdd = names_rdd.map(lambda name: (name[0], name))

# Agrupar todos los nombres por inicial
grouped_rdd = pairs_rdd.groupByKey().mapValues(list)

# Mostrar agrupados
print("Nombres agrupados por inicial:")
for item in grouped_rdd.collect():
    print(item)

# 6. Obtener una muestra de 5 elementos SIN REPETIR
sample_5 = names_rdd.distinct().takeSample(False, 5)
print("\nMuestra de 5 nombres distintos:", sample_5)

# 7. Muestra con aproximadamente la mitad de registros (pueden repetirse)
# Número total de nombres
n_total = names_rdd.count()
# Aproximadamente la mitad
sample_half = names_rdd.takeSample(True, n_total // 2)
print("\nMuestra con aproximadamente la mitad de registros:", sample_half)

# Finalizar sesión Spark
spark.stop()
