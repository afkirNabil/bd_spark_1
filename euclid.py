# pyspark==3.x
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import LongType

spark = SparkSession.builder.appName("Euclides_MCD").getOrCreate()

# 1. Algoritmo de Euclides “puro” (versión iterativa eficiente)
def gcd_python(a: int, b: int) -> int:
    while b:
        a, b = b, a % b
    return abs(a)

# 2. Convertimos a UDF para que Spark lo ejecute en los workers
gcd_udf = udf(gcd_python, LongType())

# 3. Dataset de ejemplo: pares (a,b)
data = [(1071, 462), (24, 32), (321, 1155), (118, 236)]
df = spark.createDataFrame(data, ["a", "b"])

# 4. Cálculo distribuido: se ejecuta en paralelo por partición
result = df.withColumn("mcd", gcd_udf(col("a"), col("b")))
result.show()
