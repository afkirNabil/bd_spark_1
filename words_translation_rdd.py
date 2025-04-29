from pyspark.sql import SparkSession

# 1. Crear sesión Spark
spark = SparkSession.builder.appName("WordsTranslationRDD").getOrCreate()
sc = spark.sparkContext

# 2. Listas de palabras
english = ["hello", "table", "angel", "cat", "dog", "animal", "chocolate", "dark", "doctor", "hospital", "computer"]
spanish = ["hola", "mesa", "angel", "gato", "perro", "animal", "chocolate", "oscuro", "doctor", "hospital", "ordenador"]

# 3. Crear RDD de tuplas (inglés, español)
word_pairs_rdd = sc.parallelize(zip(english, spanish))

# 4. Palabras que se escriben igual
same_words = word_pairs_rdd.filter(lambda x: x[0] == x[1])
same_words.map(lambda x: f"{x[0]}::{x[1]}").saveAsTextFile("output/words/same_words")

# 5. Palabras que son distintas
different_pairs = word_pairs_rdd.filter(lambda x: x[0] != x[1])
different_pairs.map(lambda x: f"{x[0]}::{x[1]}").saveAsTextFile("output/words/different_pairs")

# 6. Lista única con todas las palabras distintas entre sí
distinct_words = different_pairs.flatMap(lambda x: [x[0], x[1]]).distinct()
distinct_words.saveAsTextFile("output/words/distinct_words")

# 7. Agrupar todas las palabras en vocales y consonantes
all_words = sc.parallelize(english + spanish).distinct()

def starts_with_vowel(word):
    return word[0].lower() in "aeiou"

vowels = all_words.filter(starts_with_vowel)
vowels.saveAsTextFile("output/words/vowels")

consonants = all_words.filter(lambda w: not starts_with_vowel(w))
consonants.saveAsTextFile("output/words/consonants")

# Finalizar sesión
spark.stop()
