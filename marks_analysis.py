from pyspark.sql import SparkSession
import os

# Crear carpeta de salida si no existe
os.makedirs("output", exist_ok=True)

# ────────────────────────────────
# 0. Sesión Spark
# ────────────────────────────────
spark = SparkSession.builder.appName("MarksAnalysis").getOrCreate()
sc = spark.sparkContext

resultados = []  # ← Acumulador de texto para guardar al final

# ────────────────────────────────
# 1. Función auxiliar de lectura
# ────────────────────────────────
def load_subject(path: str, subject_name: str):
    rdd = (
        sc.textFile(path)
          .filter(lambda l: l.strip() != "")
          .map(lambda l: l.split(","))
          .map(lambda p: (p[0].strip(), float(p[1])))
    )
    return rdd, rdd.keys().map(lambda name: (name, subject_name))

# ────────────────────────────────
# 2. Cargar los ficheros
# ────────────────────────────────
math_rdd, math_subj = load_subject("notas_matematicas.txt", "Matematicas")
phys_rdd, phys_subj = load_subject("notas_fisica.txt",     "Fisica")
eng_rdd,  eng_subj  = load_subject("notas_ingles.txt",      "Ingles")

# 3 RDD de pares (alumno, nota)
resultados.append("— Ejemplo de registros en cada RDD —")
resultados.append(f"Matemáticas: {math_rdd.take(3)}")
resultados.append(f"Física     : {phys_rdd.take(3)}")
resultados.append(f"Inglés     : {eng_rdd.take(3)}")

# ────────────────────────────────
# 3. RDD combinado
# ────────────────────────────────
all_marks = math_rdd.union(phys_rdd).union(eng_rdd)

# 4. Nota más baja
lowest_mark = all_marks.reduceByKey(min)
resultados.append("\nNota más baja por alumno:")
for n in lowest_mark.collect():
    resultados.append(str(n))

# 5. Nota media
avg_mark = (
    all_marks.mapValues(lambda x: (x, 1))
             .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
             .mapValues(lambda s: round(s[0] / s[1], 2))
)
resultados.append("\nNota media por alumno:")
for n in avg_mark.collect():
    resultados.append(str(n))

# 6. Suspensos por asignatura
def fails_count(rdd):
    return rdd.filter(lambda x: x[1] < 5).keys().distinct().count()

fails_per_subject = [
    ("Matematicas", fails_count(math_rdd)),
    ("Fisica",      fails_count(phys_rdd)),
    ("Ingles",      fails_count(eng_rdd)),
]
resultados.append("\nNúmero de estudiantes que suspenden cada asignatura:")
for s in fails_per_subject:
    resultados.append(str(s))

# 7. Asignatura más difícil
worst_subject = max(fails_per_subject, key=lambda x: x[1])
resultados.append(f"\nAsignatura con más suspensos: {worst_subject}")

# 8. Notas ≥7 por alumno
notables = (
    all_marks.mapValues(lambda n: 1 if n >= 7 else 0)
             .reduceByKey(lambda a, b: a + b)
)
resultados.append("\nTotal de notas ≥7 por alumno:")
for n in notables.collect():
    resultados.append(str(n))

# 9. Alumnos ausentes en inglés
all_students = all_marks.keys().distinct()
english_students = eng_rdd.keys().distinct()
not_in_english = all_students.subtract(english_students).collect()
resultados.append(f"\nAlumnos que no se presentaron a Inglés: {not_in_english}")

# 10. Nº asignaturas por alumno
subjects_rdd = math_subj.union(phys_subj).union(eng_subj)
subjects_per_student = (
    subjects_rdd.groupByKey()
                .mapValues(lambda subj: len(set(subj)))
)
resultados.append("\nAsignaturas cursadas por alumno:")
for n in subjects_per_student.collect():
    resultados.append(str(n))

# 11. RDD alumno → [notas]
marks_per_student = (
    all_marks.groupByKey()
             .mapValues(list)
)
resultados.append("\nEjemplo de estructura (alumno, [notas]):")
for n in marks_per_student.take(5):
    resultados.append(str(n))

# ────────────────────────────────
# Guardar todo en output/resultados.txt
# ────────────────────────────────
with open("output/resultados.txt", "w") as f:
    f.write("\n".join(resultados))

print("✅ Resultados guardados en 'output/resultados.txt'.")

spark.stop()
