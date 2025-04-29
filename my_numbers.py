lista = [4,6,34,7,9,2,3,4,4,21,4,6,8,9,7,8,5,4,3,22,34,56,98]

# 1. Eliminar duplicados
sin_repetidos = list(set(lista))

# 2. Ordenar descendente
ordenados_desc = sorted(sin_repetidos, reverse=True)

# 3. Tomar los primeros 5
muestra = ordenados_desc[:5]

# 4. Elemento mayor (será el primero)
mayor = muestra[0]

# 5. Dos menores (últimos dos de la muestra)
menores = muestra[-2:]

# Mostrar resultados
print("Muestra de 5 elementos:", muestra)
print("Elemento mayor:", mayor)
print("Dos menores:", menores)
