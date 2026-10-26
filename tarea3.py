# -*- coding: utf-8 -*-
"""
Tarea 3 - Big Data (UNAD) - Procesamiento en batch con Apache Spark
Estudiante: Jonathan Manuel Zuñiga Altamiranda

Problema:
    ¿Cómo se distribuyen en el tiempo y en el territorio las personas lesionadas
    en accidentes de tránsito en Colombia, y dónde y cuándo conviene concentrar
    las campañas de prevención?

Conjunto de datos:
    "LESIONES ACCIDENTES DE TRÁNSITO" - Portal de Datos Abiertos de Colombia
    https://www.datos.gov.co/Seguridad-y-Defensa/LESIONES-ACCIDENTES-DE-TR-NSITO/ntej-qq7v
    Columnas: FECHA HECHO, COD_DEPTO, DEPARTAMENTO, COD_MUNI, MUNICIPIO, CANTIDAD

Ejecución (usuario vboxuser):
    python3 tarea3.py
    # o, usando el clúster Spark standalone:
    spark-submit --master spark://BIGDATA:7077 tarea3.py
"""

import csv
import os
from operator import add

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
RUTA_HDFS = "hdfs://localhost:9000/Tarea3/lesiones_transito.csv"
CARPETA_RESULTADOS = os.path.expanduser("~/resultados_tarea3")
ULTIMO_ANIO_COMPLETO = 2025          # 2026 solo tiene datos parciales

DIAS = ["Domingo", "Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado"]
MESES = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio",
         "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]


# ---------------------------------------------------------------------------
# Funciones de apoyo para visualizar y guardar resultados
# ---------------------------------------------------------------------------
def titulo(texto):
    print("\n" + "=" * 78)
    print("  " + texto)
    print("=" * 78)


def barras(filas, etiqueta, valor, ancho=40, extra=None):
    """Dibuja un gráfico de barras horizontal en la terminal."""
    maximo = max(f[valor] for f in filas) or 1
    for f in filas:
        n = int(round(f[valor] / maximo * ancho))
        texto_extra = "  " + extra(f) if extra else ""
        print("  {:<24} {:<{w}} {:>9,}{}".format(
            str(f[etiqueta])[:24], "#" * n, f[valor], texto_extra, w=ancho))


def guardar_csv(nombre, filas, columnas):
    """Guarda un resultado agregado (pequeño) en un CSV local."""
    ruta = os.path.join(CARPETA_RESULTADOS, nombre + ".csv")
    with open(ruta, "w", newline="", encoding="utf-8") as archivo:
        escritor = csv.writer(archivo)
        escritor.writerow(columnas)
        for f in filas:
            escritor.writerow([f[c] for c in columnas])


def graficar_png(nombre, filas, etiqueta, valor, titulo_grafico, horizontal=True):
    """Si matplotlib está instalado, guarda además el gráfico como PNG."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return False
    etiquetas = [str(f[etiqueta]) for f in filas]
    valores = [f[valor] for f in filas]
    fig, ax = plt.subplots(figsize=(10, 5.5))
    if horizontal:
        ax.barh(etiquetas[::-1], valores[::-1], color="#2563eb")
        ax.set_xlabel("Personas lesionadas")
    else:
        ax.bar(etiquetas, valores, color="#2563eb")
        ax.set_ylabel("Personas lesionadas")
        plt.xticks(rotation=45)
    ax.set_title(titulo_grafico)
    fig.tight_layout()
    fig.savefig(os.path.join(CARPETA_RESULTADOS, nombre + ".png"), dpi=120)
    plt.close(fig)
    return True


# ---------------------------------------------------------------------------
# 1. CARGA DE DATOS DESDE HDFS
# ---------------------------------------------------------------------------
spark = SparkSession.builder.appName("Tarea3_Lesionados_Transito").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
os.makedirs(CARPETA_RESULTADOS, exist_ok=True)

titulo("1. CARGA DE DATOS DESDE HDFS")
crudo = (spark.read.format("csv")
         .option("header", "true")
         .option("encoding", "UTF-8")
         .load(RUTA_HDFS))

total_crudo = crudo.count()
print("Archivo:", RUTA_HDFS)
print("Registros leídos: {:,}".format(total_crudo))
print("Particiones del DataFrame:", crudo.rdd.getNumPartitions())
crudo.printSchema()
crudo.show(5, truncate=False)


# ---------------------------------------------------------------------------
# 2. LIMPIEZA
# ---------------------------------------------------------------------------
titulo("2. LIMPIEZA DE DATOS")

# 2.1 Nombres de columna sin espacios y en minúscula
df = (crudo
      .withColumnRenamed("FECHA HECHO", "fecha_hecho")
      .withColumnRenamed("COD_DEPTO", "cod_depto")
      .withColumnRenamed("DEPARTAMENTO", "departamento")
      .withColumnRenamed("COD_MUNI", "cod_muni")
      .withColumnRenamed("MUNICIPIO", "municipio")
      .withColumnRenamed("CANTIDAD", "cantidad"))

# 2.2 Valores nulos por columna
print("Valores nulos o vacíos por columna:")
df.select([F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1)
                 .otherwise(0)).alias(c) for c in df.columns]).show()

# 2.3 Conversión de tipos y normalización de texto
df = (df
      .withColumn("fecha", F.to_date(F.trim("fecha_hecho"), "dd/MM/yyyy"))
      .withColumn("cantidad", F.col("cantidad").cast("int"))
      .withColumn("departamento", F.upper(F.trim("departamento")))
      .withColumn("municipio", F.upper(F.trim("municipio")))
      .withColumn("cod_muni", F.trim("cod_muni"))
      .drop("fecha_hecho"))

# 2.4 Eliminar registros inválidos (fecha no convertible o cantidad <= 0)
df = df.filter(F.col("fecha").isNotNull() &
               F.col("cantidad").isNotNull() &
               (F.col("cantidad") > 0))
total_limpio = df.count()
print("Registros válidos tras convertir tipos: {:,} (descartados: {:,})"
      .format(total_limpio, total_crudo - total_limpio))

# 2.5 Departamento sin establecer: se conservan para los totales nacionales,
#     pero se excluyen de los análisis por territorio.
sin_depto = df.filter(F.col("departamento") == "SIN ESTABLECER").count()
print("Registros con departamento 'SIN ESTABLECER': {:,}".format(sin_depto))

# 2.6 Filas repetidas: NO se eliminan. Cada fila es un reporte de lesionados;
#     dos personas lesionadas el mismo día en la misma ciudad generan filas
#     idénticas, y borrarlas subestimaría los lesionados.
repetidas = total_limpio - df.dropDuplicates().count()
print("Filas idénticas a otra: {:,} (se conservan: son reportes distintos)"
      .format(repetidas))

df.cache()


# ---------------------------------------------------------------------------
# 3. TRANSFORMACIÓN: nuevas columnas de tiempo
# ---------------------------------------------------------------------------
titulo("3. TRANSFORMACIÓN")
df = (df
      .withColumn("anio", F.year("fecha"))
      .withColumn("mes", F.month("fecha"))
      .withColumn("nombre_mes", F.element_at(F.array(*[F.lit(m) for m in MESES]), F.month("fecha")))
      .withColumn("num_dia", F.dayofweek("fecha"))            # 1 = domingo
      .withColumn("dia_semana", F.element_at(F.array(*[F.lit(d) for d in DIAS]), F.dayofweek("fecha")))
      .withColumn("fin_de_semana", F.col("num_dia").isin(1, 7)))
df.printSchema()
df.show(5, truncate=False)


# ---------------------------------------------------------------------------
# 4. ANÁLISIS EXPLORATORIO (EDA)
# ---------------------------------------------------------------------------
titulo("4.1 RESUMEN GENERAL")
resumen = df.agg(
    F.count("*").alias("registros"),
    F.sum("cantidad").alias("lesionados"),
    F.min("fecha").alias("fecha_inicial"),
    F.max("fecha").alias("fecha_final"),
    F.countDistinct("departamento").alias("departamentos"),
    F.countDistinct("cod_muni").alias("municipios")).first()
for clave, valor in resumen.asDict().items():
    print("  {:<15} {}".format(clave, "{:,}".format(valor) if isinstance(valor, int) else valor))
print("\nEstadísticas de la columna 'cantidad' (lesionados por reporte):")
df.select("cantidad").summary("count", "mean", "stddev", "min", "50%", "max").show()

# --- 4.2 Evolución anual con variación frente al año anterior (Window + lag)
titulo("4.2 LESIONADOS POR AÑO")
por_anio = df.groupBy("anio").agg(F.sum("cantidad").alias("lesionados"))
ventana = Window.orderBy("anio")
por_anio = (por_anio
            .withColumn("variacion_pct",
                        F.round((F.col("lesionados") - F.lag("lesionados").over(ventana))
                                / F.lag("lesionados").over(ventana) * 100, 1))
            .orderBy("anio"))
filas_anio = por_anio.collect()
barras(filas_anio, "anio", "lesionados",
       extra=lambda f: "" if f["variacion_pct"] is None else "{:+.1f}%".format(f["variacion_pct"]))
print("  (El año 2026 solo incluye datos hasta la fecha final del archivo.)")
guardar_csv("lesionados_por_anio", filas_anio, ["anio", "lesionados", "variacion_pct"])
graficar_png("lesionados_por_anio", filas_anio, "anio", "lesionados",
             "Personas lesionadas en accidentes de tránsito por año", horizontal=False)

# --- 4.3 Top 10 departamentos con su porcentaje del total
titulo("4.3 TOP 10 DEPARTAMENTOS CON MÁS LESIONADOS")
con_depto = df.filter(F.col("departamento") != "SIN ESTABLECER")
total_lesionados = con_depto.agg(F.sum("cantidad")).first()[0]
top_deptos = (con_depto.groupBy("departamento")
              .agg(F.sum("cantidad").alias("lesionados"))
              .withColumn("porcentaje", F.round(F.col("lesionados") / total_lesionados * 100, 1))
              .orderBy(F.desc("lesionados"))
              .limit(10).collect())
barras(top_deptos, "departamento", "lesionados", extra=lambda f: "{:.1f}%".format(f["porcentaje"]))
guardar_csv("top10_departamentos", top_deptos, ["departamento", "lesionados", "porcentaje"])
graficar_png("top10_departamentos", top_deptos, "departamento", "lesionados",
             "Top 10 departamentos con más lesionados")

# --- 4.4 Top 10 municipios (se agrupa por código DANE: hay nombres repetidos)
titulo("4.4 TOP 10 MUNICIPIOS CON MÁS LESIONADOS")
top_munis = (con_depto.groupBy("cod_muni", "municipio", "departamento")
             .agg(F.sum("cantidad").alias("lesionados"))
             .orderBy(F.desc("lesionados"))
             .limit(10).collect())
barras(top_munis, "municipio", "lesionados", extra=lambda f: f["departamento"])
guardar_csv("top10_municipios", top_munis, ["cod_muni", "municipio", "departamento", "lesionados"])
graficar_png("top10_municipios", top_munis, "municipio", "lesionados",
             "Top 10 municipios con más lesionados")

# --- 4.5 Día de la semana (solo años completos)
titulo("4.5 LESIONADOS POR DÍA DE LA SEMANA (años completos)")
completos = df.filter(F.col("anio") <= ULTIMO_ANIO_COMPLETO)
por_dia = (completos.groupBy("num_dia", "dia_semana")
           .agg(F.sum("cantidad").alias("lesionados"))
           .orderBy("num_dia").collect())
barras(por_dia, "dia_semana", "lesionados")
guardar_csv("lesionados_por_dia_semana", por_dia, ["dia_semana", "lesionados"])
graficar_png("lesionados_por_dia_semana", por_dia, "dia_semana", "lesionados",
             "Lesionados por día de la semana", horizontal=False)

finde = (completos.groupBy("fin_de_semana")
         .agg(F.sum("cantidad").alias("lesionados"),
              F.countDistinct("fecha").alias("dias")))
finde = finde.withColumn("promedio_por_dia", F.round(F.col("lesionados") / F.col("dias"), 1))
print("\nPromedio de lesionados por día: fin de semana vs. entre semana")
finde.show()

# --- 4.6 Mes del año (solo años completos)
titulo("4.6 LESIONADOS POR MES (años completos)")
por_mes = (completos.groupBy("mes", "nombre_mes")
           .agg(F.sum("cantidad").alias("lesionados"))
           .orderBy("mes").collect())
barras(por_mes, "nombre_mes", "lesionados")
guardar_csv("lesionados_por_mes", por_mes, ["nombre_mes", "lesionados"])
graficar_png("lesionados_por_mes", por_mes, "nombre_mes", "lesionados",
             "Lesionados por mes del año", horizontal=False)

# --- 4.7 Reportes con muchos lesionados (posibles accidentes masivos)
titulo("4.7 REPORTES CON 10 O MÁS LESIONADOS")
masivos = df.filter(F.col("cantidad") >= 10)
print("Reportes con 10 o más lesionados: {:,}".format(masivos.count()))
masivos.select("fecha", "departamento", "municipio", "cantidad") \
       .orderBy(F.desc("cantidad")).show(10, truncate=False)

# --- 4.8 El mismo cálculo con RDD (map + reduceByKey + takeOrdered)
titulo("4.8 TOP 5 DEPARTAMENTOS CALCULADO CON RDD")
rdd_top = (con_depto.rdd
           .map(lambda fila: (fila["departamento"], fila["cantidad"]))   # transformación
           .reduceByKey(add)                                             # transformación
           .takeOrdered(5, key=lambda par: -par[1]))                     # acción
for depto, total in rdd_top:
    print("  {:<24} {:>9,}".format(depto, total))

# --- 4.9 Consulta con Spark SQL: promedio diario de lesionados por año
titulo("4.9 SPARK SQL: PROMEDIO DIARIO DE LESIONADOS (últimos 5 años completos)")
df.createOrReplaceTempView("lesiones")
spark.sql("""
    SELECT anio,
           SUM(cantidad)                                   AS lesionados,
           COUNT(DISTINCT fecha)                           AS dias_con_reportes,
           ROUND(SUM(cantidad) / COUNT(DISTINCT fecha), 1) AS promedio_diario
    FROM lesiones
    WHERE anio BETWEEN {} AND {}
    GROUP BY anio
    ORDER BY anio
""".format(ULTIMO_ANIO_COMPLETO - 4, ULTIMO_ANIO_COMPLETO)).show()


# ---------------------------------------------------------------------------
# 5. CIERRE
# ---------------------------------------------------------------------------
titulo("5. RESULTADOS GUARDADOS")
for nombre in sorted(os.listdir(CARPETA_RESULTADOS)):
    print("  " + os.path.join(CARPETA_RESULTADOS, nombre))
print("\nAnálisis finalizado.")
spark.stop()
