# Lesionados en accidentes de tránsito en Colombia: análisis batch con Apache Spark

Tarea 3 del curso **Big Data (202016911)** de la UNAD: procesamiento de datos en batch con Apache Spark (PySpark) y en tiempo real con Apache Kafka y Spark Streaming.
Autor: Jonathan Manuel Zuñiga Altamiranda.

## Problema

¿Cómo se distribuyen **en el tiempo** (año, mes, día de la semana) y **en el territorio** (departamento y municipio) las personas lesionadas en accidentes de tránsito en Colombia? ¿Dónde y cuándo conviene concentrar las campañas de prevención vial?

## Conjunto de datos

| Campo | Detalle |
|---|---|
| Nombre | LESIONES ACCIDENTES DE TRÁNSITO |
| Fuente | Portal de Datos Abiertos de Colombia ([datos.gov.co](https://www.datos.gov.co/Seguridad-y-Defensa/LESIONES-ACCIDENTES-DE-TR-NSITO/ntej-qq7v)) |
| Tamaño | 644.042 registros, unos 26 MB (archivo descargado en octubre de 2026) |
| Periodo | 1 de enero de 2003 a 30 de septiembre de 2026 |
| Columnas | `FECHA HECHO`, `COD_DEPTO`, `DEPARTAMENTO`, `COD_MUNI`, `MUNICIPIO`, `CANTIDAD` |

Cada fila es un reporte de personas lesionadas en una fecha y un municipio. `CANTIDAD` indica cuántas personas resultaron lesionadas en ese reporte.

## Archivos

| Archivo | Descripción |
|---|---|
| `tarea3.py` | Análisis batch con Spark: carga desde HDFS, limpieza, transformación, EDA y visualización |
| `kafka_producer.py` | Productor de Kafka: simula sensores que envían temperatura y humedad cada segundo |
| `spark_streaming_consumer.py` | Consumidor con Spark Structured Streaming: estadísticas por ventana de 1 minuto y por sensor |

## Qué hace `tarea3.py`

1. **Carga** el CSV desde HDFS (`hdfs://localhost:9000/Tarea3/lesiones_transito.csv`).
2. **Limpieza**: renombra las columnas, cuenta los valores nulos, convierte la fecha (`dd/MM/yyyy`) y la cantidad a sus tipos, descarta los registros inválidos e identifica los departamentos «SIN ESTABLECER». Las filas repetidas **no** se eliminan, porque son reportes distintos de personas lesionadas.
3. **Transformación**: crea las columnas `anio`, `mes`, `nombre_mes`, `dia_semana` y `fin_de_semana`.
4. **Análisis exploratorio (EDA)**:
   - resumen general y estadísticas de `cantidad`;
   - lesionados por año, con la variación porcentual frente al año anterior (`Window` + `lag`);
   - los 10 departamentos y los 10 municipios con más lesionados;
   - lesionados por día de la semana y promedio diario en fin de semana frente a entre semana;
   - lesionados por mes;
   - reportes con 10 o más lesionados;
   - el mismo ranking de departamentos calculado con **RDD** (`map`, `reduceByKey`, `takeOrdered`);
   - una consulta con **Spark SQL**: promedio diario de lesionados por año.
5. **Visualización**: gráficos de barras en la terminal y archivos CSV con cada resultado en `~/resultados_tarea3/`.

## Requisitos

- Ubuntu Server 22.04 con Java 11
- Hadoop 3.3.6 (HDFS en ejecución)
- Apache Spark 3.5.8 en `/opt/spark`
- Apache Kafka 3.9.2 en `/opt/Kafka` y la librería `kafka-python`
- Python 3.10

## Ejecución del análisis batch

```bash
# 1) Como usuario hadoop: iniciar HDFS y cargar el archivo
su - hadoop
start-dfs.sh
hdfs dfs -mkdir -p /Tarea3
hdfs dfs -put -f /home/hadoop/lesiones_transito.csv /Tarea3/
exit

# 2) Como usuario vboxuser: variables de entorno (una sola vez, en ~/.bashrc)
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYTHONPATH=$SPARK_HOME/python:$(ls $SPARK_HOME/python/lib/py4j-*-src.zip)

# 3) Ejecutar el análisis
spark-submit --master "local[2]" --driver-memory 1g tarea3.py
```

## Ejecución del procesamiento en tiempo real

```bash
# Iniciar ZooKeeper y Kafka, y crear el tema
/opt/Kafka/bin/zookeeper-server-start.sh -daemon /opt/Kafka/config/zookeeper.properties
/opt/Kafka/bin/kafka-server-start.sh -daemon /opt/Kafka/config/server.properties
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092
