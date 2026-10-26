from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, round as redondear
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType

# Sesión de Spark (pocas particiones porque la máquina virtual es pequeña)
spark = SparkSession.builder \
    .appName("SensorDataAnalysis") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Esquema de los mensajes JSON que envía kafka_producer.py
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", FloatType()),
    StructField("humidity", FloatType()),
    StructField("timestamp", TimestampType())
])

# Lectura en streaming desde el topic sensor_data
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

# Convertir el valor (JSON) en columnas
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
              .select("data.*")

# Estadísticas por ventana de 1 minuto y por sensor
windowed_stats = parsed_df \
    .groupBy(window(col("timestamp"), "1 minute"), "sensor_id") \
    .agg(count("*").alias("eventos"),
         redondear(avg("temperature"), 2).alias("temp_promedio"),
         redondear(avg("humidity"), 2).alias("humedad_promedio")) \
    .orderBy(col("window").desc(), "sensor_id")

# Mostrar resultados en consola cada 10 segundos
query = windowed_stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 30) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
