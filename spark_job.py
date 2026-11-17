"""Spark event-time aggregation. Run inside the Spark container."""
import os

from pyspark.sql import SparkSession, functions as F, types as T

BROKER = os.environ.get("KAFKA_INTERNAL", "broker:19092")
schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("timestamp", T.StringType()),
    T.StructField("room_id", T.StringType()),
    T.StructField("temperature_c", T.DoubleType()),
    T.StructField("humidity_pct", T.DoubleType()),
    T.StructField("co2_ppm", T.DoubleType()),
    T.StructField("occupancy", T.IntegerType()),
    T.StructField("anomaly", T.BooleanType()),
    T.StructField("prediction_co2", T.DoubleType()),
    T.StructField("processed_at", T.StringType()),
])

spark = SparkSession.builder.appName("ClassroomWindows").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BROKER)
       .option("subscribe", "classroom.processed")
       .option("startingOffsets", "earliest").load())
parsed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
          .select("e.*").where("event_id IS NOT NULL")
          .withColumn("event_time", F.to_timestamp("timestamp")))
windows = (parsed.withWatermark("event_time", "2 minutes")
           .groupBy(F.window("event_time", "5 minutes"), "room_id")
           .agg(F.count("*").alias("event_count"),
                F.avg("co2_ppm").alias("avg_co2_ppm"),
                F.avg("temperature_c").alias("avg_temperature_c"),
                F.sum(F.col("anomaly").cast("int")).alias("anomaly_count")))
out = windows.select(
    F.col("room_id").cast("string").alias("key"),
    F.to_json(F.struct(F.date_format("window.start", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("window_start"),
                       F.date_format("window.end", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("window_end"),
                       "room_id", "event_count", "avg_co2_ppm", "avg_temperature_c",
                       "anomaly_count")).alias("value"))
query = (out.writeStream.format("kafka")
         .option("kafka.bootstrap.servers", BROKER)
         .option("topic", "classroom.windows")
         .option("checkpointLocation", "/workspace/data/spark-checkpoint-v2")
         .outputMode("append").start())
query.awaitTermination()
