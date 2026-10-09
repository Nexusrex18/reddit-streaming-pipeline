"""Kafka -> HDFS sink.

Reads JSON comments from Kafka and writes Parquet to HDFS, partitioned by the
comment's own event time (not ingest time):
  <HDFS_RAW_PATH>/year=YYYY/month=MM/day=DD/hour=HH/
"""
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "reddit-comments")
HDFS = os.environ.get("HDFS_URL", "hdfs://namenode:9000")
RAW = HDFS + os.environ.get("HDFS_RAW_PATH", "/data/reddit/raw")
CKPT = HDFS + os.environ.get("HDFS_CHECKPOINT_PATH", "/data/reddit/_checkpoints/raw_sink")
TRIGGER = os.environ.get("SINK_TRIGGER_SECONDS", "30") + " seconds"
MAX_OFFSETS = os.environ.get("SINK_MAX_OFFSETS_PER_TRIGGER", "50000")

schema = StructType([
    StructField("id", StringType()),
    StructField("author", StringType()),
    StructField("body", StringType()),
    StructField("subreddit", StringType()),
    StructField("created_utc", LongType()),
    StructField("ups", LongType()),
    StructField("downs", LongType()),
    StructField("score", LongType()),
])

spark = (SparkSession.builder.appName("reddit-hdfs-sink")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", MAX_OFFSETS)
       .option("failOnDataLoss", "false")
       .load())

parsed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("c"))
          .select("c.*")
          .where(F.col("subreddit").isNotNull() & F.col("created_utc").isNotNull()))

event_ts = F.to_timestamp(F.from_unixtime("created_utc"))
out = (parsed
       .withColumn("event_ts", event_ts)
       .withColumn("year", F.date_format("event_ts", "yyyy"))
       .withColumn("month", F.date_format("event_ts", "MM"))
       .withColumn("day", F.date_format("event_ts", "dd"))
       .withColumn("hour", F.date_format("event_ts", "HH")))

query = (out.writeStream
         .format("parquet")
         .option("path", RAW)
         .option("checkpointLocation", CKPT)
         .partitionBy("year", "month", "day", "hour")
         .outputMode("append")
         .trigger(processingTime=TRIGGER)
         .start())

print(f"[hdfs-sink] {TOPIC} -> {RAW} (trigger {TRIGGER})", flush=True)
query.awaitTermination()
