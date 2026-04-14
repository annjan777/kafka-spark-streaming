from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("KafkaSparkStreaming")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("product", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("status", StringType()),
    StructField("timestamp", StringType()),
    StructField("region", StringType()),
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "order_events")
    .load()
)

parsed = (
    df.select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", F.to_timestamp("timestamp"))
    .withColumn("order_value", F.col("quantity") * F.col("unit_price"))
)

agg = (
    parsed
    .groupBy("product", "region")
    .agg(F.sum("order_value").alias("revenue"))
)

query = (
    agg.writeStream
    .outputMode("complete")
    .format("console")
    .start()
)

query.awaitTermination()
