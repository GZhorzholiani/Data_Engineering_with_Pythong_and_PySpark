from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import expr, col, window, lit, current_timestamp
from Configs.config import spark_consumer_conf, snowflake_conf
from spark_event_consuming.helper_functions import get_latest_schema

spark = SparkSession.builder \
    .appName("user_registration_tumbling_room3") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", str(spark.sparkContext.defaultParallelism))

subject = "user_registration-value"

schema = get_latest_schema(subject)

topic = 'user_registration'

raw_stream = spark.readStream \
    .format("kafka") \
    .options(**spark_consumer_conf) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .withColumn("value", expr("substring(value, 6, length(value)-5)")) \
    .select("value")


user_registration = raw_stream.select(from_avro(col("value"), schema).alias("user_registration")) \
    .select(expr("CAST(user_registration.timestamp AS timestamp)").alias("timestamp")
            ,col("user_registration.user_id"))

user_registration = user_registration \
    .withWatermark("timestamp", '1 hour') \
    .groupBy(window("timestamp", "1 hour")) \
    .count() \
    .select(col("window.start"),
            col("window.end"),
            lit("tumbling"),
            col("count"),
            current_timestamp())

def write_to_snowflake(batch_df: DataFrame, batch_id):
    batch_df.write \
        .format("snowflake") \
        .options(**snowflake_conf) \
        .option("dbtable", 'user_registration_metrics') \
        .mode("append") \
        .save()

query = user_registration.writeStream \
    .foreachBatch(write_to_snowflake) \
    .outputMode("append") \
    .option("checkpointLocation", "checkPoints/UserRegistrationTumbling") \
    .trigger(processingTime="10 minutes") \
    .start()


query.awaitTermination()