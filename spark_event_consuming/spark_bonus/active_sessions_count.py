from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import expr, col, window, sum, lit

from Configs.config import spark_consumer_conf, snowflake_conf
from spark_event_consuming.helper_functions import get_latest_schema

spark = SparkSession.builder \
    .appName("active_sessions_count") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", str(spark.sparkContext.defaultParallelism))

subject_sign_in = "sign_in-value"

subject_sign_out = "sign_out-value"

sign_in_schema = get_latest_schema(subject_sign_in)
sign_out_schema = get_latest_schema(subject_sign_out)

topic_sign_in = 'sign_in'

topic_sign_out = 'sign_out'

sign_in_stream = spark.readStream \
    .format("kafka") \
    .options(**spark_consumer_conf) \
    .option("subscribe", topic_sign_in) \
    .option("startingOffsets", "earliest") \
    .load() \
    .withColumn("value", expr("substring(value, 6, length(value)-5)")) \
    .select("value") \
    .select(from_avro(col("value"), sign_in_schema).alias("sign_in")) \
    .select(expr("CAST(sign_in.timestamp AS timestamp)").alias("timestamp"), lit(1).alias("status")) \
    .withWatermark("timestamp", "1 hour") \


sign_out_stream = spark.readStream \
    .format("kafka") \
    .options(**spark_consumer_conf) \
    .option("subscribe", topic_sign_out) \
    .option("startingOffsets", "earliest") \
    .load() \
    .withColumn("value", expr("substring(value, 6, length(value)-5)")) \
    .select("value") \
    .select(from_avro(col("value"), sign_out_schema).alias("sign_out")) \
    .select(expr("CAST(sign_out.timestamp AS timestamp)").alias("timestamp"), lit(-1).alias("status")) \
    .withWatermark("timestamp", "1 hour") \


union = sign_in_stream.union(sign_out_stream)

grouped = union.groupBy(window("timestamp", '1 hour')
    ) \
    .agg(sum("status").alias("ActiveSessionCount"))


def write_to_snowflake(batch_df: DataFrame, batch_id):
    batch_df.write \
        .format("snowflake") \
        .options(**snowflake_conf) \
        .option("dbtable", 'active_sessions_count') \
        .mode("append") \
        .save()

query = grouped.writeStream \
    .foreachBatch(write_to_snowflake) \
    .outputMode("append") \
    .option("checkpointLocation", "checkPoints/ActiveSessionsCount") \
    .start()

query.awaitTermination()
