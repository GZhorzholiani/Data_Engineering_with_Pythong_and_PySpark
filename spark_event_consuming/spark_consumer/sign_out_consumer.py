from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import expr, col
from Configs.config import spark_consumer_conf, snowflake_conf
from spark_event_consuming.helper_functions import get_latest_schema

spark = SparkSession.builder \
    .appName("sign_out_room3") \
    .getOrCreate()

subject = "sign_out-value"

schema = get_latest_schema(subject)

topic = "sign_out"

raw_stream = spark.readStream \
    .format("kafka") \
    .options(**spark_consumer_conf) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load() \
    .withColumn("value", expr("substring(value, 6, length(value)-5)")) \
    .select("value")

sign_out = raw_stream.select(from_avro(col("value"), schema).alias("sign_out")) \
    .select(expr("CAST(sign_out.timestamp AS timestamp)").alias("timestamp")
            ,col("sign_out.event_name")
            ,col("sign_out.user_id"))

def write_to_snowflake(batch_df: DataFrame, batch_id):
    batch_df.write \
        .format("snowflake") \
        .options(**snowflake_conf) \
        .option("dbtable", 'SIGN_OUT') \
        .mode("append") \
        .save()

query = sign_out.writeStream \
    .foreachBatch(write_to_snowflake) \
    .outputMode("append") \
    .option("checkpointLocation", "checkPoints/SignOutToSnowflake") \
    .start()

query.awaitTermination()