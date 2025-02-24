from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import expr, col
from Configs.config import spark_consumer_conf, snowflake_conf
from spark_event_consuming.helper_functions import get_latest_schema

spark = SparkSession.builder \
    .appName("sign_in_room3") \
    .getOrCreate()

subject = "sign_in-value"

schema = get_latest_schema(subject)

topic = 'sign_in'

raw_stream = spark.readStream \
    .format("kafka") \
    .options(**spark_consumer_conf) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load() \
    .withColumn("value", expr("substring(value, 6, length(value)-5)")) \
    .select("value")

sign_in = raw_stream.select(from_avro(col("value"), schema).alias("sign_in")) \
    .select(expr("CAST(sign_in.timestamp AS timestamp)").alias("timestamp")
            ,col("sign_in.event_name")
            ,col("sign_in.user_id"))

def write_to_snowflake(batch_df: DataFrame, batch_id):
    batch_df.write \
        .format("snowflake") \
        .options(**snowflake_conf) \
        .option("dbtable", 'SIGN_IN') \
        .mode("append") \
        .save()

query = sign_in.writeStream \
    .foreachBatch(write_to_snowflake) \
    .outputMode("append") \
    .option("checkpointLocation", "checkPoints/SignInToSnowflake") \
    .start()

query.awaitTermination()
