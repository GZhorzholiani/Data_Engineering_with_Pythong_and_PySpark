from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import expr, col
from Configs.config import spark_consumer_conf, snowflake_conf
from spark_event_consuming.helper_functions import get_latest_schema

spark = SparkSession.builder \
    .appName("user_registration_room3") \
    .getOrCreate()

subject = "user_registration-value"

schema = get_latest_schema(subject)

topic = 'user_registration'

raw_stream = spark.readStream \
    .format("kafka") \
    .options(**spark_consumer_conf) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load() \
    .withColumn("value", expr("substring(value, 6, length(value)-5)")) \
    .select("value")

user_registration = raw_stream.select(from_avro(col("value"), schema).alias("user_registration")) \
    .select(expr("CAST(user_registration.timestamp AS timestamp)").alias("timestamp")
            ,col("user_registration.event_name")
            ,col("user_registration.user_id")
            ,col("user_registration.age")
            ,col("user_registration.masked_email")
            ,col("user_registration.preferred_language"))

def write_to_snowflake(batch_df: DataFrame, batch_id):
    batch_df.write \
        .format("snowflake") \
        .options(**snowflake_conf) \
        .option("dbtable", 'USER_REGISTRATION') \
        .mode("append") \
        .save()

query = user_registration.writeStream \
    .foreachBatch(write_to_snowflake) \
    .outputMode("append") \
    .option("checkpointLocation", "checkPoints/UserRegistrationToSnowflake") \
    .start()

query.awaitTermination()
