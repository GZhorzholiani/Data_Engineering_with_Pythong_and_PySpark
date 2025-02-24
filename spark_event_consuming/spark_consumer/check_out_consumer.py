from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import expr, col
from Configs.config import spark_consumer_conf, snowflake_conf
from spark_event_consuming.helper_functions import get_latest_schema

spark = SparkSession.builder \
    .appName("check_out_room3") \
    .getOrCreate()

subject = "check_out-value"

schema = get_latest_schema(subject)

topic = 'check_out'

raw_stream = spark.readStream \
    .format("kafka") \
    .options(**spark_consumer_conf) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .withColumn("value", expr("substring(value, 6, length(value)-5)")) \
    .select("value")


check_out = raw_stream.select(from_avro(col("value"), schema).alias("check_out")) \
    .select(expr("CAST(check_out.timestamp AS timestamp)").alias("timestamp")
            ,col("check_out.event_name")
            ,col("check_out.user_id")
            ,col("check_out.cart_id")
            ,col("check_out.payment_method"))

def write_to_snowflake(batch_df: DataFrame, batch_id):
    batch_df.write \
        .format("snowflake") \
        .options(**snowflake_conf) \
        .option("dbtable", 'CHECK_OUT') \
        .mode("append") \
        .save()

query = check_out.writeStream \
    .foreachBatch(write_to_snowflake) \
    .outputMode("append") \
    .option("checkpointLocation", "checkPoints/CheckOutToSnowflake") \
    .start()



query.awaitTermination()