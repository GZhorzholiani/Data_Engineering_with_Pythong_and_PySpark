from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import expr, col
from Configs.config import spark_consumer_conf, snowflake_conf
from spark_event_consuming.helper_functions import get_latest_schema

spark = SparkSession.builder \
    .appName("add_to_cart_room3") \
    .getOrCreate()

subject = "add_to_cart-value"

schema = get_latest_schema(subject)

topic = 'add_to_cart'

raw_stream = spark.readStream \
    .format("kafka") \
    .options(**spark_consumer_conf) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .withColumn("value", expr("substring(value, 6, length(value)-5)")) \
    .select("value")


add_to_cart = raw_stream.select(from_avro(col("value"), schema).alias("add_to_cart")) \
    .select(expr("CAST(add_to_cart.timestamp AS timestamp)").alias("timestamp")
            ,col("add_to_cart.event_name")
            ,col("add_to_cart.item_id")
            ,col("add_to_cart.user_id")
            ,col("add_to_cart.cart_id"))


def write_to_snowflake(batch_df: DataFrame, batch_id):
    batch_df.write \
        .format("snowflake") \
        .options(**snowflake_conf) \
        .option("dbtable", 'ADD_TO_CART') \
        .mode("append") \
        .save()

query = add_to_cart.writeStream \
    .foreachBatch(write_to_snowflake) \
    .outputMode("append") \
    .option("checkpointLocation", "checkPoints/AddToCartToSnowflake") \
    .start()

query.awaitTermination()