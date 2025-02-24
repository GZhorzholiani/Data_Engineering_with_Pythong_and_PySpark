from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import expr, col
from Configs.config import spark_consumer_conf, snowflake_conf
from spark_event_consuming.helper_functions import get_latest_schema

spark = SparkSession.builder \
    .appName("item_view_room3") \
    .getOrCreate()

subject = "item_view-value"

schema = get_latest_schema(subject)

topic = "item_view"

raw_stream = spark.readStream \
    .format("kafka") \
    .options(**spark_consumer_conf) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .withColumn("value", expr("substring(value, 6, length(value)-5)")) \
    .select("value")

item_view = raw_stream.select(from_avro(col("value"), schema).alias("item_view")) \
    .select(expr("CAST(item_view.timestamp AS timestamp)").alias("timestamp")
            ,col("item_view.event_name")
            ,col("item_view.item_id")
            ,col("item_view.user_id"))

def write_to_snowflake(batch_df: DataFrame, batch_id):
    batch_df.write \
        .format("snowflake") \
        .options(**snowflake_conf) \
        .option("dbtable", 'ITEM_VIEW') \
        .mode("append") \
        .save()

query = item_view.writeStream \
    .foreachBatch(write_to_snowflake) \
    .outputMode("append") \
    .option("checkpointLocation", "checkPoints/ItemViewToSnowflake") \
    .start()

query.awaitTermination()