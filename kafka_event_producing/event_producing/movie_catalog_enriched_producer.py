from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, to_json
from Configs.config import spark_consumer_conf

spark = SparkSession.builder \
    .appName("Movies_producer") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()


movie_stream = spark.readStream \
    .parquet("s3a://simplify-development/nika_parquet") \
    .select(to_json(expr("struct(*)")).alias("value"),
            expr("CAST(item_id AS STRING)").alias("key")) \


query = movie_stream.writeStream \
    .format("kafka") \
    .options(**spark_consumer_conf) \
    .option("topic", "movies_catalog_enriched") \
    .option("checkpointLocation", "checkPoints/MovieProduceToKafkaCheckpoint") \
    .trigger(once=True) \
    .start()

query.awaitTermination()
