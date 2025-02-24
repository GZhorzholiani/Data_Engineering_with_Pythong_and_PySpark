from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import expr, col, from_json, window, sum, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType

from Configs.config import spark_consumer_conf, snowflake_conf
from spark_event_consuming.helper_functions import get_latest_schema

spark = SparkSession.builder \
    .appName("movie_sales_aggregated") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", str(spark.sparkContext.defaultParallelism))

subject_add_to_cart = "add_to_cart-value"

subject_check_out = "check_out-value"

add_to_cart_schema = get_latest_schema(subject_add_to_cart)
check_out_schema = get_latest_schema(subject_check_out)

movie_schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("Actor", StringType(), True),
    StructField("AspectRatio", StringType(), True),
    StructField("AudienceRating", StringType(), True),
    StructField("Binding", StringType(), True),
    StructField("Director", StringType(), True),
    StructField("EAN", StringType(), True),
    StructField("EANList", StringType(), True),
    StructField("Format", StringType(), True),
    StructField("IsEligibleForTradeIn", StringType(), True),
    StructField("ItemDimensions", StringType(), True),
    StructField("Label", StringType(), True),
    StructField("Languages", StringType(), True),
    StructField("ListPrice", StringType(), True),
    StructField("Manufacturer", StringType(), True),
    StructField("NumberOfDiscs", StringType(), True),
    StructField("NumberOfItems", StringType(), True),
    StructField("PackageDimensions", StringType(), True),
    StructField("PackageQuantity", StringType(), True),
    StructField("ProductGroup", StringType(), True),
    StructField("ProductTypeName", StringType(), True),
    StructField("Publisher", StringType(), True),
    StructField("RegionCode", StringType(), True),
    StructField("ReleaseDate", StringType(), True),
    StructField("RunningTime", StringType(), True),
    StructField("Studio", StringType(), True),
    StructField("Title", StringType(), True),
    StructField("TradeInValue", StringType(), True),
    StructField("UPC", StringType(), True),
    StructField("UPCList", StringType(), True),
    StructField("Creator", StringType(), True),
    StructField("Genre", StringType(), True),
    StructField("PictureFormat", StringType(), True),
    StructField("Brand", StringType(), True),
    StructField("CatalogNumberList", StringType(), True),
    StructField("Color", StringType(), True),
    StructField("Edition", StringType(), True),
    StructField("IsAutographed", StringType(), True),
    StructField("IsMemorabilia", StringType(), True),
    StructField("MPN", StringType(), True),
    StructField("PartNumber", StringType(), True),
    StructField("EpisodeSequence", StringType(), True),
    StructField("PublicationDate", StringType(), True),
    StructField("Artist", StringType(), True),
    StructField("Feature", StringType(), True),
    StructField("IsAdultProduct", StringType(), True),
    StructField("ISBN", StringType(), True),
    StructField("Model", StringType(), True),
    StructField("OperatingSystem", StringType(), True),
    StructField("HardwarePlatform", StringType(), True),
    StructField("TrackSequence", StringType(), True),
    StructField("Author", StringType(), True),
    StructField("ManufacturerMaximumAge", StringType(), True),
    StructField("ManufacturerMinimumAge", StringType(), True),
    StructField("SKU", StringType(), True),
    StructField("Warranty", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("NumberOfPages", StringType(), True),
    StructField("Size", StringType(), True),
    StructField("ESRBAgeRating", StringType(), True),
    StructField("Platform", StringType(), True),
    StructField("LegalDisclaimer", StringType(), True),
    StructField("EISBN", StringType(), True),
    StructField("CEROAgeRating", StringType(), True),
    StructField("ClothingSize", StringType(), True),
    StructField("ItemPartNumber", StringType(), True),
    StructField("SubscriptionLength", StringType(), True),
    StructField("MediaType", StringType(), True),
    StructField("ManufacturerPartsWarrantyDescription", StringType(), True),
    StructField("HazardousMaterialType", StringType(), True),
    StructField("SeikodoProductCode", StringType(), True),
    StructField("ProductTypeSubcategory", StringType(), True),
    StructField("IssuesPerYear", StringType(), True),
    StructField("MagazineType", StringType(), True),
    StructField("NumberOfIssues", StringType(), True)
])

topic_add_to_cart = 'add_to_cart'

topic_check_out = 'check_out'

topic_movies = 'movies_catalog_enriched'


movie_stream = spark.readStream \
    .format("kafka") \
    .options(**spark_consumer_conf) \
    .option("subscribe", topic_movies) \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(expr("CAST(value AS STRING)").alias("value"), movie_schema).alias("json_value")) \
    .select("json_value.item_id", "json_value.ListPrice") \
    .withColumn("timestamp", current_timestamp()) \
    .withWatermark("timestamp", "1 hour") #delayThreshhold '1 day'

check_out_stream = spark.readStream \
    .format("kafka") \
    .options(**spark_consumer_conf) \
    .option("subscribe", topic_check_out) \
    .option("startingOffsets", "earliest") \
    .load() \
    .withColumn("value", expr("substring(value, 6, length(value)-5)")) \
    .select("value") \
    .select(from_avro(col("value"), check_out_schema).alias("check_out")) \
    .select(expr("CAST(check_out.timestamp AS timestamp)").alias("timestamp")
            ,col("check_out.cart_id")) \
    .withWatermark("timestamp", "1 hour") #delayThreshhold '1 day'


add_to_cart_stream = spark.readStream \
    .format("kafka") \
    .options(**spark_consumer_conf) \
    .option("subscribe", topic_add_to_cart) \
    .option("startingOffsets", "earliest") \
    .load() \
    .withColumn("value", expr("substring(value, 6, length(value)-5)")) \
    .select("value") \
    .select(from_avro(col("value"), add_to_cart_schema).alias("add_to_cart")) \
    .select(expr("CAST(add_to_cart.timestamp AS timestamp)").alias("timestamp")
            ,col("add_to_cart.item_id")
            ,col("add_to_cart.cart_id")) \
    .withWatermark("timestamp", "1 hour") #delayThreshhold '1 day'


add_to_cart_movie = add_to_cart_stream \
    .join(movie_stream, "item_id") \
    .select(add_to_cart_stream.item_id,
            "cart_id",
            expr("substring(regexp_replace(ListPrice, ',', ''), 2, length(ListPrice))").cast(FloatType()).alias("ListPrice"))

joined = add_to_cart_movie \
    .join(check_out_stream, "cart_id") \
    .select("item_id",
            "timestamp",
            "ListPrice")


grouped = joined \
    .groupBy(
    "item_id",
    window("timestamp", '1 hour') #windowDuration '1 day'
    ) \
    .agg(
    sum("ListPrice").alias("purchase_amount")
)

final = grouped \
    .select("item_id",
            "purchase_amount",
            current_timestamp().alias("generate_date"))

def write_to_snowflake(batch_df: DataFrame, batch_id):
    batch_df.write \
        .format("snowflake") \
        .options(**snowflake_conf) \
        .option("dbtable", 'movie_sales_aggregated') \
        .mode("append") \
        .save()

query = final.writeStream \
    .foreachBatch(write_to_snowflake) \
    .outputMode("append") \
    .option("checkpointLocation", "checkPoints/MovieSalesAggregated") \
    .start()

query.awaitTermination()
