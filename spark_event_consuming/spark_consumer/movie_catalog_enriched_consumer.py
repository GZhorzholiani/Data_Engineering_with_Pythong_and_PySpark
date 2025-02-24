from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from Configs.config import spark_consumer_conf, snowflake_conf

spark = SparkSession.builder \
    .appName("MovieConsumerToSnowflake") \
    .getOrCreate()

topic = 'movies_catalog_enriched'


schema = StructType([
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


raw_stream = spark.readStream \
    .format("kafka") \
    .options(**spark_consumer_conf) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(expr("CAST(value AS STRING)").alias("value"), schema).alias("json_value"))

movie_stream = raw_stream.selectExpr("json_value.*")

def write_to_snowflake(batch_df, batch_id):
    batch_df.write \
        .format("snowflake") \
        .options(**snowflake_conf) \
        .option("dbtable", 'movies_catalog_enriched') \
        .mode("append") \
        .save()

query = movie_stream.writeStream \
    .foreachBatch(write_to_snowflake) \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoints/MovieConsumerToSnowflakeCheckpoint") \
    .trigger(once=True) \
    .start()

query.awaitTermination()