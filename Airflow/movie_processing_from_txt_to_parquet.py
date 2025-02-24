from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import split, col, explode, collect_list
from pyspark.sql.functions import sum, when, expr, instr
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

column_names = [ 'Actor', 'Artist', 'AspectRatio', 'AudienceRating', 'Author', 'Binding', 'Brand',
                'CEROAgeRating', 'CatalogNumberList', 'ClothingSize', 'Color', 'Creator', 'Department',
                'Director', 'EAN', 'EANList', 'EISBN', 'ESRBAgeRating', 'Edition', 'EpisodeSequence',
                'Feature', 'Format', 'Genre', 'HardwarePlatform', 'HazardousMaterialType', 'ISBN', 'IsAdultProduct',
                'IsAutographed', 'IsEligibleForTradeIn', 'IsMemorabilia', 'IssuesPerYear', 'ItemDimensions',
                'ItemPartNumber', 'Label', 'Languages', 'LegalDisclaimer', 'MPN', 'MagazineType', 'Manufacturer',
                'ManufacturerMaximumAge', 'ManufacturerMinimumAge', 'ManufacturerPartsWarrantyDescription',
                'MediaType', 'Model', 'NumberOfDiscs', 'NumberOfIssues', 'NumberOfItems', 'NumberOfPages',
                'OperatingSystem', 'PackageDimensions', 'PackageQuantity', 'PartNumber', 'PictureFormat',
                'Platform', 'ProductGroup', 'ProductTypeName', 'ProductTypeSubcategory', 'PublicationDate',
                'Publisher', 'RegionCode', 'ReleaseDate', 'RunningTime', 'SKU', 'SeikodoProductCode', 'Size',
                'Studio', 'SubscriptionLength', 'Title', 'TrackSequence', 'UPC', 'UPCList', 'Warranty', 'ListPrice',
                'TradeInValue']


columns = map(lambda x: expr(f"CASE WHEN size({x}) = 0 THEN NULL ELSE {x} END").alias(x), column_names)

columns_1 = map(lambda x: expr(f"array_join({x}, ',')").alias(x), column_names)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Movie_to_parquet_room3").getOrCreate()

# Load raw text file
raw_data = spark.read.text("s3a://simplify-development/Movies.txt")

df = raw_data.withColumn("id", monotonically_increasing_id())

windowSpec = Window.orderBy("id")
df = df.withColumn("item_id", sum(when(col("value").startswith("ITEM"), 1).otherwise(0)).over(windowSpec))

df = df.filter(~col("value").startswith("ITEM"))

df = df.withColumn("key_value", split(col("value"), "="))

df = df.withColumn("key", col("key_value")[0]).withColumn("value", col("key_value")[1]).drop("key_value")

df = df.groupBy("item_id", "key").agg(collect_list("value").alias("values"))

df = df.withColumn("value", explode(col("values"))).drop("values")

df_final = df.groupBy("item_id").pivot("key").agg(collect_list("value"))

df_final = df_final.select(col("item_id").cast(StringType()), *columns)

df_final = df_final.select(col("item_id"), *columns_1) \
    .withColumn("dollar_index_List_price", instr("ListPrice", "$")) \
    .withColumn("dollar_index_Trade_in_value", instr("TradeInValue", "$")) \
    .withColumn("ListPriceNew", expr("substring(ListPrice, dollar_index_List_price, length(ListPrice))")) \
    .withColumn("TradeInValueNew", expr("substring(TradeInValue, dollar_index_Trade_in_value, length(TradeInValue))")) \
    .drop("ListPrice") \
    .drop("TradeInValue") \
    .drop("dollar_index_List_price") \
    .drop("dollar_index_Trade_in_value") \
    .withColumnRenamed("ListPriceNew", "ListPrice") \
    .withColumnRenamed("TradeInValueNew", "TradeInValue")

df_final.write.parquet("s3a://simplify-development/nika_parquet", mode="overwrite")
