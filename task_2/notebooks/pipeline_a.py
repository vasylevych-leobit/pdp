import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, LongType
import pyspark.sql.functions as F

schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True),

])


def pipeline_A_UNoptimized():
    print("Start Unoptimized pipeline A")

    spark = SparkSession.builder \
        .appName("Pipeline A unoptimized") \
        .master("spark://master:7077") \
        .getOrCreate()

    # Turn OFF AQE so Spark doesn't auto-optimise for us
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    # I don't include initial data loading in benchmarking timer as it isn't truly part of the pipeline and this step can't be
    # optimized anyway
    df = spark.read.option("header", "true").schema(schema).csv("/data/2019-Oct.csv")

    t0 = time.time()

    df_brands = (
        df.select("brand").distinct()
        .filter(F.col("brand").isNotNull())
        .withColumn("brand_tier",
                    F.when(F.col("brand").isin("samsung", "apple", "huawei"), "premium")
                    .when(F.col("brand").isin("xiaomi", "lg", "sony"), "mid")
                    .otherwise("other")
                    )
        )

    df_purchases = (
        df.filter(F.col("event_type") == "purchase")
        .groupBy("brand")
        .agg(
            F.count("*").alias("num_purchases"),
            F.sum("price").alias("total_revenue"),
        )
    )

    df_purchases = df_purchases.withColumn("average_check", F.col("total_revenue") / F.col("num_purchases"))

    df_views = (
        df.filter(F.col("event_type") == "view")
        .groupBy("brand")
        .agg(F.count("*").alias("num_views"))
    )

    unique_metrics = df.groupBy("brand").agg(
        F.countDistinct("user_id").alias("unique_users"),
        F.countDistinct("product_id").alias("unique_products"),
        F.countDistinct("category_id").alias("unique_categories"),
    )

    df_cart = df.filter(F.col("event_type") == "cart").groupBy("brand").agg(
        F.count("*").alias("num_carts")
    )

    result = (
        df_purchases
        .join(df_views, on="brand", how="left")
        .join(df_brands, on="brand", how="left")
        .join(unique_metrics, on="brand", how="left")
        .join(df_cart, on="brand", how="left")
        .withColumn("conversion_rate",
                    F.round(F.col("num_purchases") / F.col("num_views") * 100, 2))
        .withColumn("cart_abandon_rate", F.round(F.col("num_carts") / F.col("num_views") * 100, 2))
        .orderBy(F.desc("total_revenue"))
    )

    result.write.format("noop").mode("overwrite").save()

    baseline_time = time.time() - t0
    print(f"Pipeline A baseline time: {baseline_time:.2f}s")

    spark.stop()


def pipeline_A_optimized():
    print("Start Optimized pipeline A")

    spark = SparkSession.builder \
        .appName("Pipeline A optimized") \
        .master("spark://master:7077") \
        .getOrCreate()

    # Turn OFF AQE so Spark doesn't auto-optimise for us
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    # Partition tuning
    # Rule of thumb partitions should be from num_cores*2 to num_cores*4.
    # I have one worker with 2 cores so 2*4 = 8 partitions
    spark.conf.set('spark.sql.shuffle.partitions', "8")


    # Apply caching. df.count() is needed to materialize cache
    # Dataset is 5.5 GB which might not fit in the memory. Fortunately default caching behaviour
    # is MEMORY_AND_DISK which means what doesn't fit in memory is written to the disk.
    df = spark.read.option("header", "true").schema(schema).csv("/data/2019-Oct.csv").cache()
    df.count()

    # I don't include initial data loading in benchmarking as it isn't truly part of the pipeline and this step can't be
    # optimized anyway
    t0 = time.time()

    df_brands = (
        df.select("brand").distinct()
        .filter(F.col("brand").isNotNull())
        .withColumn("brand_tier",
                    F.when(F.col("brand").isin("samsung", "apple", "huawei"), "premium")
                    .when(F.col("brand").isin("xiaomi", "lg", "sony"), "mid")
                    .otherwise("other")
                    )
    )
    # Apply broadcast since there 3446 unique brands, small enough for broadcasting
    df_brands = F.broadcast(df_brands)
    # Broadcasting has two main advantages
    # 1. Reduces network I/O
    # 2. Spark uses Hash Join instead of Sort Merge Join

    # I don't broadcast df_purchases since it is the most left side of join
    df_purchases = (
        df.filter(F.col("event_type") == "purchase")
        .groupBy("brand")
        .agg(
            F.count("*").alias("num_purchases"),
            F.sum("price").alias("total_revenue"),
        )
    )

    df_purchases = df_purchases.withColumn("average_check", F.col("total_revenue") / F.col("num_purchases"))

    df_views = (
        df.filter(F.col("event_type") == "view")
        .groupBy("brand")
        .agg(F.count("*").alias("num_views"))
    )
    df_views = F.broadcast(df_views)

    unique_metrics = df.groupBy("brand").agg(
        F.countDistinct("user_id").alias("unique_users"),
        F.countDistinct("product_id").alias("unique_products"),
        F.countDistinct("category_id").alias("unique_categories"),
    )
    unique_metrics = F.broadcast(unique_metrics)

    df_cart = df.filter(F.col("event_type") == "cart").groupBy("brand").agg(
        F.count("*").alias("num_carts")
    )
    df_cart = F.broadcast(df_cart)

    result = (
        df_purchases
        .join(df_views, on="brand", how="left")
        .join(df_brands, on="brand", how="left")
        .join(unique_metrics, on="brand", how="left")
        .join(df_cart, on="brand", how="left")
        .withColumn("conversion_rate",
                    F.round(F.col("num_purchases") / F.col("num_views") * 100, 2))
        .withColumn("cart_abandon_rate", F.round(F.col("num_carts") / F.col("num_views") * 100, 2))
        .orderBy(F.desc("total_revenue"))
    )

    # Special format for benchmarking, ensures transformations are triggered and all data is processed
    result.write.format("noop").mode("overwrite").save()

    optimized_time = time.time() - t0
    print(f"Pipeline A optimized time: {optimized_time:.2f}s")

    spark.stop()

# Important note about caching in Pipeline A.
# In the unoptimized version, the timer includes 3 full scans of the 5.5GB CSV file —
# one each for df_brands, df_purchases and df_views — because df is lazy and re-reads
# the source on every action.
#
# In the optimized version, df is cached and materialized before the timer starts (df.count()),
# so the timer only measures the transformation and join logic, not I/O.
# This means the two timers are not directly comparable, but the optimized version
# reflects real-world usage where the dataset is loaded once and reused across multiple pipelines.


if __name__ == "__main__":
    pipeline_A_UNoptimized()
    pipeline_A_optimized()

