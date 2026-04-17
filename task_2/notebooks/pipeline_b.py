import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def pipeline_B_unoptimized():
    print("Start Unoptimized pipeline B")

    spark = SparkSession.builder \
        .appName("Pipeline B unoptimized") \
        .master("spark://master:7077") \
        .getOrCreate()

    spark.conf.set("spark.sql.adaptive.enabled", "false")

    # parquet reads are much faster than csv
    df = spark.read.parquet("/data/taxi_raw/yellow_tripdata_202*.parquet")

    df_zones = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/data/taxi_zone_lookup.csv")

    t0 = time.time()

    # Trip stats by pickup location
    df_pickup_stats = (
        df.groupBy("PULocationID")
        .agg(
            F.count("*").alias("num_trips"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("total_amount").alias("avg_fare"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("tip_amount").alias("avg_tip"),
        )
    )

    # Trip stats by dropoff location
    df_dropoff_stats = (
        df.groupBy("DOLocationID")
        .agg(
            F.count("*").alias("num_dropoffs"),
            F.avg("total_amount").alias("avg_dropoff_fare"),
        )
    )

    # Payment type breakdown by pickup location
    df_payment_stats = (
        df.groupBy("PULocationID", "payment_type")
        .agg(
            F.count("*").alias("num_trips_by_payment"),
            F.sum("total_amount").alias("revenue_by_payment"),
        )
    )

    # Hourly trip volume
    df_hourly = (
        df.withColumn("hour", F.hour("tpep_pickup_datetime"))
        .groupBy("hour")
        .agg(
            F.count("*").alias("num_trips"),
            F.avg("total_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
        )
        .orderBy("hour")
    )

    # Vendor performance
    df_vendor_stats = (
        df.groupBy("VendorID")
        .agg(
            F.count("*").alias("num_trips"),
            F.avg("total_amount").alias("avg_fare"),
            F.avg("tip_amount").alias("avg_tip"),
            F.avg("trip_distance").alias("avg_distance"),
        )
    )

    result_pickup = (
        df_pickup_stats
        .join(df_zones, df_pickup_stats["PULocationID"] == df_zones["LocationID"], how="left")
        .drop("LocationID")
        .orderBy(F.desc("total_revenue"))
    )

    result_dropoff = (
        df_dropoff_stats
        .join(df_zones, df_dropoff_stats["DOLocationID"] == df_zones["LocationID"], how="left")
        .drop("LocationID")
        .orderBy(F.desc("num_dropoffs"))
    )

    result_payment = (
        df_payment_stats
        .join(df_zones, df_payment_stats["PULocationID"] == df_zones["LocationID"], how="left")
        .drop("LocationID")
        .orderBy(F.desc("revenue_by_payment"))
    )

    result_pickup.write.format("noop").mode("overwrite").save()
    result_dropoff.write.format("noop").mode("overwrite").save()
    result_payment.write.format("noop").mode("overwrite").save()
    df_hourly.write.format("noop").mode("overwrite").save()
    df_vendor_stats.write.format("noop").mode("overwrite").save()

    t1 = time.time()
    print(f"Pipeline B unoptimized time: {t1 - t0:.2f}s")

    spark.stop()


def pipeline_B_optimized():
    print("Start Optimized pipeline B")

    spark = SparkSession.builder \
        .appName("Pipeline B optimized") \
        .master("spark://master:7077") \
        .getOrCreate()

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    df = spark.read.parquet("/data/taxi_raw/yellow_tripdata_202*.parquet")


    df_zones = F.broadcast(
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/data/taxi_zone_lookup.csv")
    )

    t0 = time.time()

    df_pickup_stats = (
        df.groupBy("PULocationID")
        .agg(
            F.count("*").alias("num_trips"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("total_amount").alias("avg_fare"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("tip_amount").alias("avg_tip"),
        )
    )

    df_dropoff_stats = (
        df.groupBy("DOLocationID")
        .agg(
            F.count("*").alias("num_dropoffs"),
            F.avg("total_amount").alias("avg_dropoff_fare"),
        )
    )

    df_payment_stats = (
        df.groupBy("PULocationID", "payment_type")
        .agg(
            F.count("*").alias("num_trips_by_payment"),
            F.sum("total_amount").alias("revenue_by_payment"),
        )
    )

    df_hourly = (
        df.withColumn("hour", F.hour("tpep_pickup_datetime"))
        .groupBy("hour")
        .agg(
            F.count("*").alias("num_trips"),
            F.avg("total_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
        )
        .orderBy("hour")
    )

    df_vendor_stats = (
        df.groupBy("VendorID")
        .agg(
            F.count("*").alias("num_trips"),
            F.avg("total_amount").alias("avg_fare"),
            F.avg("tip_amount").alias("avg_tip"),
            F.avg("trip_distance").alias("avg_distance"),
        )
    )

    result_pickup = (
        df_pickup_stats
        .join(df_zones, df_pickup_stats["PULocationID"] == df_zones["LocationID"], how="left")
        .drop("LocationID")
        .orderBy(F.desc("total_revenue"))
    )

    result_dropoff = (
        df_dropoff_stats
        .join(df_zones, df_dropoff_stats["DOLocationID"] == df_zones["LocationID"], how="left")
        .drop("LocationID")
        .orderBy(F.desc("num_dropoffs"))
    )

    result_payment = (
        df_payment_stats
        .join(df_zones, df_payment_stats["PULocationID"] == df_zones["LocationID"], how="left")
        .drop("LocationID")
        .orderBy(F.desc("revenue_by_payment"))
    )

    result_pickup.write.format("noop").mode("overwrite").save()
    result_dropoff.write.format("noop").mode("overwrite").save()
    result_payment.write.format("noop").mode("overwrite").save()
    df_hourly.write.format("noop").mode("overwrite").save()
    df_vendor_stats.write.format("noop").mode("overwrite").save()

    t1 = time.time()
    print(f"Pipeline B optimized time: {t1 - t0:.2f}s")

    spark.stop()

if __name__ == "__main__":
    pipeline_B_unoptimized()
    pipeline_B_optimized()