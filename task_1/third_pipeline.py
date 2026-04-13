# Pipeline 3 - NYC Taxi 2024
# Features:
#  1. Auto Loader (cloudFiles) for incremental Parquet ingestion
#  2. Native SDP expectations with DQ quarantine pattern
#  3. AUTO CDC for Silver SCD Type 1
#  4. Materialized Views for Gold analytics layer

from pyspark import pipelines as dp
from pyspark.sql import functions as sf
from pyspark.sql.functions import broadcast
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)


RAW_VOLUME_PATH  = spark.conf.get("raw_volume_path", "/Volumes/workspace/lnd/raw_data/nyc_taxi/parquet")
ZONE_LOOKUP_PATH = f"/Volumes/workspace/lnd/raw_data/nyc_taxi/lookup/taxi_zone_lookup.csv"
CATALOG         = spark.conf.get("catalog", "workspace")

# BRONZE LAYER — Streaming table via Auto Loader

# Auto Loader (cloudFiles) tracks which files have been processed using an
# internal checkpoint — only new monthly files are ingested on each run.

@dp.table(
    name="bronze.nyc_taxi_raw",
    comment="Raw NYC Yellow Taxi trip records ingested via Auto Loader. Append-only. No transformations.",
)
def nyc_taxi_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{RAW_VOLUME_PATH}/_schema_checkpoint")
        .option("mergeSchema", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(RAW_VOLUME_PATH)
        .withColumn("_ingestion_timestamp", sf.current_timestamp())
        .withColumn("_source_file", sf.col("_metadata.file_path"))
        .withColumn("_batch_id", sf.lit(str(__import__("uuid").uuid4())))
    )


# SILVER LAYER — Quarantine pattern + AUTO CDC
# The quarantine pattern uses a two-flow approach:
#   1. A temporary staging table with @dp.expect_all applied (warn mode — all rows
#      written, but is_quarantined flag is computed for DQ-failing rows).
#   2. A permanent quarantine streaming table written via @dp.append_flow,
#      reading only the flagged rows.
#   3. A clean view of valid rows fed into AUTO CDC for dedup + SCD Type 1.
#
# AUTO CDC (dp.create_auto_cdc_flow) replaces manual DeltaTable.merge() —
# it handles ordering and deduplication.


# Data Quality rules dictionary
DQ_RULES = {
    "pickup_datetime_not_null":   "tpep_pickup_datetime IS NOT NULL",
    "dropoff_datetime_not_null":  "tpep_dropoff_datetime IS NOT NULL",
    "dropoff_after_pickup":       "tpep_dropoff_datetime > tpep_pickup_datetime",
    "trip_within_2024":           "tpep_pickup_datetime >= '2024-01-01' AND tpep_pickup_datetime < '2025-01-01'",
    "fare_amount_positive":       "CAST(fare_amount AS DOUBLE) > 0",
    "total_amount_positive":      "CAST(total_amount AS DOUBLE) > 0",
    "trip_distance_non_negative": "CAST(trip_distance AS DOUBLE) >= 0",
    "pu_location_valid":          "CAST(PULocationID AS INT) BETWEEN 1 AND 265",
    "do_location_valid":          "CAST(DOLocationID AS INT) BETWEEN 1 AND 265",
    "payment_type_valid":         "CAST(payment_type AS INT) BETWEEN 1 AND 6 OR payment_type IS NULL",
}

# Expression that is TRUE when a row FAILS at least one rule. If FAILS then quarantine it
_quarantine_expr = "NOT ({})".format(" AND ".join(f"({v})" for v in DQ_RULES.values()))


# Staging: apply all expectations, tag quarantine rows
# temporary=True means this table is scoped to the pipeline only
@dp.table(
    temporary=True,
    comment="Staging table: all Bronze rows with is_quarantined flag computed.",
    cluster_by=["is_quarantined"],
)
@dp.expect_all(DQ_RULES)
def nyc_taxi_staging():
    return (
        spark.readStream.table("bronze.nyc_taxi_raw")
        .withColumn("tpep_pickup_datetime",  sf.col("tpep_pickup_datetime").cast(TimestampType()))
        .withColumn("tpep_dropoff_datetime", sf.col("tpep_dropoff_datetime").cast(TimestampType()))
        .withColumn("passenger_count",       sf.col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance",         sf.col("trip_distance").cast(DoubleType()))
        .withColumn("PULocationID",          sf.col("PULocationID").cast(IntegerType()))
        .withColumn("DOLocationID",          sf.col("DOLocationID").cast(IntegerType()))
        .withColumn("RatecodeID",            sf.col("RatecodeID").cast(IntegerType()))
        .withColumn("payment_type",          sf.col("payment_type").cast(IntegerType()))
        .withColumn("fare_amount",           sf.col("fare_amount").cast(DoubleType()))
        .withColumn("extra",                 sf.col("extra").cast(DoubleType()))
        .withColumn("mta_tax",               sf.col("mta_tax").cast(DoubleType()))
        .withColumn("tip_amount",            sf.col("tip_amount").cast(DoubleType()))
        .withColumn("tolls_amount",          sf.col("tolls_amount").cast(DoubleType()))
        .withColumn("improvement_surcharge", sf.col("improvement_surcharge").cast(DoubleType()))
        .withColumn("total_amount",          sf.col("total_amount").cast(DoubleType()))
        .withColumn("congestion_surcharge",  sf.col("congestion_surcharge").cast(DoubleType()))
        .withColumn("Airport_fee",           sf.col("Airport_fee").cast(DoubleType()))
        # Compute DQ flag — True means this row failed at least one rule
        .withColumn("is_quarantined", sf.expr(_quarantine_expr))
        .withColumn("_dq_checked_at", sf.current_timestamp())
    )


# Quarantine sink: append-only, permanent, full rejection audit trail
dp.create_streaming_table(
    name="silver.nyc_taxi_quarantine",
    comment="Permanently stores all DQ-rejected trip records with is_quarantined=true"
)

@dp.append_flow(target="silver.nyc_taxi_quarantine")
def quarantine_flow():
    return (
        spark.readStream.table("nyc_taxi_staging")
        .filter("is_quarantined = true")
    )


# Clean view: only valid rows, feeds into AUTO CDC
@dp.view(
    comment="Valid (non-quarantined) trip records, ready for AUTO CDC upsert into Silver.",
)
def nyc_taxi_clean():
    return (
        spark.readStream.table("nyc_taxi_staging")
        .filter("is_quarantined = false")
        .drop("is_quarantined", "_dq_checked_at")
    )


# Step 4 — Silver target table definition (AUTO CDC will write into this)
dp.create_streaming_table(
    name="silver.nyc_taxi_trips",
    comment="Cleaned, deduplicated NYC Yellow Taxi trips. "
            "SCD Type 1 — latest record per composite business key wins."
)

# AUTO CDC: declarative SCD Type 1
# Replaces manual DeltaTable.merge() — handles ordering and dedup
#
# Business key: no single surrogate key exists in TLC data, so we compose
# one from the fields that uniquely identify a trip.
# ---------------------------------------------------------------------------
dp.create_auto_cdc_flow(
    target="silver.nyc_taxi_trips",
    source="nyc_taxi_clean",
    keys=[                           # composite business key
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
    ],
    sequence_by="_ingestion_timestamp",
    stored_as_scd_type=1,
    ignore_null_updates=True
)


# Zone lookup dimension - ingested once as a batch streaming table
# Small enough to broadcast in Gold joins
@dp.table(
    name="silver.taxi_zone_lookup",
    comment="NYC TLC taxi zone dimension: maps LocationID to Borough + Zone + service_zone.",
)
def taxi_zone_lookup():
    # Batch read - this is a small static CSV, not a stream
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"ZONE_LOOKUP_PATH")
        .select(
            sf.col("LocationID").cast(IntegerType()).alias("location_id"),
            sf.col("Borough").alias("borough"),
            sf.col("Zone").alias("zone"),
            sf.col("service_zone").alias("service_zone"),
        )
    )


# GOLD LAYER — Materialized Views

# Revenue and fare analysis by pickup borough
@dp.materialized_view(
    name="gold.revenue_by_borough",
    comment="Aggregated fare revenue, tip rate, and trip volume grouped by pickup borough. "
            "Refreshed incrementally when silver.nyc_taxi_trips changes.",
)
def revenue_by_borough():
    trips = spark.read.table("silver.nyc_taxi_trips")
    zones = spark.read.table("silver.taxi_zone_lookup")

    return (
        trips
        # Broadcast the small zone lookup to avoid a shuffle join
        .join(broadcast(zones.alias("pu_zone")), trips.PULocationID == sf.col("pu_zone.location_id"), "left")
        .groupBy(
            sf.col("pu_zone.borough").alias("pickup_borough"),
            sf.col("pu_zone.service_zone").alias("service_zone"),
        )
        .agg(
            sf.count("*").alias("total_trips"),
            sf.round(sf.sum("fare_amount"), 2).alias("total_fare_revenue"),
            sf.round(sf.avg("fare_amount"), 2).alias("avg_fare"),
            sf.round(sf.sum("tip_amount"), 2).alias("total_tips"),
            sf.round(sf.avg("tip_amount"), 2).alias("avg_tip"),
            sf.round(
                sf.sum("tip_amount") / sf.nullif(sf.sum("fare_amount"), sf.lit(0)) * 100, 2
            ).alias("tip_rate_pct"),
            sf.round(sf.sum("total_amount"), 2).alias("total_revenue"),
            sf.round(sf.avg("trip_distance"), 2).alias("avg_trip_distance_miles"),
            sf.round(sf.avg("passenger_count"), 1).alias("avg_passenger_count"),
            # Payment type breakdown
            sf.round(
                sf.sum(sf.when(sf.col("payment_type") == 1, 1).otherwise(0)) / sf.count("*") * 100, 1
            ).alias("credit_card_pct"),
        )
        .filter(sf.col("pickup_borough").isNotNull())
        .orderBy(sf.desc("total_revenue"))
    )


# Time-based patterns (hour of day, day of week)
@dp.materialized_view(
    name="gold.trip_time_patterns",
    comment="Trip volume and revenue broken down by hour-of-day and day-of-week. "
            "Useful for demand forecasting and surge pricing analysis.",
)
def trip_time_patterns():
    return (
        spark.read.table("silver.nyc_taxi_trips")
        .withColumn("pickup_hour",       sf.hour("tpep_pickup_datetime"))
        .withColumn("pickup_dayofweek",  sf.dayofweek("tpep_pickup_datetime"))   # 1=Sunday
        .withColumn("pickup_dayname",    sf.date_format("tpep_pickup_datetime", "EEEE"))
        .withColumn("pickup_month",      sf.month("tpep_pickup_datetime"))
        .withColumn(
            "trip_duration_minutes",
            sf.round(
                (sf.unix_timestamp("tpep_dropoff_datetime") - sf.unix_timestamp("tpep_pickup_datetime")) / 60, 1
            ),
        )
        .groupBy("pickup_hour", "pickup_dayofweek", "pickup_dayname", "pickup_month")
        .agg(
            sf.count("*").alias("total_trips"),
            sf.round(sf.avg("total_amount"), 2).alias("avg_total_amount"),
            sf.round(sf.avg("trip_duration_minutes"), 1).alias("avg_duration_minutes"),
            sf.round(sf.avg("trip_distance"), 2).alias("avg_distance_miles"),
            sf.round(sf.avg("passenger_count"), 1).alias("avg_passengers"),
            sf.sum(sf.when(sf.col("RatecodeID").isin(2, 3), 1).otherwise(0)).alias("airport_trips"),
        )
        .orderBy("pickup_month", "pickup_dayofweek", "pickup_hour")
    )


# Comprehensive trip summary
@dp.materialized_view(
    name="gold.trip_summary",
    comment="Comprehensive Gold table: route-level aggregation (pickup to dropoff zone pair) "
            "enriched with borough and zone names. Primary analytical table for dashboards.",
)
def trip_summary():
    trips = spark.read.table("silver.nyc_taxi_trips")
    zones = spark.read.table("silver.taxi_zone_lookup")

    pu_zones = zones.select(
        sf.col("location_id").alias("pu_loc_id"),
        sf.col("borough").alias("pickup_borough"),
        sf.col("zone").alias("pickup_zone"),
    )
    do_zones = zones.select(
        sf.col("location_id").alias("do_loc_id"),
        sf.col("borough").alias("dropoff_borough"),
        sf.col("zone").alias("dropoff_zone"),
    )

    return (
        trips
        .join(broadcast(pu_zones), trips.PULocationID == sf.col("pu_loc_id"), "left")
        .join(broadcast(do_zones), trips.DOLocationID == sf.col("do_loc_id"), "left")
        .withColumn(
            "trip_duration_minutes",
            sf.round(
                (sf.unix_timestamp("tpep_dropoff_datetime") - sf.unix_timestamp("tpep_pickup_datetime")) / 60, 1,
            ),
        )
        .withColumn("pickup_hour",      sf.hour("tpep_pickup_datetime"))
        .withColumn("pickup_dayname",   sf.date_format("tpep_pickup_datetime", "EEEE"))
        .withColumn("pickup_month",     sf.month("tpep_pickup_datetime"))
        .withColumn("is_airport_trip",  sf.col("RatecodeID").isin(2, 3))
        .withColumn(
            "payment_type_label",
            sf.when(sf.col("payment_type") == 1, "Credit Card")
            .when(sf.col("payment_type") == 2, "Cash")
            .when(sf.col("payment_type") == 3, "No Charge")
            .when(sf.col("payment_type") == 4, "Dispute")
            .otherwise("Unknown"),
        )
        .groupBy(
            "pickup_borough", "pickup_zone",
            "dropoff_borough", "dropoff_zone",
            "pickup_month", "pickup_hour", "pickup_dayname",
            "is_airport_trip", "payment_type_label",
        )
        .agg(
            sf.count("*").alias("total_trips"),
            sf.round(sf.sum("fare_amount"),  2).alias("total_fare"),
            sf.round(sf.avg("fare_amount"),  2).alias("avg_fare"),
            sf.round(sf.sum("tip_amount"),   2).alias("total_tips"),
            sf.round(sf.avg("tip_amount"),   2).alias("avg_tip"),
            sf.round(sf.sum("total_amount"), 2).alias("total_revenue"),
            sf.round(sf.avg("total_amount"), 2).alias("avg_total_amount"),
            sf.round(sf.avg("trip_distance"), 2).alias("avg_distance_miles"),
            sf.round(sf.avg("trip_duration_minutes"), 1).alias("avg_duration_minutes"),
            sf.round(sf.sum("congestion_surcharge"), 2).alias("total_congestion_surcharge"),
            sf.round(
                sf.sum("fare_amount") / sf.nullif(sf.sum("trip_distance"), sf.lit(0)), 2
            ).alias("revenue_per_mile"),
        )
        .filter(sf.col("pickup_borough").isNotNull())
        .filter(sf.col("total_trips") >= 5)
    )
