# Databricks notebook source
import logging
import uuid
from datetime import datetime

import pyspark.sql.functions as sf
from pyspark.sql import Window
from delta.tables import DeltaTable

# COMMAND ----------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze

# COMMAND ----------

# =============================================================================
# BRONZE LAYER — Ingest raw data as-is, append only, with CDC metadata
# =============================================================================
# Bronze principles:
#   • No business transformations — preserve source fidelity
#   • Append-only to keep full history
#   • Add metadata columns for traceability (batch_id, ingestion_timestamp, source_file)
#   • Schema evolution enabled via mergeSchema

def ingest_bronze(source_file_path: str, target_table_name: str) -> None:
    """
    Ingest raw JSON files into a Bronze Delta table.

    Strategy: append-only with mergeSchema to survive source schema changes.
    We do NOT merge/deduplicate at Bronze — that is Silver's responsibility.
    Full raw history must be preserved for auditability and reprocessing.
    """
    logger.info(f"[BRONZE] Ingesting '{source_file_path}' → '{target_table_name}'")

    raw_df = (
        spark.read
        .format("csv")
        .option("inferSchema", "false")   # Keep all values as strings — true Bronze
        .option("header", "true")
        .load(source_file_path)
    )

    record_count = raw_df.count()
    logger.info(f"[BRONZE] Records read: {record_count}")

    # --- CDC metadata (computed once on driver) ---
    # Future enhancement: set batch_id as JobRunId or DagRunId to prevent duplicates
    # from rerunning the same job
    batch_id = str(uuid.uuid4())

    enriched_df = (
        raw_df
        .withColumn("_ingestion_timestamp", sf.current_timestamp())   # per-row server time
        .withColumn("_source_file", sf.lit(source_file_path))
        .withColumn("_batch_id", sf.lit(batch_id))
        # _is_deleted: set to True upstream when a source record is logically deleted.
        # Defaulting to False here for insert/update events.
        .withColumn("_is_deleted", sf.lit(False))
    )

    if spark.catalog.tableExists(target_table_name):
        logger.info(f"[BRONZE] Table exists — appending with schema evolution")
        (
            enriched_df
            .writeTo(target_table_name)
            .option("mergeSchema", "true")
            .append()
        )
    else:
        logger.info(f"[BRONZE] Table does not exist — creating")
        enriched_df.write.format("delta").saveAsTable(target_table_name)

    logger.info(f"[BRONZE] Done. batch_id={batch_id}")


# COMMAND ----------

ingest_bronze("/Volumes/workspace/lnd/raw_data/ecommerce_store/2019-Oct-trimmed.csv", "bronze.ecommerce_store")

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver

# COMMAND ----------

def process_data(df):
    return (
        df.select(
            sf.col("event_time").cast("timestamp"),
            sf.col("event_type").cast("string"),
            sf.col("product_id").cast("bigint"),
            sf.col("category_id").cast("bigint"),
            sf.col("category_code").cast("string"),
            sf.col("brand").cast("string"),
            sf.col("price").cast("double"),
            sf.col("user_id").cast("bigint"),
            sf.col("user_session").cast("string"),
            sf.col("_ingestion_timestamp").cast("timestamp"),
            sf.col("_source_file").cast("string"),
            sf.col("_batch_id").cast("string"),
            sf.col("_is_deleted").cast("boolean")
        )
        .filter(sf.col("user_id").isNotNull())
        .filter(sf.col("price").isNotNull())
        .filter(sf.col("price") > 0)
    )


def extract_brands(df):
    """
    Here we could enrich the brands table with additional information from other sources (Tax information, stocks price etc.)
    """
    df_brands = (
        df.select("brand", "_ingestion_timestamp", "_source_file", "_batch_id", "_is_deleted")
        .filter(sf.col("brand").isNotNull())
        .withColumn("brand_tier",
                    sf.when(sf.col("brand").isin("samsung", "apple", "huawei"), "premium")
                    .when(sf.col("brand").isin("xiaomi", "lg", "sony"), "mid")
                    .otherwise("other")
                    )
        )
    
    return df_brands

def extract_purchases(df):
    df_purchases = (
        df.filter(sf.col("event_type") == "purchase")
        .select("event_time", "product_id", "category_id", "category_code", "brand", "price", "user_id", "user_session", "_ingestion_timestamp", "_source_file", "_batch_id", "_is_deleted")
    )

    return df_purchases

def extract_views(df):
    df_views = (
        df.filter(sf.col("event_type") == "view")
        .select("event_time", "product_id", "category_id", "category_code", "brand", "price", "user_id", "user_session", "_ingestion_timestamp", "_source_file", "_batch_id", "_is_deleted")
    )

    return df_views

def extract_carts(df):
    df_carts = (
        df.filter(sf.col("event_type") == "cart")
        .select("event_time", "product_id", "category_id", "category_code", "brand", "price", "user_id", "user_session", "_ingestion_timestamp", "_source_file", "_batch_id", "_is_deleted")
    )

    return df_carts

def extract_users(df):
    """
    Here we could enrich the user table with additional information from other sources (e.g Demographics, Location etc.)
    """

    df_users = (
        df.select("user_id", "user_session", "_ingestion_timestamp", "_source_file", "_batch_id", "_is_deleted")
        .filter(sf.col("user_id").isNotNull())
    )

    return df_users


# COMMAND ----------

EXTRACT_FUNCTIONS = {
    "silver.ecommerce_store": process_data,
    "silver.ecommerce_brands": extract_brands,
    "silver.ecommerce_purchases": extract_purchases,
    "silver.ecommerce_views": extract_views,
    "silver.ecommerce_carts": extract_carts,
    "silver.ecommerce_users": extract_users
}

def _get_extract_fn(target_table_name: str):
    fn = EXTRACT_FUNCTIONS.get(target_table_name)
    if fn is None:
        raise ValueError(
            f"No extract function registered for '{target_table_name}'. "
            f"Register it in EXTRACT_FUNCTIONS."
        )
    return fn

# COMMAND ----------

def incremental_load(
    source_table_name: str,
    target_table_name: str,
    unique_keys: list[str],
    start_date: str = "9999-01-01",
    end_date: str = "9999-01-01",
) -> None:
    """
    Incrementally load Bronze → Silver using a Delta MERGE.

    Watermark strategy:
        - If Silver exists: filter Bronze for records newer than max(_ingestion_timestamp)
        - If Silver does not exist: full load (no filter)

    Deduplication: row_number() over unique_keys ordered by _ingestion_timestamp DESC
    keeps only the latest version of each business key within the batch.

    Soft deletes: rows with _is_deleted=True trigger a DELETE in the MERGE.
    """
    is_incremental = start_date == "9999-01-01" or end_date == "9999-01-01"

    source_table_exists = spark.catalog.tableExists(source_table_name)
    if not source_table_exists:
        logger.info(f"[SILVER]: {source_table_name} does not exist")
        raise Exception(f"Source table {source_table_name} does not exist")
    
    extract_function = _get_extract_fn(target_table_name)

    source_df = spark.read.table(source_table_name)
    target_table_exists = spark.catalog.tableExists(target_table_name)

    # ---- Determine incremental slice ----------------------------------------
    if target_table_exists and is_incremental:
        target_df = spark.read.table(target_table_name)
        max_ts = target_df.select(sf.max("_ingestion_timestamp")).first()[0]
        logger.info(f"[SILVER] Watermark for {target_table_name}: {max_ts}")
        incremental_df = source_df.filter(sf.col("_ingestion_timestamp") > max_ts)
    elif target_table_exists and not is_incremental:
        incremental_df = (
            source_df
            .filter(sf.col("_ingestion_timestamp") > start_date)
            .filter(sf.col("_ingestion_timestamp") < end_date)
        )
    else:
        # First run — full load
        logger.info(f"[SILVER] First run for {target_table_name} — full load")
        incremental_df = source_df

    raw_count = incremental_df.count()
    logger.info(f"[SILVER] Records in slice: {raw_count}")

    if raw_count == 0:
        logger.info(f"[SILVER] No new records for {target_table_name}. Skipping.")
        return

    # ---- Deduplication ----------------------------------
    dedup_keys = list(unique_keys)
    w = (
        Window
        .partitionBy(dedup_keys)
        .orderBy(sf.col("_ingestion_timestamp").desc())
    )
    deduped_df = (
        incremental_df
        .withColumn("_row_num", sf.row_number().over(w))
        .filter(sf.col("_row_num") == 1)
        .drop("_row_num")
    )

    deduped_count = deduped_df.count()
    logger.info(f"[SILVER] Records after dedup: {deduped_count}")

    extracted_df = extract_function(deduped_df)

    # ---- Separate deletes from upserts --------------------------------------
    deletes_df = extracted_df.filter(sf.col("_is_deleted") == True)
    upserts_df = extracted_df.filter(sf.col("_is_deleted") == False)

    # ---- Write to Silver ----------------------------------------------------
    merge_condition = " AND ".join(
        [f"target.{k} = source.{k}" for k in dedup_keys]
    )

    if target_table_exists:
        delta_table = DeltaTable.forName(spark, target_table_name)
        try:
            (
                delta_table.alias("target")
                .merge(extracted_df.alias("source"), merge_condition)
                .whenMatchedDelete(condition="source._is_deleted = true")
                .whenMatchedUpdateAll(condition="source._is_deleted = false")
                .whenNotMatchedInsertAll(condition="source._is_deleted = false")
                .execute()
            )
            logger.info(
                f"[SILVER] MERGE complete for {target_table_name}. "
                f"Upserts: {upserts_df.count()}, Deletes: {deletes_df.count()}"
            )
        except Exception as e:
            logger.error(f"[SILVER] MERGE failed for {target_table_name}: {e}")
            raise
    else:
        (
            upserts_df          # exclude deletes on first load (nothing to delete yet)
            .write
            .format("delta")
            .saveAsTable(target_table_name)
        )
        logger.info(f"[SILVER] Created {target_table_name} with {upserts_df.count()} records")
 


# COMMAND ----------

SILVER_CONFIG = [
    ("bronze.ecommerce_store",        "silver.ecommerce_store",        ["user_id", "user_session", "event_type", "event_time"]),  # IMPORTANT: THIS TABLE HAS TO BE PROCESSED FIRST
    ("silver.ecommerce_store",        "silver.ecommerce_brands",       ["brand"]),
    ("silver.ecommerce_store",        "silver.ecommerce_purchases",    ["user_id", "user_session", "event_time"]),
    ("silver.ecommerce_store",        "silver.ecommerce_views",        ["user_id", "user_session", "event_time"]),
    ("silver.ecommerce_store",        "silver.ecommerce_carts",        ["user_id", "user_session", "event_time"]),
    ("silver.ecommerce_store",        "silver.ecommerce_users",        ["user_id", "user_session"])
]

for src, tgt, keys in SILVER_CONFIG:
    incremental_load(src, tgt, keys)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold

# COMMAND ----------

def build_gold_layer():
    df_brands = spark.read.table("silver.ecommerce_brands")
    df_purchases = spark.read.table("silver.ecommerce_purchases")
    df_views = spark.read.table("silver.ecommerce_views")
    df_carts = spark.read.table("silver.ecommerce_carts")
    df_ecommerce_store = spark.read.table("silver.ecommerce_store")


    brands_agg = (
        df_purchases
        .groupBy("brand")
        .agg(
            sf.count("*").alias("num_purchases"),
            sf.sum("price").alias("total_revenue"),
        )
        .withColumn("average_check", sf.col("total_revenue") / sf.col("num_purchases"))
    )

    df_views_agg = (
        df_views
        .groupBy("brand")
        .agg(sf.count("*").alias("num_views"))
    )

    unique_metrics = df_ecommerce_store.groupBy("brand").agg(
        sf.countDistinct("user_id").alias("unique_users"),
        sf.countDistinct("product_id").alias("unique_products"),
        sf.countDistinct("category_id").alias("unique_categories"),
    )

    df_cart = df_carts.groupBy("brand").agg(
        sf.count("*").alias("num_carts")
    )

    gold_df = (
        brands_agg
        .join(df_views_agg, on="brand", how="left")
        .join(df_brands, on="brand", how="left")
        .join(unique_metrics, on="brand", how="left")
        .join(df_cart, on="brand", how="left")
        .withColumn("conversion_rate", sf.when(sf.col("num_carts") > 0,
                    sf.round(sf.col("num_purchases") / sf.col("num_carts") * 100, 2)
                    ).otherwise(sf.lit(None))
        )                   
        .withColumn("cart_abandon_rate", sf.when(sf.col("num_views") > 0,
                    sf.round(sf.col("num_carts") / sf.col("num_views") * 100, 2)).otherwise(sf.lit(None))
                    )
        .filter(sf.col("brand").isNotNull())
        .drop("_ingestion_timestamp")
        .drop("_batch_id")
        .drop("_source_file")
        .drop("_is_deleted")
    )

    target_table = "gold.brand_analysis"

    if spark.catalog.tableExists(target_table):
        delta_table = DeltaTable.forName(spark, target_table)
        try:
            (
                delta_table.alias("target")
                .merge(gold_df.alias("source"), "target.brand = source.brand")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info(f"[GOLD] MERGE complete for {target_table}")
        except Exception as e:
            logger.error(f"[GOLD] MERGE failed: {e}")
            raise
    else:
        gold_df.write.format("delta").saveAsTable(target_table)
        logger.info(f"[GOLD] Created {target_table}")


# COMMAND ----------

build_gold_layer()