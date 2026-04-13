# Databricks notebook source
import logging
import uuid
from datetime import datetime

import pyspark.sql.functions as sf
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import StringType, StructField, StructType


# COMMAND ----------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# COMMAND ----------

CATALOG = "workspace"
LND_SCHEMA = "lnd"
VOLUME_FOLDER = f"/Volumes/{CATALOG}/{LND_SCHEMA}/raw_data"

# Source to volume folder mappings for raw file ingestion
SOURCE_VOLUME_TABLES = {
    "customers": {
        "source": "samples.bakehouse.sales_customers",
        "target_bronze": "bronze.sales_customers",
        "unique_keys": ["customerID"],
    },
    "franchises": {
        "source": "samples.bakehouse.sales_franchises",
        "target_bronze": "bronze.sales_franchises",
        "unique_keys": ["franchiseID"],
    },
    "suppliers": {
        "source": "samples.bakehouse.sales_suppliers",
        "target_bronze": "bronze.sales_suppliers",
        "unique_keys": ["supplierID"],
    },
    "transactions": {
        "source": "samples.bakehouse.sales_transactions",
        "target_bronze": "bronze.sales_transactions",
        "unique_keys": ["transactionID"],
    },
}

# COMMAND ----------


# Simulate source - dump catalog tables to raw volume files
# In a real pipeline this step does not exist — raw files arrive from the
# source system. Here we simulate that by
# exporting catalog tables to JSON files in a volume.

def save_data_to_volume(source_table_name: str, folder_name: str) -> None:
    """Export a catalog table to a JSON volume."""
    partition_folder = datetime.now().strftime("%Y-%m-%d_%H")
    output_path = f"{VOLUME_FOLDER}/{folder_name}/{partition_folder}"
    logger.info(f"Saving '{source_table_name}' → '{output_path}'")
    (
        spark.read.table(source_table_name)
        .write.format("json")
        .mode("overwrite")
        .save(output_path)
    )


# COMMAND ----------

for folder_name, config in SOURCE_VOLUME_TABLES.items():
    save_data_to_volume(config["source"], folder_name)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze

# COMMAND ----------

# BRONZE LAYER — Ingest raw data as-is, append only, with CDC metadata
# Bronze principles:
# 1. No business transformations — preserve source fidelity, no deduplication
# 2. Append-only to keep full history
# 3. Add metadata columns for traceability (batch_id, ingestion_timestamp, source_file)
# 4. Schema evolution enabled via mergeSchema

def ingest_bronze(source_file_path: str, target_table_name: str) -> None:
    """
    Ingest raw JSON files into a Bronze Delta table.
    """
    logger.info(f"[BRONZE] Ingesting '{source_file_path}' → '{target_table_name}'")

    raw_df = (
        spark.read
        .format("json")
        .option("inferSchema", "false")   # Keep all values as strings
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
        .withColumn("_ingestion_timestamp", sf.current_timestamp())
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

# Ingest all sources into Bronze
partition_folder = datetime.now().strftime("%Y-%m-%d_%H")

for folder_name, config in SOURCE_VOLUME_TABLES.items():
    source_path = f"{VOLUME_FOLDER}/{folder_name}/{partition_folder}"
    ingest_bronze(source_path, config["target_bronze"])

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver

# COMMAND ----------

# SILVER LAYER — Deduplicate, validate, transform, MERGE into clean tables
# Silver principles:
# 1. Incremental load watermarked by _ingestion_timestamp
# 2. Deduplication within each micro-batch on business keys
# 3. Domain-specific transformations applied before MERGE
# 4. Soft deletes propagated from Bronze with _is_deleted flag
# 5. ACID MERGE guarantees exactly-once semantics

# --- Domain transformation functions ---
# Each prepare_* function receives a deduplicated incremental DataFrame
# and returns a transformed DataFrame ready for MERGE into Silver.
# I explicitly select each column to ensure no extra columns are added.
# This is a best practice to avoid unexpected schema changes

def prepare_sales_customers(df: DataFrame) -> DataFrame:
    """Normalise phone numbers to digits only."""
    return (
        df.select(
            sf.col("customerID").cast("bigint"),
            sf.col("first_name").cast("string"),
            sf.col("last_name").cast("string"),
            sf.col("email_address").cast("string"),
            sf.col("phone_number").cast("string"),
            sf.col("address").cast("string"),
            sf.col("city").cast("string"),
            sf.col("state").cast("string"),
            sf.col("country").cast("string"),
            sf.col("continent").cast("string"),
            sf.col("postal_zip_code").cast("bigint"),
            sf.col("gender").cast("string"),
            sf.col("_ingestion_timestamp").cast("timestamp"),
            sf.col("_source_file").cast("string"),
            sf.col("_batch_id").cast("string"),
            sf.col("_is_deleted").cast("boolean"),
        )
        .withColumn("phone_number", sf.regexp_replace("phone_number", r"[^0-9]", ""))
        .filter(sf.col("customerID").isNotNull())
    )


def prepare_sales_franchises(df: DataFrame) -> DataFrame:
    return (
        df.select(
            sf.col("city").cast("string"),
            sf.col("country").cast("string"),
            sf.col("district").cast("string"),
            sf.col("franchiseID").cast("bigint"),
            sf.col("latitude").cast("double"),
            sf.col("longitude").cast("double"),
            sf.col("name").cast("string"),
            sf.col("size").cast("string"),
            sf.col("supplierID").cast("bigint"),
            sf.col("zipcode").cast("string"),
            sf.col("_ingestion_timestamp").cast("timestamp"),
            sf.col("_source_file").cast("string"),
            sf.col("_batch_id").cast("string"),
            sf.col("_is_deleted").cast("boolean")
        )
        .filter(sf.col("franchiseID").isNotNull())
    )


def prepare_sales_suppliers(df: DataFrame) -> DataFrame:
    return (
        df.select(
            sf.col("approved").cast("string"),
            sf.col("city").cast("string"),
            sf.col("continent").cast("string"),
            sf.col("district").cast("string"),
            sf.col("ingredient").cast("string"),
            sf.col("latitude").cast("double"),
            sf.col("longitude").cast("double"),
            sf.col("name").cast("string"),
            sf.col("size").cast("string"),
            sf.col("supplierID").cast("string"),
            sf.col("_ingestion_timestamp").cast("timestamp"),
            sf.col("_source_file").cast("string"),
            sf.col("_batch_id").cast("string"),
            sf.col("_is_deleted").cast("boolean")
        )
        .filter(sf.col("supplierID").isNotNull())
        )


def prepare_sales_transactions(df: DataFrame) -> DataFrame:
        return (
        df.select(
            sf.col("cardNumber").cast("bigint"),
            sf.col("customerID").cast("bigint"),
            sf.col("dateTime").cast("string"),
            sf.col("franchiseID").cast("bigint"),
            sf.col("paymentMethod").cast("string"),
            sf.col("product").cast("string"),
            sf.col("quantity").cast("bigint"),
            sf.col("totalPrice").cast("bigint"),
            sf.col("transactionID").cast("bigint"),
            sf.col("unitPrice").cast("bigint"),
            sf.col("_ingestion_timestamp").cast("timestamp"),
            sf.col("_source_file").cast("string"),
            sf.col("_batch_id").cast("string"),
            sf.col("_is_deleted").cast("boolean")
        )
        .filter(sf.col("transactionID").isNotNull())
        .filter(sf.col("totalPrice").isNotNull())
        )


PREPARE_FUNCTIONS = {
    "silver.sales_customers":        prepare_sales_customers,
    "silver.sales_franchises":       prepare_sales_franchises,
    "silver.sales_suppliers":        prepare_sales_suppliers,
    "silver.sales_transactions":     prepare_sales_transactions,
}


def _get_prepare_fn(target_table_name: str):
    fn = PREPARE_FUNCTIONS.get(target_table_name)
    if fn is None:
        raise ValueError(
            f"No prepare function registered for '{target_table_name}'. "
            f"Register it in PREPARE_FUNCTIONS."
        )
    return fn


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

    Deduplication: row_number() over unique_keys ordered by _ingestion_timestamp DESC.

    Soft deletes: rows with _is_deleted=True trigger a DELETE in the MERGE.
    """
    is_incremental = start_date == "9999-01-01" or end_date == "9999-01-01"
    prepare_fn = _get_prepare_fn(target_table_name)

    source_df = spark.read.table(source_table_name)
    table_exists = spark.catalog.tableExists(target_table_name)

    if table_exists and is_incremental:
        target_df = spark.read.table(target_table_name)
        max_ts = target_df.select(sf.max("_ingestion_timestamp")).first()[0]
        logger.info(f"[SILVER] Watermark for {target_table_name}: {max_ts}")
        incremental_df = source_df.filter(sf.col("_ingestion_timestamp") > max_ts)
    elif table_exists and not is_incremental:
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

    # ---- Apply domain transformations ---------------------------------------
    prepared_df = prepare_fn(deduped_df)

    # ---- Separate deletes from upserts --------------------------------------
    deletes_df = prepared_df.filter(sf.col("_is_deleted") == True)
    upserts_df = prepared_df.filter(sf.col("_is_deleted") == False)

    # ---- Write to Silver ----------------------------------------------------
    merge_condition = " AND ".join(
        [f"target.{k} = source.{k}" for k in dedup_keys]
    )

    if table_exists:
        delta_table = DeltaTable.forName(spark, target_table_name)
        try:
            (
                delta_table.alias("target")
                .merge(prepared_df.alias("source"), merge_condition)
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

# Silver ingestion calls
SILVER_CONFIG = [
    ("bronze.sales_customers",        "silver.sales_customers",        ["customerID"]),
    ("bronze.sales_franchises",       "silver.sales_franchises",       ["franchiseID"]),
    ("bronze.sales_suppliers",        "silver.sales_suppliers",        ["supplierID"]),
    ("bronze.sales_transactions",     "silver.sales_transactions",     ["transactionID"]),
]

for src, tgt, keys in SILVER_CONFIG:
    incremental_load(src, tgt, keys)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold

# COMMAND ----------

# GOLD LAYER — Aggregated business-level table with MERGE
# Gold principles:
# 1. Read from Silver only
# 2. Business aggregations and joins
# 3. MERGE for idempotent writes (safe to re-run)
# 4. SCD Type 1 (overwrite on match)

def build_gold_customer_sales_summary() -> None:
    """
    Build customer sales summary: transactions aggregated per customer,
    enriched with customer profile.

    SCD Strategy: Type 1 — latest values overwrite previous on each run.
    """
    customers = spark.read.table("silver.sales_customers")
    transactions = spark.read.table("silver.sales_transactions")

    # --- Aggregation ---------------------------------------------------------
    trans_agg = (
        transactions
        .groupBy("customerID")
        .agg(
            sf.sum("totalPrice").alias("total_spent"),
            sf.countDistinct("transactionID").alias("total_transactions"),
            sf.round(sf.avg("totalPrice"), 2).alias("average_check"),
            sf.sum("quantity").alias("total_items_bought"),
            sf.countDistinct("franchiseID").alias("unique_franchises_visited"),
            sf.max("dateTime").alias("last_transaction_date"),
        )
    )

    customers_enriched = (
        customers
        .withColumn("full_name", sf.concat_ws(" ", sf.col("first_name"), sf.col("last_name")))
        .select(
            "customerID",
            "full_name",
            "email_address",
            "city",
            "state",
            "country",
        )
    )

    gold_df = (
        customers_enriched
        .join(trans_agg, "customerID", "left")
        .withColumn("_gold_updated_at", sf.current_timestamp())
    )

    target_table = "gold.customer_sales_summary"

    if spark.catalog.tableExists(target_table):
        delta_table = DeltaTable.forName(spark, target_table)
        try:
            (
                delta_table.alias("target")
                .merge(gold_df.alias("source"), "target.customerID = source.customerID")
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


build_gold_customer_sales_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Lake Time Travel

# COMMAND ----------

# DELTA LAKE — Time Travel demonstration

def demonstrate_time_travel(table_name: str) -> None:
    """
    Show Delta Lake time travel capabilities:
      - Query table history
      - Read a specific historical version
      - Read as of a timestamp
      - Restore to a previous version (commented out — destructive in prod)
    """
    logger.info(f"\n{'='*60}\nTime Travel Demo: {table_name}\n{'='*60}")

    # 1. Inspect table history
    history_df = spark.sql(f"DESCRIBE HISTORY {table_name}")
    display(history_df)

    # 2. Get the current and previous version numbers
    versions = history_df.select("version").orderBy(sf.col("version").desc()).limit(2).collect()
    current_version = versions[0]["version"]
    logger.info(f"Current version: {current_version}")

    if len(versions) < 2:
        logger.info("Only one version available — skipping historical reads.")
        return

    previous_version = versions[1]["version"]

    # 3. Read a specific version (VERSION AS OF)
    df_version = (
        spark.read
        .format("delta")
        .option("versionAsOf", previous_version)
        .table(table_name)
    )
    logger.info(f"Records at version {previous_version}: {df_version.count()}")

    # 4. Read as of a timestamp (TIMESTAMP AS OF)
    timestamp_version = history_df.select("timestamp").orderBy(sf.col("timestamp").asc()).first()[0]
    logger.info(timestamp_version)
    try:
        df_timestamp = (
            spark.read
            .format("delta")
            .option("timestampAsOf", timestamp_version)
            .table(table_name)
        )
        logger.info(f"Records as of {timestamp_version}: {df_timestamp.count()}")
    except Exception:
        logger.info("Timestamp version not available — table is too new.")

    # 5. SQL-style time travel (equivalent approach)
    spark.sql(f"SELECT COUNT(*) FROM {table_name} VERSION AS OF {previous_version}").show()

    # 6. Restore to a previous version — CAUTION: commented out for safety
    # delta_table = DeltaTable.forName(spark, table_name)
    # delta_table.restoreToVersion(previous_version)
    # logger.info(f"Restored {table_name} to version {previous_version}")


# Run on Silver customers (will have multiple versions after repeated pipeline runs)
demonstrate_time_travel("silver.sales_customers")
demonstrate_time_travel("gold.customer_sales_summary")