# Databricks notebook source
# MAGIC %md
# MAGIC # SCD type 2
# MAGIC ## This is the first pipeline from the first PDP goal

# COMMAND ----------

import logging
import uuid
from datetime import datetime, timedelta

import pyspark.sql.functions as sf
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import StringType, StructField, StructType


# COMMAND ----------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# COMMAND ----------

# this block is unchanged

CATALOG = "workspace"
LND_SCHEMA = "lnd"
VOLUME_FOLDER = f"/Volumes/{CATALOG}/{LND_SCHEMA}/raw_data"

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

# this block is unchanged

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
# MAGIC #### Bronze

# COMMAND ----------

# this block is unchanged

def ingest_bronze(source_file_path: str, target_table_name: str) -> None:
    """
    Ingest raw JSON files into a Bronze Delta table.
    """
    logger.info(f"[BRONZE] Ingesting '{source_file_path}' → '{target_table_name}'")

    raw_df = (
        spark.read
        .format("json")
        .option("inferSchema", "false")
        .load(source_file_path)
    )

    record_count = raw_df.count()
    logger.info(f"[BRONZE] Records read: {record_count}")

    batch_id = str(uuid.uuid4())

    enriched_df = (
        raw_df
        .withColumn("_ingestion_timestamp", sf.current_timestamp())
        .withColumn("_source_file", sf.lit(source_file_path))
        .withColumn("_batch_id", sf.lit(batch_id))
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

# this block is unchanged
partition_folder = datetime.now().strftime("%Y-%m-%d_%H")

for folder_name, config in SOURCE_VOLUME_TABLES.items():
    source_path = f"{VOLUME_FOLDER}/{folder_name}/{partition_folder}"
    ingest_bronze(source_path, config["target_bronze"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Silver

# COMMAND ----------

# this block is unchanged

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
        logger.info(f"[SILVER] First run for {target_table_name} — full load")
        incremental_df = source_df

    raw_count = incremental_df.count()
    logger.info(f"[SILVER] Records in slice: {raw_count}")

    if raw_count == 0:
        logger.info(f"[SILVER] No new records for {target_table_name}. Skipping.")
        return

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

    
    prepared_df = prepare_fn(deduped_df)

    
    deletes_df = prepared_df.filter(sf.col("_is_deleted") == True)
    upserts_df = prepared_df.filter(sf.col("_is_deleted") == False)

    
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
            upserts_df          
            .write
            .format("delta")
            .saveAsTable(target_table_name)
        )
        logger.info(f"[SILVER] Created {target_table_name} with {upserts_df.count()} records")


# COMMAND ----------

# this block is unchanged
SILVER_CONFIG = [
    ("bronze.sales_franchises",       "silver.sales_franchises",       ["franchiseID"]),
    ("bronze.sales_suppliers",        "silver.sales_suppliers",        ["supplierID"]),
    ("bronze.sales_transactions",     "silver.sales_transactions",     ["transactionID"]),
]

for src, tgt, keys in SILVER_CONFIG:
    incremental_load(src, tgt, keys)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define incremental load with SCD type 2
# MAGIC

# COMMAND ----------

def save_as_scd2(source_df, target_table: str, business_keys: list[str], tracked_cols: list[str]):
    """
    Here are demonstrated CTE and SCD type 2
    """
    source_df.createOrReplaceTempView("scd2_source")

    # if target doesn't exist, we save source as scd2
    if not spark.catalog.tableExists(target_table):
        result = (
            source_df
            .withColumn("effective_from", sf.current_timestamp())
            .withColumn("effective_to", sf.lit(None).cast("timestamp"))
            .withColumn("is_current", sf.lit(True))
        )
        result.write.format("delta").saveAsTable(target_table)
        return

    # join source to current target rows to find changes
    target_df = spark.read.table(target_table).filter(sf.col("is_current") == True)
    target_df.createOrReplaceTempView("scd2_target")

    change_condition = " OR ".join([f"s.{c} != t.{c}" for c in tracked_cols])
    key_condition = " AND ".join([f"s.{k} = t.{k}" for k in business_keys])

    # fetch new and updated records
    changed_df = spark.sql(f"""
        WITH source_data AS (
            SELECT * FROM scd2_source
        ),
        current_target AS (
            SELECT * FROM scd2_target
        ),
        changes AS (
            SELECT s.*
            FROM source_data s
            JOIN current_target t ON {key_condition}
            WHERE {change_condition}
            -- and t.is_current = true
        ),
        new_records AS (
            SELECT s.*
            FROM source_data s
            LEFT JOIN current_target t ON {key_condition}
            WHERE t.{business_keys[0]} IS NULL
        )
        SELECT * FROM changes
        UNION ALL
        SELECT * FROM new_records
    """)

    delta_table = DeltaTable.forName(spark, target_table)

    # set effective_to for old rows
    key_join = " AND ".join([f"target.{k} = updates.{k}" for k in business_keys])
    delta_table.alias("target").merge(
        changed_df.alias("updates"), key_join
    ).whenMatchedUpdate(
        condition="target.is_current = true",
        set={"effective_to": "current_timestamp()", "is_current": "false"}
    ).execute()

    # Step 5: insert new and changed records
    new_rows = changed_df \
        .withColumn("effective_from", sf.current_timestamp()) \
        .withColumn("effective_to", sf.lit(None).cast("timestamp")) \
        .withColumn("is_current", sf.lit(True))

    new_rows.write.format("delta").mode("append").saveAsTable(target_table)

# COMMAND ----------

def incremental_load_with_scd2(
    source_table_name: str,
    target_table_name: str,
    unique_keys: list[str],
    tracked_columns: list[str],
    start_date: str = "9999-01-01",
    end_date: str = "9999-01-01",
) -> None:
    """
    Here Window function row_number() is implemented through SQL
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

    
    dedup_keys = list(unique_keys)
    incremental_df.createOrReplaceTempView("incremental_df")

    deduped_df = spark.sql(f"""
                with temp as(
                    select *, 
                    row_number() over(partition by {",".join(dedup_keys)} order by _ingestion_timestamp desc) as _row_num
                    from incremental_df
                )
                select * 
                from temp
                where _row_num = 1
                """)
    deduped_df.drop("_row_num")

    deduped_count = deduped_df.count()
    logger.info(f"[SILVER] Records after dedup: {deduped_count}")

    
    prepared_df = prepare_fn(deduped_df)

    save_as_scd2(prepared_df, target_table_name, business_keys=unique_keys, tracked_cols=tracked_columns)
    

# COMMAND ----------

PREPARE_FUNCTIONS["silver.sales_customers_scd2"] = prepare_sales_customers

tracked_columns=["first_name", "last_name", "email_address", "phone_number", "address", "city", "state", "country", "continent", "postal_zip_code", "gender"]

incremental_load_with_scd2(
    source_table_name="bronze.sales_customers",
    target_table_name="silver.sales_customers_scd2",
    unique_keys=["customerID"],
    tracked_columns=tracked_columns
)

# COMMAND ----------

# simulate change in customers data

customer = spark.read.table("bronze.sales_customers").filter(sf.col("customerID") == 2000000).orderBy(sf.col("_ingestion_timestamp").asc()).limit(1)

# Update name of customerID = 2000000
update = (
    customer
        .withColumn("first_name", sf.when(sf.col("customerID") == 2000000, "THE BEST NAME").otherwise(sf.col("first_name"))
                    )
        .withColumn("_ingestion_timestamp", sf.when(sf.col("customerID") == 2000000, sf.current_timestamp()).otherwise(sf.col("_ingestion_timestamp")))
)
update.writeTo("bronze.sales_customers").append()


# COMMAND ----------

incremental_load_with_scd2(
    source_table_name="bronze.sales_customers",
    target_table_name="silver.sales_customers_scd2",
    unique_keys=["customerID"],
    tracked_columns=tracked_columns
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select customerID, first_name, effective_from, effective_to, is_current, *
# MAGIC from silver.sales_customers_scd2
# MAGIC where customerID = 2000000
# MAGIC order by effective_to desc
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gold

# COMMAND ----------

# this block is untouched

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


# build_gold_customer_sales_summary()