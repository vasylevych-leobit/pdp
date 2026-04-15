from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as sf


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


