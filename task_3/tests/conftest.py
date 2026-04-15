import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, IntegerType


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("unit-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def sales_customers_schema():
     return StructType([
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("continent", StringType(), True),
        StructField("country", StringType(), True),
        StructField("customerID", StringType(), True),
        StructField("email_address", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("postal_zip_code", StringType(), True),
        StructField("state", StringType(), True),
        StructField("_ingestion_timestamp", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_batch_id", StringType(), True),
        StructField("_is_deleted", BooleanType(), True),
    ])


@pytest.fixture
def base_customer():
    return {
        "address": "69075 Logan Circles",
        "city": "East Catherine",
        "continent": "Asia",
        "country": "Japan",
        "customerID": "2000260",
        "email_address": "scollier@example.org",
        "first_name": "Amanda",
        "gender": "female",
        "last_name": "Reed",
        "phone_number": "+1-999-308-9110",
        "postal_zip_code": "6657",
        "state": "Rhode Island",
        "_ingestion_timestamp": "2026-04-13 14:41:30.538445",
        "_source_file": "/Volumes/workspace/lnd/raw_data",
        "_batch_id": "8c46efb9-5ccf-48bb-a50d-c701b6e2094d",
        "_is_deleted": False,
    }

@pytest.fixture
def sales_franchises_schema():
    return StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("district", StringType(), True),
        StructField("franchiseID", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("name", StringType(), True),
        StructField("size", StringType(), True),
        StructField("supplierID", StringType(), True),
        StructField("zipcode", StringType(), True),
        StructField("_ingestion_timestamp", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_batch_id", StringType(), True),
        StructField("_is_deleted", BooleanType(), True),
    ])


@pytest.fixture
def base_franchise():
    return {
        "city": "Paris",
        "country": "France",
        "district": "Île-de-France",
        "franchiseID": "101",
        "latitude": "48.8566",
        "longitude": "2.3522",
        "name": "Bakehouse Paris",
        "size": "large",
        "supplierID": "201",
        "zipcode": "75001",
        "_ingestion_timestamp": "2026-04-13 14:41:30.538445",
        "_source_file": "/Volumes/workspace/lnd/raw_data",
        "_batch_id": "8c46efb9-5ccf-48bb-a50d-c701b6e2094d",
        "_is_deleted": False,
    }


@ pytest.fixture
def sales_suppliers_schema():
    return StructType([
        StructField("approved", StringType(), True),
        StructField("city", StringType(), True),
        StructField("continent", StringType(), True),
        StructField("district", StringType(), True),
        StructField("ingredient", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("name", StringType(), True),
        StructField("size", StringType(), True),
        StructField("supplierID", StringType(), True),
        StructField("_ingestion_timestamp", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_batch_id", StringType(), True),
        StructField("_is_deleted", BooleanType(), True),
    ])

@pytest.fixture
def base_supplier():
    return {
        "approved": "ministry",
        "city": "Paris",
        "continent": "France",
        "district": "Île-de-France",
        "ingredient": "banana",
        "latitude": "48.8566",
        "longitude": "2.3522",
        "name": "Bakehouse Paris",
        "size": "large",
        "supplierID": "201",
        "_ingestion_timestamp": "2026-04-13 14:41:30.538445",
        "_source_file": "/Volumes/workspace/lnd/raw_data",
        "_batch_id": "8c46efb9-5ccf-48bb-a50d-c701b6e2094d",
        "_is_deleted": False,
    }


@ pytest.fixture
def sales_transactions_schema():
    return StructType([
        StructField("cardNumber", StringType(), True),
        StructField("customerID", StringType(), True),
        StructField("dateTime", StringType(), True),
        StructField("franchiseID", StringType(), True),
        StructField("paymentMethod", StringType(), True),
        StructField("product", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("totalPrice", StringType(), True),
        StructField("transactionID", StringType(), True),
        StructField("unitPrice", StringType(), True),
        StructField("_ingestion_timestamp", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_batch_id", StringType(), True),
        StructField("_is_deleted", BooleanType(), True),
    ])

@pytest.fixture
def base_transaction():
    return {
        "cardNumber": "4111111111111111",
        "customerID": "1",
        "dateTime": "2024-01-15 09:00:00",
        "franchiseID": "101",
        "paymentMethod": "credit",
        "product": "Croissant",
        "quantity": "2",
        "totalPrice": "10",
        "transactionID": "1001",
        "unitPrice": "5",
        "_ingestion_timestamp": "2026-04-13 14:41:30.538445",
        "_source_file": "/Volumes/workspace/lnd/raw_data",
        "_batch_id": "8c46efb9-5ccf-48bb-a50d-c701b6e2094d",
        "_is_deleted": False,
    }


@pytest.fixture
def ecommerce_store_schema():
    return StructType([
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_session", StringType(), True),
        StructField("_ingestion_timestamp", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_batch_id", StringType(), True),
        StructField("_is_deleted", BooleanType(), True),
    ])


@pytest.fixture
def base_ecommerce():
    return {
        "event_time": "2024-01-15 09:00:00",
        "event_type": "purchase",
        "product_id": "qwertyuiop",
        "category_id": "ret.fok",
        "category_code": "who knows",
        "brand": "ukraine def tech",
        "price": "2",
        "user_id": "10",
        "user_session": "1001",
        "_ingestion_timestamp": "2026-04-13 14:41:30.538445",
        "_source_file": "/Volumes/workspace/lnd/raw_data",
        "_batch_id": "8c46efb9-5ccf-48bb-a50d-c701b6e2094d",
        "_is_deleted": False,
    }


@pytest.fixture
def ecommerce_brands_schema():
    return StructType([
        StructField("brand", StringType(), True),
        StructField("brand_tier", StringType(), True),
        StructField("_ingestion_timestamp", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_batch_id", StringType(), True),
        StructField("_is_deleted", BooleanType(), True),
    ])


@pytest.fixture
def base_ecommerce_brand():
    return {
        "brand": "Leobit",
        "brand_tier": "premium",
        "_ingestion_timestamp": "2026-04-13 14:41:30.538445",
        "_source_file": "/Volumes/workspace/lnd/raw_data",
        "_batch_id": "8c46efb9-5ccf-48bb-a50d-c701b6e2094d",
        "_is_deleted": False,
    }
