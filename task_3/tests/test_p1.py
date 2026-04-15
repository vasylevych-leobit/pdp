from pyspark.sql.types import LongType, TimestampType, BooleanType, DoubleType, StringType
import pyspark.sql.functions as sf
from pipeline_sources.transformations_p1 import (
    prepare_sales_customers,
    prepare_sales_franchises,
    prepare_sales_suppliers,
    prepare_sales_transactions
)


class TestPrepareSalesCustomers:
    def test_phone_number(self, spark, sales_customers_schema, base_customer):
        df = spark.createDataFrame(
            [base_customer],
            schema=sales_customers_schema
        )

        rows = prepare_sales_customers(df).collect()
        assert rows[0]["phone_number"] == "19993089110"


    def test_phone_number_strips_dots(self, spark, sales_customers_schema, base_customer):
        row = {**base_customer, "phone_number": ".5.55.222.33.....33"}
        df = spark.createDataFrame([row], schema=sales_customers_schema)
        rows = prepare_sales_customers(df).collect()
        assert rows[0]["phone_number"] == "5552223333"


    def test_null_customer_id(self, spark, sales_customers_schema, base_customer):
        null_row = {**base_customer, "customerID": None}

        df = spark.createDataFrame(
            [base_customer, null_row]
            ,schema=sales_customers_schema
        )

        num_rows = prepare_sales_customers(df).count()
        assert num_rows == 1


    def test_customer_id_datatype(self, spark, sales_customers_schema, base_customer):
        df = spark.createDataFrame(
            [base_customer]
            ,schema=sales_customers_schema
        )

        result = prepare_sales_customers(df)

        assert isinstance(result.schema["customerID"].dataType, LongType)


    def test_postal_zipcode_datatype(self, spark, sales_customers_schema, base_customer):
        df = spark.createDataFrame(
            [base_customer]
            ,schema=sales_customers_schema
        )

        result = prepare_sales_customers(df)

        assert isinstance(result.schema["postal_zip_code"].dataType, LongType)


    def test_ingestion_timestamp_datatype(self, spark, sales_customers_schema, base_customer):
        df = spark.createDataFrame(
            [base_customer]
            ,schema=sales_customers_schema
        )

        result = prepare_sales_customers(df)

        assert isinstance(result.schema["_ingestion_timestamp"].dataType, TimestampType)


    def test_is_deleted_datatype(self, spark, sales_customers_schema, base_customer):

        df = spark.createDataFrame(
            [base_customer]
            ,schema=sales_customers_schema
        )

        result = prepare_sales_customers(df)

        assert isinstance(result.schema["_is_deleted"].dataType, BooleanType)


    def test_num_of_columns(self, spark, sales_customers_schema, base_customer):
        df = (spark.createDataFrame(
            [base_customer]
            ,schema=sales_customers_schema
        )
        .withColumn("extra_col", sf.lit("test")))

        result = prepare_sales_customers(df)

        assert "extra_col" not in result.columns


class TestPrepareSalesFranchises:
    def test_null_franchise_id(self, spark, sales_franchises_schema, base_franchise):
        null_id = {**base_franchise, "franchiseID": None}
        df = spark.createDataFrame([base_franchise, null_id], schema=sales_franchises_schema)

        result = prepare_sales_franchises(df).count()

        assert result == 1

    def test_franchise_id_datatype(self, spark, sales_franchises_schema, base_franchise):
        df = spark.createDataFrame(
            [base_franchise]
            ,schema=sales_franchises_schema
        )

        result = prepare_sales_franchises(df)

        assert isinstance(result.schema["franchiseID"].dataType, LongType)

    def test_latitude_datatype(self, spark, sales_franchises_schema, base_franchise):
        df = spark.createDataFrame(
            [base_franchise]
            ,schema=sales_franchises_schema
        )

        result = prepare_sales_franchises(df)

        assert isinstance(result.schema["latitude"].dataType, DoubleType)

    def test_longitude_datatype(self, spark, sales_franchises_schema, base_franchise):
        df = spark.createDataFrame(
            [base_franchise]
            ,schema=sales_franchises_schema
        )

        result = prepare_sales_franchises(df)

        assert isinstance(result.schema["longitude"].dataType, DoubleType)

    def test_supplier_id_datatype(self, spark, sales_franchises_schema, base_franchise):
        df = spark.createDataFrame(
            [base_franchise]
            , schema=sales_franchises_schema
        )

        result = prepare_sales_franchises(df)

        assert isinstance(result.schema["supplierID"].dataType, LongType)

    def test_ingestion_timestamp_datatype(self, spark, sales_franchises_schema, base_franchise):
        df = spark.createDataFrame(
            [base_franchise]
            , schema=sales_franchises_schema
        )

        result = prepare_sales_franchises(df)

        assert isinstance(result.schema["_ingestion_timestamp"].dataType, TimestampType)

    def test_is_deleted_datatype(self, spark, sales_franchises_schema, base_franchise):
        df = spark.createDataFrame(
            [base_franchise]
            , schema=sales_franchises_schema
        )

        result = prepare_sales_franchises(df)

        assert isinstance(result.schema["_is_deleted"].dataType, BooleanType)

    def test_num_of_columns(self, spark, sales_franchises_schema, base_franchise):
        df = (spark.createDataFrame(
            [base_franchise]
            ,schema=sales_franchises_schema
        )
        .withColumn("extra_col", sf.lit("test")))

        result = prepare_sales_franchises(df)

        assert "extra_col" not in result.columns


class TestPrepareSalesSuppliers:
    def test_supplier_id_null(self, spark, sales_suppliers_schema, base_supplier):
        nul_row = {**base_supplier, "supplierID": None}
        df = spark.createDataFrame([base_supplier, nul_row], schema=sales_suppliers_schema)

        result = prepare_sales_suppliers(df).count()

        assert result == 1

    def test_supplier_id_datatype(self, spark, sales_suppliers_schema, base_supplier):
        df = spark.createDataFrame(
            [base_supplier]
            ,schema=sales_suppliers_schema
        )

        result = prepare_sales_suppliers(df)

        assert isinstance(result.schema["supplierID"].dataType, StringType)

    def test_latitude_datatype(self, spark, sales_suppliers_schema, base_supplier):
        df = spark.createDataFrame(
            [base_supplier]
            ,schema=sales_suppliers_schema
        )

        result = prepare_sales_suppliers(df)

        assert isinstance(result.schema["latitude"].dataType, DoubleType)

    def test_longitude_datatype(self, spark, sales_suppliers_schema, base_supplier):
        df = spark.createDataFrame(
            [base_supplier]
            ,schema=sales_suppliers_schema
        )

        result = prepare_sales_suppliers(df)

        assert isinstance(result.schema["longitude"].dataType, DoubleType)

    def test_ingestion_timestamp_datatype(self, spark, sales_suppliers_schema, base_supplier):
        df = spark.createDataFrame(
            [base_supplier]
            , schema=sales_suppliers_schema
        )

        result = prepare_sales_suppliers(df)

        assert isinstance(result.schema["_ingestion_timestamp"].dataType, TimestampType)

    def test_is_deleted_datatype(self, spark, sales_suppliers_schema, base_supplier):
        df = spark.createDataFrame(
            [base_supplier]
            , schema=sales_suppliers_schema
        )

        result = prepare_sales_suppliers(df)

        assert isinstance(result.schema["_is_deleted"].dataType, BooleanType)

    def test_num_of_columns(self, spark, sales_suppliers_schema, base_supplier):
        df = (spark.createDataFrame(
            [base_supplier]
            ,schema=sales_suppliers_schema
        )
        .withColumn("extra_col", sf.lit("test")))

        result = prepare_sales_suppliers(df)

        assert "extra_col" not in result.columns


class TestSalesTransactions:
    def test_transaction_id_null(self, spark, sales_transactions_schema, base_transaction):
        nul_row = {**base_transaction, "transactionID": None}
        df = spark.createDataFrame([base_transaction, nul_row], schema=sales_transactions_schema)

        result = prepare_sales_transactions(df).count()

        assert result == 1

    def test_total_price_null(self, spark, sales_transactions_schema, base_transaction):
        nul_row = {**base_transaction, "totalPrice": None}
        df = spark.createDataFrame([base_transaction, nul_row], schema=sales_transactions_schema)

        result = prepare_sales_transactions(df).count()

        assert result == 1

    def test_card_number_datatype(self, spark, sales_transactions_schema, base_transaction):
        df = spark.createDataFrame(
            [base_transaction]
            , schema=sales_transactions_schema
        )

        result = prepare_sales_transactions(df)

        assert isinstance(result.schema["cardNumber"].dataType, LongType)

    def test_customer_id_datatype(self, spark, sales_transactions_schema, base_transaction):
        df = spark.createDataFrame(
            [base_transaction]
            , schema=sales_transactions_schema
        )

        result = prepare_sales_transactions(df)

        assert isinstance(result.schema["customerID"].dataType, LongType)

    def test_franchise_id_datatype(self, spark, sales_transactions_schema, base_transaction):
        df = spark.createDataFrame(
            [base_transaction]
            , schema=sales_transactions_schema
        )

        result = prepare_sales_transactions(df)

        assert isinstance(result.schema["franchiseID"].dataType, LongType)

    def test_quantity_datatype(self, spark, sales_transactions_schema, base_transaction):
        df = spark.createDataFrame(
            [base_transaction]
            , schema=sales_transactions_schema
        )

        result = prepare_sales_transactions(df)

        assert isinstance(result.schema["quantity"].dataType, LongType)

    def test_total_price_datatype(self, spark, sales_transactions_schema, base_transaction):
        df = spark.createDataFrame(
            [base_transaction]
            , schema=sales_transactions_schema
        )

        result = prepare_sales_transactions(df)

        assert isinstance(result.schema["totalPrice"].dataType, LongType)

    def test_transaction_id_datatype(self, spark, sales_transactions_schema, base_transaction):
        df = spark.createDataFrame(
            [base_transaction]
            , schema=sales_transactions_schema
        )

        result = prepare_sales_transactions(df)

        assert isinstance(result.schema["transactionID"].dataType, LongType)

    def test_unit_price_datatype(self, spark, sales_transactions_schema, base_transaction):
        df = spark.createDataFrame(
            [base_transaction]
            , schema=sales_transactions_schema
        )

        result = prepare_sales_transactions(df)

        assert isinstance(result.schema["unitPrice"].dataType, LongType)

    def test_ingestion_timestamp_datatype(self, spark, sales_transactions_schema, base_transaction):
        df = spark.createDataFrame(
            [base_transaction]
            , schema=sales_transactions_schema
        )

        result = prepare_sales_transactions(df)

        assert isinstance(result.schema["_ingestion_timestamp"].dataType, TimestampType)

    def test_is_deleted_datatype(self, spark, sales_transactions_schema, base_transaction):
        df = spark.createDataFrame(
            [base_transaction]
            , schema=sales_transactions_schema
        )

        result = prepare_sales_transactions(df)

        assert isinstance(result.schema["_is_deleted"].dataType, BooleanType)
