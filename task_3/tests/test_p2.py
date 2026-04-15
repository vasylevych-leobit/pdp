import pyspark.sql.functions as sf

from pipeline_sources.transformations_p2 import (
    process_data,
    extract_brands,
    extract_purchases,
    extract_views,
    extract_carts,
    extract_users
)


class TestProcessData:
    def test_user_id_null(self, spark, ecommerce_store_schema, base_ecommerce):
        null_row = {**base_ecommerce, "user_id": None}

        df = spark.createDataFrame(
            [base_ecommerce, null_row]
            ,schema=ecommerce_store_schema
        )

        num_rows = process_data(df).count()
        assert num_rows == 1

    def test_price_null(self, spark, ecommerce_store_schema, base_ecommerce):
        null_row = {**base_ecommerce, "price": None}

        df = spark.createDataFrame(
            [base_ecommerce, null_row]
            , schema=ecommerce_store_schema
        )

        num_rows = process_data(df).count()
        assert num_rows == 1

    def test_price_zero(self, spark, ecommerce_store_schema, base_ecommerce):
        null_row = {**base_ecommerce, "price": 0}

        df = spark.createDataFrame(
            [base_ecommerce, null_row]
            , schema=ecommerce_store_schema
        )

        num_rows = process_data(df).count()
        assert num_rows == 1


class TestExtractBrands:
    def test_brand_null(self, spark, ecommerce_brands_schema, base_ecommerce_brand):
        null_row = {**base_ecommerce_brand, "brand": None}

        df = spark.createDataFrame(
            [base_ecommerce_brand, null_row]
            , schema=ecommerce_brands_schema
        )

        num_rows = extract_brands(df).count()
        assert num_rows == 1

    def test_brand_tier_column_present(self, spark, ecommerce_brands_schema, base_ecommerce_brand):
        df = spark.createDataFrame(
            [base_ecommerce_brand]
            , schema=ecommerce_brands_schema
        )

        result = extract_brands(df)
        assert "brand_tier" in result.columns


    def test_brand_tier_premium(self, spark, ecommerce_brands_schema, base_ecommerce_brand):
        premium_brand = {**base_ecommerce_brand, "brand": "apple"}

        df = spark.createDataFrame(
            [base_ecommerce_brand, premium_brand]
            , schema=ecommerce_brands_schema
        )

        num_rows = extract_brands(df).filter(sf.col("brand_tier") == "premium").count()
        assert num_rows == 1

    def test_brand_tier_medium(self, spark, ecommerce_brands_schema, base_ecommerce_brand):
        mid_brand = {**base_ecommerce_brand, "brand": "sony"}

        df = spark.createDataFrame(
            [base_ecommerce_brand, mid_brand]
            , schema=ecommerce_brands_schema
        )

        num_rows = extract_brands(df).filter(sf.col("brand_tier") == "mid").count()
        assert num_rows == 1

    def test_brand_tier_other(self, spark, ecommerce_brands_schema, base_ecommerce_brand):
        df = spark.createDataFrame(
            [base_ecommerce_brand]
            , schema=ecommerce_brands_schema
        )

        num_rows = extract_brands(df).filter(sf.col("brand_tier") == "other").count()
        assert num_rows == 1
