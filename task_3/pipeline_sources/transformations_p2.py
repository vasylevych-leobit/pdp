import pyspark.sql.functions as sf


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


