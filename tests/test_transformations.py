import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from src.etl.transformations import categorize_spending, enrich_with_lookups
from pyspark.sql.types import StructType, StructField, LongType, StringType

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
            .master("local[*]") \
            .appName("Testing") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .getOrCreate()

def test_categorize_spending_basic(spark):
    """This test is provided as a starting point. It will FAIL until you implement categorize_spending."""
    
    input_data = [(5.0,), (25.0,), (100.0,), (500.0,)]
    input_df = spark.createDataFrame(input_data, ["amount"])

    expected_data = [
        (5.0, "micro"),
        (25.0, "small"),
        (100.0, "medium"),
        (500.0, "large"),
    ]
    expected_df = spark.createDataFrame(expected_data, ["amount", "spending_tier"])

    result_df = categorize_spending(input_df)
    assert_df_equality(result_df, expected_df, ignore_nullable=True)


# TODO: Add a test for categorize_spending with negative amounts (refunds)
# Hint: -25.0 should be categorized as "small" (based on absolute value)
def test_categorize_spending_negative_amount(spark):
    input_data = [(-25.0,)]
    input_df = spark.createDataFrame(input_data, ["amount"])

    expected_data = [(-25.0, "small")]
    expected_df = spark.createDataFrame(expected_data, ["amount", "spending_tier"])

    result_df = categorize_spending(input_df)
    assert_df_equality(result_df, expected_df, ignore_nullable=True)

# TODO: Add a test for categorize_spending boundary values
# Hint: Test exact boundaries — 10.0, 50.0, 200.0
def test_categorize_spending_boundaries(spark):
    input_data = [(10.0,), (50.0,), (200.0,)]
    input_df = spark.createDataFrame(input_data, ["amount"])

    expected_data = [
        (10.0, "small"),
        (50.0, "medium"),
        (200.0, "large"),
    ]
    expected_df = spark.createDataFrame(expected_data, ["amount", "spending_tier"])
    
    result_df = categorize_spending(input_df)
    assert_df_equality(result_df, expected_df, ignore_nullable=True)

# TODO: Add a test for enrich_with_lookups
# Hint: Create small category and merchant DataFrames, join them with transactions,
#       and verify the result includes category_name and merchant_name
def test_enrich_with_lookups(spark):
    transactions_data = [
        (1, 1, 10),
    ]
    transactions_df = spark.createDataFrame(
        transactions_data,
        ["transaction_id", "category_id", "merchant_id"]
    )

    categories_data = [
        (1, "Food", "essential"),
    ]
    categories_df = spark.createDataFrame(
        categories_data,
        ["category_id", "category_name", "budget_type"]
    )

    merchants_data = [
        (10, "Starbucks", "cafe"),
    ]
    merchants_df = spark.createDataFrame(
        merchants_data,
        ["merchant_id", "merchant_name", "merchant_type"]
    )

    result_df = enrich_with_lookups(
        transactions_df,
        categories_df,
        merchants_df
    )

    expected_data = [
        (1, 1, 10, "Food", "essential", "Starbucks", "cafe"),
    ]
    expected_df = spark.createDataFrame(
        expected_data,
        [
            "transaction_id",
            "category_id",
            "merchant_id",
            "category_name",
            "budget_type",
            "merchant_name",
            "merchant_type",
        ],
    )
    assert_df_equality(result_df, expected_df, ignore_nullable=True)

# TODO: Add a test for enrich_with_lookups with orphan merchant_ids
# Hint: Include a transaction whose merchant_id is NOT in the merchants table.
#       After a left join, merchant_name should be null for that row.
def test_enrich_with_lookups_orphan_merchant(spark):
    transactions_data = [
        (1, 1, 999),
    ]
    transactions_df = spark.createDataFrame(
        transactions_data,
        ["transaction_id", "category_id", "merchant_id"]
    )

    categories_data = [
        (1, "Food", "essential"),
    ]
    categories_df = spark.createDataFrame(
        categories_data,
        ["category_id", "category_name", "budget_type"]
    )

    merchants_data = [
        (10, "Starbucks", "cafe"),
    ]
    merchants_df = spark.createDataFrame(
        merchants_data,
        ["merchant_id", "merchant_name", "merchant_type"]
    )

    result_df = enrich_with_lookups(
        transactions_df,
        categories_df,
        merchants_df
    )

    schema = StructType([
        StructField("transaction_id", LongType(), True),
        StructField("category_id", LongType(), True),
        StructField("merchant_id", LongType(), True),
        StructField("category_name", StringType(), True),
        StructField("budget_type", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_type", StringType(), True),
    ])

    expected_data = [
        (1, 1, 999, "Food", "essential", None, None),
    ]
    expected_df = spark.createDataFrame(expected_data, schema)
    assert_df_equality(result_df, expected_df, ignore_nullable=True)