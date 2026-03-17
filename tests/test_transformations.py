import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from src.etl.transformations import categorize_spending, enrich_with_lookups


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("Testing").getOrCreate()


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


# TODO: Add a test for categorize_spending boundary values
# Hint: Test exact boundaries — 10.0, 50.0, 200.0


# TODO: Add a test for enrich_with_lookups
# Hint: Create small category and merchant DataFrames, join them with transactions,
#       and verify the result includes category_name and merchant_name


# TODO: Add a test for enrich_with_lookups with orphan merchant_ids
# Hint: Include a transaction whose merchant_id is NOT in the merchants table.
#       After a left join, merchant_name should be null for that row.
