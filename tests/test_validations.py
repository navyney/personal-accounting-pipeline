import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from src.etl.validations import filter_valid_transactions


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("Testing").getOrCreate()

# TODO: Add a test for filter_valid_transactions — removes null amounts
# Hint: Include rows with None amount, verify they are removed
def test_filter_removes_null_amounts(spark):
    input_data = [
        (1, "2020-01-01", 101, "itemA", 1, 10, 50.0, "cash"),
        (2, "2020-01-02", 102, "itemB", 1, 10, None, "card"),
    ]

    columns = [
        "transaction_id", "date", "member_id", "item_name",
        "category_id", "merchant_id", "amount", "payment_method"
    ]

    input_df = spark.createDataFrame(input_data, columns)
    expected_data = [
        (1, "2020-01-01", 101, "itemA", 1, 10, 50.0, "cash"),
    ]

    expected_df = spark.createDataFrame(expected_data, columns)
    result_df = filter_valid_transactions(input_df)
    assert_df_equality(result_df, expected_df, ignore_row_order=True)

# TODO: Add a test for filter_valid_transactions — removes out-of-range dates
# Hint: Include a row with date "2010-05-15", verify it is removed
def test_filter_removes_invalid_dates(spark):
    input_data = [
        (1, "2010-05-15", 101, "itemA", 1, 10, 20.0, "cash"),
        (2, "2020-06-10", 102, "itemB", 1, 10, 30.0, "card"),
    ]

    columns = [
        "transaction_id", "date", "member_id", "item_name",
        "category_id", "merchant_id", "amount", "payment_method"
    ]

    input_df = spark.createDataFrame(input_data, columns)
    
    expected_data = [
        (2, "2020-06-10", 102, "itemB", 1, 10, 30.0, "card"),
    ]
    expected_df = spark.createDataFrame(expected_data, columns)
    
    result_df = filter_valid_transactions(input_df)
    assert_df_equality(result_df, expected_df, ignore_row_order=True)

# TODO: Add a test for filter_valid_transactions — keeps refunds (negative amounts)
# Hint: Include a row with amount -25.0, verify it is kept
def test_filter_keeps_negative_amounts(spark):
    input_data = [
        (1, "2020-01-01", 101, "itemA", 1, 10, -25.0, "cash"),
        (2, "2020-01-02", 102, "itemB", 1, 10, 40.0, "card"),
    ]

    columns = [
        "transaction_id", "date", "member_id", "item_name",
        "category_id", "merchant_id", "amount", "payment_method"
    ]

    input_df = spark.createDataFrame(input_data, columns)
    
    expected_data = [
        (1, "2020-01-01", 101, "itemA", 1, 10, -25.0, "cash"),
        (2, "2020-01-02", 102, "itemB", 1, 10, 40.0, "card"),
    ]
    expected_df = spark.createDataFrame(expected_data, columns)
    
    result_df = filter_valid_transactions(input_df)
    assert_df_equality(result_df, expected_df, ignore_row_order=True)