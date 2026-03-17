import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from src.etl.validations import filter_valid_transactions


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("Testing").getOrCreate()


# TODO: Add a test for filter_valid_transactions — removes null amounts
# Hint: Include rows with None amount, verify they are removed


# TODO: Add a test for filter_valid_transactions — removes out-of-range dates
# Hint: Include a row with date "2010-05-15", verify it is removed


# TODO: Add a test for filter_valid_transactions — keeps refunds (negative amounts)
# Hint: Include a row with amount -25.0, verify it is kept
