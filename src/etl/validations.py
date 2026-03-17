from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def filter_valid_transactions(df: DataFrame) -> DataFrame:
    """Filter out invalid transactions.

    Rules:
        - Remove rows where amount is null or empty
        - Remove rows where date is outside 2016-01-01 to 2025-12-31
        - Keep negative amounts (these are refunds)

    Args:
        df: Input transactions DataFrame with columns: transaction_id, date,
            member_id, item_name, category_id, merchant_id, amount, payment_method

    Returns:
        DataFrame with only valid transactions
    """

    df = df.filter(col("amount").isNotNull())
    df = df.filter(
        (col("date") >= "2016-01-01") &
        (col("date") <= "2025-12-31")
    )

    return df