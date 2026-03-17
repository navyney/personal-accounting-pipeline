# Define your explicit schemas here using StructType and StructField.
#
# You need three schemas:
#   - transaction_schema  (for transactions.csv)
#   - category_schema     (for categories.csv)
#   - merchant_schema     (for merchants.csv)
#
# Example:
#   from pyspark.sql.types import StructType, StructField, StringType, FloatType
#
#   my_schema = StructType([
#       StructField("id", StringType(), True),
#       StructField("value", FloatType(), True),
#   ])
#
# Hint: Look at the column descriptions in PROJECT.md.
# Hint: Think carefully about which columns should be StringType vs FloatType.
#       The CSV stores everything as text — your schema controls how Spark reads it.

from pyspark.sql.types import StructType, StructField, StringType, FloatType

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", FloatType(), True),
    StructField("payment_method", StringType(), True),
])

category_schema = StructType([
    StructField("category_id", StringType(), True),
    StructField("category_name", StringType(), True),
    StructField("budget_type", StringType(), True),
])

merchant_schema = StructType([
    StructField("merchant_id", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_type", StringType(), True),
    StructField("location", StringType(), True),
])