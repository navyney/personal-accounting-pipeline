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
