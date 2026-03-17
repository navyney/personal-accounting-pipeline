from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max
from etl.schemas import transaction_schema, category_schema, merchant_schema

def main():
    spark = SparkSession.builder \
            .master("local[*]") \
            .appName("DataExploration") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .getOrCreate()

    transactions = spark.read \
        .schema(transaction_schema) \
        .option("header", True) \
        .csv("personal-accounting-pipeline/data/transactions.csv")

    categories = spark.read \
        .schema(category_schema) \
        .option("header", True) \
        .csv("personal-accounting-pipeline/data/categories.csv")

    merchants = spark.read \
        .schema(merchant_schema) \
        .option("header", True) \
        .csv("personal-accounting-pipeline/data/merchants.csv")

    print(f"Total transactions:", transactions.count())
    print(f"Unique family members:", transactions.select("member_id").distinct().count())
    print(f"Unique merchants:", transactions.select("merchant_id").distinct().count())
    print(f"Unique categories:", categories.select("category_id").distinct().count())
    print(f"Rows with null amount:", transactions.filter(col("amount").isNull()).count())
    print("Date range:")
    transactions.select(
        min("date"),
        max("date")
    ).show()

if __name__ == "__main__":
    main()