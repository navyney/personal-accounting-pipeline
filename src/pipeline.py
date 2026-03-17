from pyspark.sql import SparkSession
from etl.schemas import transaction_schema, category_schema, merchant_schema
from etl.validations import filter_valid_transactions

def main():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("AccountingPipeline") \
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

    transactions.write.mode("overwrite").parquet("personal-accounting-pipeline/output/raw/transactions/")
    categories.write.mode("overwrite").parquet("personal-accounting-pipeline/output/raw/categories/")
    merchants.write.mode("overwrite").parquet("personal-accounting-pipeline/output/raw/merchants/")

    # STAGED layer
    clean_transactions = filter_valid_transactions(transactions)
    clean_transactions.write.mode("overwrite").parquet("personal-accounting-pipeline/output/staged/transactions/")
        
if __name__ == "__main__":
    main()