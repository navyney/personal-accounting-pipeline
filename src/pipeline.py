from pyspark.sql import SparkSession
from etl.schemas import transaction_schema, category_schema, merchant_schema
from etl.validations import filter_valid_transactions
from etl.transformations import enrich_with_lookups, categorize_spending
from pyspark.sql.functions import col, year, month, sum, avg, row_number, to_date
from pyspark.sql.window import Window

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
    clean_transactions = categorize_spending(clean_transactions)
    clean_transactions.write.mode("overwrite").parquet("personal-accounting-pipeline/output/staged/transactions/")

    # output/analytics 
    staged_transactions = spark.read.parquet("personal-accounting-pipeline/output/staged/transactions/")
    enriched = enrich_with_lookups(
        staged_transactions,
        categories,
        merchants
    )
    enriched.write \
        .mode("overwrite") \
        .parquet("personal-accounting-pipeline/output/analytics/enriched_transactions")

    # query for answering in the report
    print("Transactions with missing merchant: " ,enriched.filter(col("merchant_name").isNull()).count())

    # part 5
    analytics_df = staged_transactions \
        .withColumn("date", to_date("date")) \
        .join(categories, "category_id", "left") \
        .join(merchants, "merchant_id", "left")

    monthly_by_category = analytics_df.groupBy(
        year("date").alias("year"),
        month("date").alias("month"),
        "category_name"
    ).agg(
        sum("amount").alias("total_amount")
    )

    monthly_by_category.write \
        .mode("overwrite") \
        .parquet("personal-accounting-pipeline/output/analytics/monthly_by_category/")
    
    yearly_by_member = analytics_df.groupBy(
        year("date").alias("year"),
        "member_id"
    ).agg(
        sum("amount").alias("total_amount")
    )

    yearly_by_member.write \
        .mode("overwrite") \
        .parquet("personal-accounting-pipeline/output/analytics/yearly_by_member/")

    merchant_yearly = analytics_df.groupBy(
        year("date").alias("year"),
        "merchant_name"
    ).agg(
        sum("amount").alias("total_amount")
    )

    window_spec = Window.partitionBy("year").orderBy(col("total_amount").desc())

    ranked = merchant_yearly.withColumn(
        "rank",
        row_number().over(window_spec)
    )

    top_merchants_by_year = ranked.filter(
        col("rank") <= 10
    )

    top_merchants_by_year.write \
        .mode("overwrite") \
        .parquet("personal-accounting-pipeline/output/analytics/top_merchants_by_year/")

    avg_amount_by_year = analytics_df.groupBy(
        year("date").alias("year")
    ).agg(
        avg("amount").alias("avg_amount")
    )

    avg_amount_by_year.write \
        .mode("overwrite") \
        .parquet("personal-accounting-pipeline/output/analytics/avg_amount_by_year/")

if __name__ == "__main__":
    main()