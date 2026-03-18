from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def main():

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("AnswerPart5") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    avg = spark.read.parquet("personal-accounting-pipeline/output/analytics/avg_amount_by_year")
    monthly = spark.read.parquet("personal-accounting-pipeline/output/analytics/monthly_by_category")
    members = spark.read.parquet("personal-accounting-pipeline/output/analytics/yearly_by_member")
    merchants = spark.read.parquet("personal-accounting-pipeline/output/analytics/top_merchants_by_year")
    enriched = spark.read.parquet("personal-accounting-pipeline/output/staged/transactions")

    print("\n===== Schema of Transactions =====")
    enriched.printSchema()

    # convert date to proper date type
    enriched = enriched.withColumn("date", to_date("date"))

    print("\n===== Average Transaction Amount Per Year =====")
    avg.orderBy("year").show()

    print("\n===== Year-over-Year Change =====")
    window = Window.orderBy("year")

    avg_with_change = avg.withColumn(
        "prev_year",
        lag("avg_amount").over(window)
    )

    avg_with_change = avg_with_change.withColumn(
        "yoy_percent",
        ((col("avg_amount") - col("prev_year")) / col("prev_year")) * 100
    )

    avg_with_change.orderBy("year").show()

    print("\n===== Total Spending by Category =====")
    monthly.groupBy("category_name") \
        .sum("total_amount") \
        .orderBy(desc("sum(total_amount)")) \
        .show()

    print("\n===== Spending by Category per Year =====")
    monthly.groupBy("year", "category_name") \
        .sum("total_amount") \
        .orderBy("year") \
        .show()

    print("\n===== Total Spending by Family Member =====")
    members.groupBy("member_id") \
        .sum("total_amount") \
        .orderBy(desc("sum(total_amount)")) \
        .show()

    print("\n===== Member Spending by Category =====")
    enriched.groupBy("member_id", "category_id") \
        .sum("amount") \
        .orderBy(desc("sum(amount)")) \
        .show()

    print("\n===== Spending Tier Distribution =====")
    tier = enriched.groupBy("spending_tier").count()
    total = enriched.count()

    tier = tier.withColumn(
        "percentage",
        col("count") / total * 100
    )

    tier.show()

    print("\n===== Spending Tier Distribution by Year =====")
    enriched.withColumn("year", year("date")) \
        .groupBy("year", "spending_tier") \
        .count() \
        .orderBy("year") \
        .show()

    print("\n===== Top Merchants Overall =====")
    merchants.groupBy("merchant_name") \
        .sum("total_amount") \
        .orderBy(desc("sum(total_amount)")) \
        .show()

    spark.stop()

if __name__ == "__main__":
    main()