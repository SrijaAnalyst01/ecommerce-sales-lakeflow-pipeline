from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum, count, avg, round, dense_rank
from pyspark.sql.window import Window

@dp.materialized_view(
    name="gold_city_sales",
    comment="City-wise delivered orders and sales performance"
)
def gold_city_sales():
    delivered_df = spark.read.table("gold_delivered_orders")

    city_sales_df = (
        delivered_df
        .groupBy("city")
        .agg(
            count("*").alias("total_delivered_orders"),
            sum("order_amount").alias("total_revenue"),
            round(avg("order_amount"), 2).alias("average_order_value")
        )
    )

    ranking_window = Window.orderBy(col("total_revenue").desc())

    return city_sales_df.withColumn(
        "revenue_rank",
        dense_rank().over(ranking_window)
    )
