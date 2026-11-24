from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum, count, dense_rank, round
from pyspark.sql.window import Window

@dp.materialized_view(
    name="gold_product_sales",
    comment="Product-wise delivered sales and product performance"
)
def gold_product_sales():
    delivered_df = spark.read.table("gold_delivered_orders")

    product_sales_df = (
        delivered_df
        .groupBy("product_name")
        .agg(
                count("*").alias("total_order_records"),
            sum("quantity").alias("total_units_sold"),
            sum("order_amount").alias("total_revenue"),
            round(
                sum("order_amount") / sum("quantity"), 2
            ).alias("average_selling_price")
        )
    )

    ranking_window = Window.orderBy(col("total_revenue").desc())

    return product_sales_df.withColumn(
        "revenue_rank",
        dense_rank().over(ranking_window)
    )


