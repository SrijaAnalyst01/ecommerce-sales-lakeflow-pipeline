from pyspark import pipelines as dp
from pyspark.sql.functions import count, sum, avg, round, countDistinct, min, max

# Step 7: Overall executive KPIs from Delivered orders
@dp.materialized_view(
    name="gold_sales_summary",
    comment="Overall business KPIs and delivered sales summary"
)
def gold_sales_summary():
    delivered_df = spark.read.table("ecommerce_sdp.default.gold_delivered_orders")

    return (
        delivered_df
        .agg(
            count("*").alias("total_delivered_records"),
            countDistinct("order_id").alias("total_delivered_orders"),
            countDistinct("customer_id").alias("unique_customers"),
            sum("quantity").alias("total_units_sold"),
            sum("order_amount").alias("total_delivered_revenue"),
            round(avg("order_amount"), 2).alias("avg_record_value"),
            round(sum("order_amount") / countDistinct("order_id"), 2).alias("avg_order_value"),
            min("order_date").alias("first_order_date"),
            max("order_date").alias("last_order_date")
        )
    )
