from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, sum, round, countDistinct
from pyspark.sql.window import Window

# Step 6: Order lifecycle summary across ALL valid Silver records
@dp.materialized_view(
    name="gold_order_summary",
    comment="Order lifecycle and status distribution across all valid Silver records"
)
def gold_order_summary():
    silver_df = spark.read.table("ecommerce_sdp.default.silver_orders")
    total_window = Window.partitionBy()

    return (
        silver_df
        .groupBy("order_status")
        .agg(
            count("*").alias("total_order_records"),
            countDistinct("order_id").alias("distinct_orders"),
            sum("quantity").alias("total_units"),
            sum("order_amount").alias("total_amount")
        )
        .withColumn(
            "status_percentage",
            round((col("total_order_records") / sum("total_order_records").over(total_window)) * 100, 2)
        )
        .orderBy(col("total_order_records").desc())
    )
