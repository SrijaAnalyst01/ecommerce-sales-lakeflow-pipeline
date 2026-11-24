# Databricks notebook source
from pyspark.sql.functions import sum as _sum, count, col

# 1. View final Gold summary tables
order_summary_df = spark.read.table("ecommerce_sdp.default.gold_order_summary")
sales_summary_df = spark.read.table("ecommerce_sdp.default.gold_sales_summary")
display(order_summary_df)
display(sales_summary_df)

# 2. Confirm gold_order_summary total records == silver_orders count
silver_df = spark.read.table("ecommerce_sdp.default.silver_orders")
silver_cnt = silver_df.count()
summary_cnt = order_summary_df.agg(_sum("total_order_records")).collect()[0][0]
print(f"Silver Valid: {silver_cnt} | Order Summary Total: {summary_cnt} | Diff: {silver_cnt - summary_cnt}")

# 3. Inspect the 14 overlapping order_id rows (multi-product vs. duplicate check)
dup_ids = silver_df.groupBy("order_id").agg(count("*").alias("cnt")).filter(col("cnt") > 1)
display(silver_df.join(dup_ids, on="order_id", how="inner").orderBy("order_id"))


# COMMAND ----------

from pyspark.sql.functions import sum as _sum, count

bronze_cnt = spark.read.table("ecommerce_sdp.default.bronze_orders_sdp").count()
valid_cnt = spark.read.table("ecommerce_sdp.default.silver_orders").count()
rejected_cnt = spark.read.table("ecommerce_sdp.default.silver_orders_rejected").count()
typed_cnt = spark.read.table("ecommerce_sdp.default.silver_orders_typed").count()

# Duplicates removed = (Typed - Rejected) - Valid
dup_cnt = (typed_cnt - rejected_cnt) - valid_cnt

print(f"Bronze: {bronze_cnt} | Valid (Deduplicated): {valid_cnt} | Rejected: {rejected_cnt} | Duplicates Removed: {dup_cnt}")

# Check final Gold summaries & verify 0 remaining duplicates
display(spark.read.table("ecommerce_sdp.default.gold_order_summary"))
display(spark.read.table("ecommerce_sdp.default.gold_sales_summary"))
