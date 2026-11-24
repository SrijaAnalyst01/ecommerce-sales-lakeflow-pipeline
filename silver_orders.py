from pyspark import pipelines as dp
from pyspark.sql.functions import col, concat_ws, when, array

def apply_validation_rules(df):
    valid_statuses = ["Delivered", "Shipped", "Pending", "Cancelled", "Returned"]
    
    # Evaluate rules R1–R6; nulls in array() are automatically ignored by concat_ws
    reasons = array(
        when(col("order_id").isNull(), "R1: Missing order_id"),
        when(col("order_date").isNull(), "R2: Invalid or missing order_date"),
        when(col("quantity").isNull() | (col("quantity") <= 0), "R3: Quantity must be > 0"),
        when(col("unit_price").isNull() | (col("unit_price") <= 0), "R4: Unit price must be > 0"),
        when(
            col("customer_id").isNull() | col("product_name").isNull() | 
            col("category").isNull() | col("city").isNull() | col("payment_mode").isNull(),
            "R5: Missing required dimension field"
        ),
        when(col("order_status").isNull() | (~col("order_status").isin(valid_statuses)), "R6: Invalid order_status")
    )
    
    return df.withColumn("rejection_reason", concat_ws("; ", reasons))

@dp.table(
    name="silver_orders",
    comment="Validated and deduplicated e-commerce order records passing business rules R1-R6"
)
def silver_orders():
    business_columns = [
        "order_id", "order_date", "customer_id", "customer_name",
        "city", "product_name", "category", "quantity",
        "unit_price", "payment_mode", "order_status"
    ]
    df = spark.readStream.table("ecommerce_sdp.default.silver_orders_typed")
    validated_df = apply_validation_rules(df)
    return (
        validated_df
        .filter(col("rejection_reason") == "")
        .drop("rejection_reason")
        .dropDuplicates(business_columns)
    )

@dp.table(
    name="silver_orders_rejected",
    comment="Invalid e-commerce order records with rejection reasons"
)
def silver_orders_rejected():
    df = spark.readStream.table("ecommerce_sdp.default.silver_orders_typed")
    validated_df = apply_validation_rules(df)
    return validated_df.filter(col("rejection_reason") != "")
