import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from datetime import datetime, timedelta
import boto3

# --------------------------------------
# 1. Initialization
# --------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# --------------------------------------
# 2. Parameters (Dynamic)
# --------------------------------------
args = getResolvedOptions(
    sys.argv, 
    ["JOB_NAME", "processing_date", "s3_bucket", "aws_region", "sns_topic_arn"]
)
job.init(args["JOB_NAME"], args)

processing_date_str = args["processing_date"]
s3_bucket = args["s3_bucket"]
aws_region = args["aws_region"]
sns_topic_arn = args["sns_topic_arn"]

# --------------------------------------
# 3. Date Calculations
# --------------------------------------
processing_date = datetime.strptime(processing_date_str, '%Y-%m-%d')
previous_date_str = (processing_date - timedelta(days=1)).strftime('%Y-%m-%d')

# --------------------------------------
# 4. S3 Paths
# --------------------------------------
staging_sales_path = f"s3://{s3_bucket}/staging/pos_sales/date={processing_date_str}/"
raw_inventory_path_today = f"s3://{s3_bucket}/raw/warehouse_inventory/date={processing_date_str}/"
raw_inventory_path_yesterday = f"s3://{s3_bucket}/raw/warehouse_inventory/date={previous_date_str}/"
dim_products_path = f"s3://{s3_bucket}/processed/dim_products/"
processed_output_path = f"s3://{s3_bucket}/processed/reconciled_inventory/date={processing_date_str}/"

# --------------------------------------
# 5. Load DataFrames
# --------------------------------------
daily_sales_df = spark.read.parquet(staging_sales_path)

opening_stock_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(raw_inventory_path_yesterday)
    .select(
        F.upper(F.trim(F.col("sku"))).alias("sku"),
        F.col("stock_on_hand").alias("opening_stock")
    )
)

actual_closing_stock_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(raw_inventory_path_today)
    .select(
        F.upper(F.trim(F.col("sku"))).alias("sku"),
        F.col("stock_on_hand").alias("actual_closing_stock")
    )
)

dim_products_df = spark.read.parquet(dim_products_path)

# --------------------------------------
# 6. Join and Reconcile
# --------------------------------------
inventory_df = opening_stock_df.join(actual_closing_stock_df, "sku", "full_outer")
reconciliation_df = inventory_df.join(daily_sales_df, "sku", "left")
reconciliation_with_names_df = reconciliation_df.join(
    dim_products_df.select("sku", "product_name"), "sku", "left"
)

# --------------------------------------
# 7. Calculate Expected Stock & Discrepancy
# --------------------------------------
final_df = (
    reconciliation_with_names_df
    .fillna(0, subset=['opening_stock', 'actual_closing_stock', 'total_quantity_sold'])
    .withColumn("expected_closing_stock", F.col("opening_stock") - F.col("total_quantity_sold"))
    .withColumn("discrepancy", F.col("actual_closing_stock") - F.col("expected_closing_stock"))
    .withColumn("date", F.lit(processing_date_str))
)

# --------------------------------------
# 8. Select and Rename Final Columns
# --------------------------------------
final_df_selected = final_df.select(
    F.col("date").cast("date").alias("date_key"),
    F.col("sku"),
    F.col("product_name"),
    F.col("opening_stock"),
    F.col("total_quantity_sold").alias("quantity_sold"),
    F.col("expected_closing_stock"),
    F.col("actual_closing_stock"),
    F.col("discrepancy").alias("discrepancy_amount")
)

# --------------------------------------
# 9. Write to Processed Zone
# --------------------------------------
final_df_selected.write.mode("overwrite").parquet(processed_output_path)

# --------------------------------------
# 10. Check for Discrepancies & Send Notification
# --------------------------------------
discrepancy_df = final_df_selected.filter(F.col("discrepancy_amount") != 0)
discrepancy_count = discrepancy_df.count()

if discrepancy_count > 0:
    print("Discrepancies found. Preparing to send SNS notification.")

    sns_client = boto3.client('sns', region_name=aws_region)
    discrepancy_examples = discrepancy_df.limit(5).collect()

    message = f"Inventory reconciliation for date {processing_date_str} found discrepancies.\n\n"
    message += f"Total items with discrepancies: {discrepancy_count}\n\n"
    message += "Example Discrepancies:\n"

    for row in discrepancy_examples:
        product_name = row['product_name'] if row['product_name'] else "N/A"
        message += f"- SKU: {row['sku']}, Product: {product_name}, Discrepancy: {row['discrepancy_amount']}\n"

    message += f"\nFull report available at: {processed_output_path}"
    subject = f"Alert: Inventory Discrepancy Found for {processing_date_str}"

    response = sns_client.publish(TopicArn=sns_topic_arn, Message=message, Subject=subject)
    print(f"SNS notification sent! Message ID: {response['MessageId']}")
else:
    print("No discrepancies found. No notification sent.")

# --------------------------------------
# 11. Commit Glue Job
# --------------------------------------
job.commit()
