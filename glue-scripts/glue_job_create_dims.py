import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F

# ============================================
# 🧩 Glue Job: Create Product Dimension Table
# ============================================

# --- Initialization ---
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# --- Parameters (Dynamic) ---
args = getResolvedOptions(sys.argv, ["JOB_NAME", "processing_date"])
job.init(args["JOB_NAME"], args)

processing_date = args["processing_date"]
s3_bucket = "my-retail-etl-project"

# --- Define S3 Paths ---
input_path = f"s3://{s3_bucket}/raw/warehouse_inventory/date={processing_date}/"
output_path = f"s3://{s3_bucket}/processed/dim_products/"

# ============================================
# 📥 Step 1: Read Raw Inventory Data
# ============================================
inventory_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(input_path)
)

# ============================================
# 🧹 Step 2: Data Cleaning & Dimension Extraction
# ============================================
# ✅ Reads 'sku', 'product_name', and 'category' columns and ensures consistency.
dim_products_df = (
    inventory_df
    .withColumn("sku", F.upper(F.trim(F.col("sku"))))
    .withColumn("product_name", F.trim(F.col("product_name")))
    .withColumn("category", F.trim(F.col("category")))
    .select("sku", "product_name", "category")
    .dropDuplicates(["sku"])
)

# ============================================
# 💾 Step 3: Write to Processed Zone
# ============================================
# Coalesce to 1 file for dimension table readability and overwrite safely.
dim_products_df.coalesce(1).write.mode("overwrite").parquet(output_path)

# --- Commit the Job ---
job.commit()

# ============================================
# ✅ End of Script
# ============================================
