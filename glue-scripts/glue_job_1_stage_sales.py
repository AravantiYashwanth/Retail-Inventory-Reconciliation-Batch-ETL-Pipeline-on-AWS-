import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F

# ============================================
# 🧩 Glue Job: Stage POS Sales Data
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
input_path = f"s3://{s3_bucket}/raw/pos_sales/date={processing_date}/"
output_path = f"s3://{s3_bucket}/staging/pos_sales/date={processing_date}/"

# ============================================
# 🧮 Step 1: Read Raw Data
# ============================================
raw_pos_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(input_path)
)

# ============================================
# 🧹 Step 2: Data Cleaning & Standardization
# ============================================
# ✅ Correct column names: 'sku' and 'quantity'
cleaned_df = (
    raw_pos_df
    .withColumn("sku", F.upper(F.trim(F.col("sku"))))
    .withColumn("quantity", F.col("quantity").cast("int"))
)

# ============================================
# 📊 Step 3: Aggregation
# ============================================
# Group by SKU and calculate total quantity sold for the day
aggregated_sales_df = (
    cleaned_df.groupBy("sku")
    .agg(F.sum("quantity").alias("total_quantity_sold"))
    .withColumn("date", F.lit(processing_date))
)

# ============================================
# 🧾 Step 4: Final Selection & Schema Alignment
# ============================================
final_df = aggregated_sales_df.select(
    F.col("date").cast("date").alias("date_key"),
    F.col("sku"),
    F.col("total_quantity_sold")
)

# ============================================
# 💾 Step 5: Write to Staging Zone
# ============================================
final_df.write.mode("overwrite").parquet(output_path)

# --- Commit the Job ---
job.commit()

# ============================================
# ✅ End of Script
# ============================================
