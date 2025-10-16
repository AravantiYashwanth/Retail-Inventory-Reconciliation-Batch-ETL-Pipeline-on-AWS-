from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.param import Param # Import the Param class

# --- Configuration ---
AWS_CONN_ID = "aws_default"
REDSHIFT_CONN_ID = "redshift_default"
IAM_ROLE_ARN = "arn:aws:iam::487615743519:role/RedshiftS3ReadRole"
S3_BUCKET = "my-retail-etl-project"
SNS_TOPIC_ARN = "arn:aws:sns:ap-south-1:487615743519:inventory-discrepancy-alerts"
AWS_REGION = "ap-south-1"

# --- MODIFICATION: Use `params.processing_date` instead of `ds` ---
# This makes the SQL commands use the date provided by the user in the UI.
SQL_DELETE_FACT_DAILY_SALES = "DELETE FROM fact_daily_sales WHERE date_key = '{{ params.processing_date }}';"
SQL_COPY_FACT_DAILY_SALES = f"""
    COPY fact_daily_sales
    FROM 's3://{S3_BUCKET}/staging/pos_sales/date={{ params.processing_date }}/'
    IAM_ROLE '{IAM_ROLE_ARN}'
    FORMAT AS PARQUET;
"""

SQL_TRUNCATE_DIM_PRODUCTS = "TRUNCATE dim_products;"
SQL_COPY_DIM_PRODUCTS = f"""
    COPY dim_products
    FROM 's3://{S3_BUCKET}/processed/dim_products/'
    IAM_ROLE '{IAM_ROLE_ARN}'
    FORMAT AS PARQUET;
"""

SQL_DELETE_FACT_INVENTORY_RECONCILIATION = "DELETE FROM fact_inventory_reconciliation WHERE date_key = '{{ params.processing_date }}';"
SQL_COPY_FACT_INVENTORY_RECONCILIATION = f"""
    COPY fact_inventory_reconciliation
    FROM 's3://{S3_BUCKET}/processed/reconciled_inventory/date={{ params.processing_date }}/'
    IAM_ROLE '{IAM_ROLE_ARN}'
    FORMAT AS PARQUET;
"""

with DAG(
    dag_id="manual_glue_redshift_retail_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    doc_md="""
    ## Manual Retail Data Pipeline
    Orchestrates AWS Glue jobs and Redshift loads. 
    Click the play button and enter a `processing_date` to run.
    """,
    # --- MODIFICATION: Define a parameter for the user to enter ---
    params={
        "processing_date": Param(
            "2025-10-12", # This is the default value that will appear in the box
            type="string",
            title="Processing Date",
            description="The date for which to run the pipeline (format: YYYY-MM-DD).",
        )
    },
    tags=["glue", "redshift", "manual-trigger"],
) as dag:
    
    start = EmptyOperator(task_id="start")

    # --- MODIFICATION: Use `params.processing_date` in script_args ---
    glue_job_stage_pos_sales = GlueJobOperator(task_id="glue_job_stage_pos_sales", job_name="glue-job-1-stage-pos-sales", aws_conn_id=AWS_CONN_ID, script_args={"--processing_date": "{{ params.processing_date }}"})
    glue_job_create_dim_products = GlueJobOperator(task_id="glue_job_create_dim_products", job_name="glue-job-create-dim-products", aws_conn_id=AWS_CONN_ID, script_args={"--processing_date": "{{ params.processing_date }}"})
    glue_job_reconcile_inventory = GlueJobOperator(
        task_id="glue_job_reconcile_inventory", 
        job_name="glue-job-2-reconcile-inventory", 
        aws_conn_id=AWS_CONN_ID, 
        script_args={
            "--processing_date": "{{ params.processing_date }}", 
            "--s3_bucket": S3_BUCKET, 
            "--aws_region": AWS_REGION, 
            "--sns_topic_arn": SNS_TOPIC_ARN
        }
    )

    # Redshift tasks now use the single-statement SQL variables
    delete_fact_daily_sales = SQLExecuteQueryOperator(task_id="delete_fact_daily_sales", conn_id=REDSHIFT_CONN_ID, sql=SQL_DELETE_FACT_DAILY_SALES)
    load_fact_daily_sales = SQLExecuteQueryOperator(task_id="load_fact_daily_sales_to_redshift", conn_id=REDSHIFT_CONN_ID, sql=SQL_COPY_FACT_DAILY_SALES)

    truncate_dim_products = SQLExecuteQueryOperator(task_id="truncate_dim_products", conn_id=REDSHIFT_CONN_ID, sql=SQL_TRUNCATE_DIM_PRODUCTS)
    load_dim_products = SQLExecuteQueryOperator(task_id="load_dim_products_to_redshift", conn_id=REDSHIFT_CONN_ID, sql=SQL_COPY_DIM_PRODUCTS)
    
    delete_fact_inventory_reconciliation = SQLExecuteQueryOperator(task_id="delete_fact_inventory_reconciliation", conn_id=REDSHIFT_CONN_ID, sql=SQL_DELETE_FACT_INVENTORY_RECONCILIATION)
    load_fact_inventory_reconciliation = SQLExecuteQueryOperator(task_id="load_fact_inventory_reconciliation_to_redshift", conn_id=REDSHIFT_CONN_ID, sql=SQL_COPY_FACT_INVENTORY_RECONCILIATION)

    end = EmptyOperator(task_id="end")


    start >> [glue_job_stage_pos_sales, glue_job_create_dim_products]
    glue_job_stage_pos_sales >> delete_fact_daily_sales >> load_fact_daily_sales
    glue_job_create_dim_products >> truncate_dim_products >> load_dim_products
    [glue_job_stage_pos_sales, glue_job_create_dim_products] >> glue_job_reconcile_inventory
    glue_job_reconcile_inventory >> delete_fact_inventory_reconciliation >> load_fact_inventory_reconciliation
    [load_fact_daily_sales, load_dim_products, load_fact_inventory_reconciliation] >> end

