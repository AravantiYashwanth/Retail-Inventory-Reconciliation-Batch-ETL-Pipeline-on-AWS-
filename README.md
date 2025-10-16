
# 📦 Retail Inventory Reconciliation (Batch ETL Pipeline on AWS)

![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-FDEE21?style=for-the-badge&logo=apachespark&logoColor=black)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![Redshift](https://img.shields.io/badge/Amazon%20Redshift-8C4282?style=for-the-badge&logo=amazon-redshift&logoColor=white)
![Glue](https://img.shields.io/badge/AWS%20Glue-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)
![QuickSight](https://img.shields.io/badge/Amazon%20QuickSight-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)

---

## 📘 Table of Contents
- [🚀 Overview](#-overview)
- [🎯 Business Goal](#-business-goal)
- [🏗️ Solution Architecture](#️-solution-architecture)
- [⚙️ Technologies Used](#️-technologies-used)
- [🧠 Key Features & Logic](#-key-features--logic)
- [📂 Project Structure](#-project-structure)
- [📊 Redshift Schema](#-redshift-schema)
- [💡 Learnings & Outcomes](#-learnings--outcomes)
- [🏁 Future Enhancements](#-future-enhancements)
- [👨‍💻 Author](#-author)

---

## 🚀 Overview

This project implements a **scalable batch ETL pipeline** on **AWS** to automate the process of **retail inventory reconciliation**.  
The pipeline ingests **daily Point-of-Sale (POS)** and **warehouse inventory data**, performs complex **transformations**, and loads curated data into **Amazon Redshift Serverless** for analytics.

The result: a complete end-to-end data engineering solution enabling **Amazon QuickSight** dashboards for:
- Real-time sales performance  
- Inventory tracking  
- Discrepancy detection between expected and actual stock  

---

## 🎯 Business Goal

The primary objective is to empower business teams with **daily refreshed BI dashboards** in **Amazon QuickSight** to monitor:

- 🧾 **Sales Performance** – Track daily sales and trends  
- 📊 **Product Trends** – Identify top-selling products and categories  
- 🏬 **Inventory Reconciliation** – Detect mismatches between expected and actual stock  

---

## 🏗️ Solution Architecture

The pipeline follows a **multi-layered Medallion-style data flow** built entirely on **AWS serverless components**.

```text
Amazon S3 (Raw Zone)
      │
      ▼
AWS Glue (ETL Jobs)
 ├── Job 1: Stage Sales Data
 ├── Job 2: Build Product Dimension
 └── Job 3: Reconcile Inventory
      │
      ▼
Amazon Redshift (fact & dim tables)
      │
      ▼
Amazon QuickSight Dashboards
````

### 🔄 Data Flow Summary

1. **Ingestion** – Daily POS and inventory CSVs land in the **raw zone** of Amazon S3.
2. **Staging** – **Glue Job 1** aggregates sales by product and stores in Parquet format.
3. **Transformation** – **Glue Job 2 & 3** build product dimensions and calculate discrepancies:

   * `expected_closing_stock = opening_stock - total_quantity_sold`
   * `discrepancy = actual_closing_stock - expected_closing_stock`
4. **Alerting** – If discrepancies exist, **Amazon SNS** sends automated notifications.
5. **Data Warehousing** – Clean data is loaded into **Amazon Redshift Serverless**.
6. **Orchestration** – Entire workflow managed via **Apache Airflow DAG**.
7. **Visualization** – Dashboards in **Amazon QuickSight** provide insights to stakeholders.

---

## ⚙️ Technologies Used

| Category              | Technologies               | Purpose                                             |
| --------------------- | -------------------------- | --------------------------------------------------- |
| **Data Lake Storage** | Amazon S3                  | Central storage for raw, staged, and processed data |
| **ETL Processing**    | AWS Glue, PySpark          | Serverless transformation and reconciliation        |
| **Data Warehouse**    | Amazon Redshift Serverless | Analytics-ready schema for BI dashboards            |
| **Orchestration**     | Apache Airflow             | Schedule and manage dependent workflows             |
| **Alerting**          | Amazon SNS                 | Notify stakeholders of stock discrepancies          |
| **BI Visualization**  | Amazon QuickSight          | Interactive dashboards for business teams           |
| **IAM Security**      | AWS IAM                    | Manage secure service-to-service access             |

---

## 🧠 Key Features & Logic

### 🧩 Core Reconciliation Logic (PySpark)

```python
final_df = source_data.withColumn(
    "expected_closing_stock", (F.col("opening_stock") - F.col("total_quantity_sold"))
).withColumn(
    "discrepancy", (F.col("actual_closing_stock") - F.col("expected_closing_stock"))
)
```

### 🏗️ Additional Features

* **Data Partitioning**: S3 data is partitioned by date (`date=YYYY-MM-DD`) for fast Athena/Redshift queries.
* **Idempotent Loads**: Airflow ensures re-runs overwrite data for the same processing date.
* **Scalability**: Fully serverless components scale automatically based on workload.
* **Dependency Management**: Airflow DAG enforces task execution order for consistent data freshness.
* **Error Handling**: SNS notifications on job failure or data mismatches.

---

## 📂 Project Structure

```text
retail-inventory-reconciliation/
├── dags/
│   └── retail_pipeline_dag.py          # Airflow DAG for orchestration
│
├── glue-scripts/
│   ├── glue_job_1_stage_sales.py       # Aggregates raw POS sales
│   ├── glue_job_create_dims.py         # Builds product dimension table
│   └── glue_job_2_reconcile.py         # Performs inventory reconciliation
│
├── sql/
│   └── create_redshift_tables.sql      # DDL for Redshift schema
│
└── README.md
```

---

## 📊 Redshift Schema

### 🧱 Dimension: `dim_products`

```sql
CREATE TABLE IF NOT EXISTS dim_products (
    sku VARCHAR(50) NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    DISTSTYLE ALL,
    SORTKEY (sku)
);
```

### 📈 Fact: `fact_daily_sales`

```sql
CREATE TABLE IF NOT EXISTS fact_daily_sales (
    date_key DATE,
    sku VARCHAR(50),
    total_quantity_sold BIGINT,
    DISTKEY (sku),
    SORTKEY (date_key)
);
```

### 🧮 Fact: `fact_inventory_reconciliation`

```sql
CREATE TABLE IF NOT EXISTS fact_inventory_reconciliation (
    date_key DATE,
    sku VARCHAR(50),
    product_name VARCHAR(255),
    opening_stock INT,
    quantity_sold BIGINT,
    expected_closing_stock BIGINT,
    actual_closing_stock INT,
    discrepancy_amount BIGINT,
    DISTKEY(sku),
    SORTKEY(date_key)
);
```

---

## 💡 Learnings & Outcomes

✅ Built a **modular, serverless data pipeline** using modern AWS tools
✅ Mastered **PySpark transformations** in AWS Glue
✅ Implemented **Airflow orchestration** with clear task dependencies
✅ Designed **data models (Star Schema)** optimized for analytics
✅ Delivered **automated reconciliation & alerting** workflows
✅ Enhanced understanding of **data warehouse optimization** in Redshift

---

## 🏁 Future Enhancements

* Integrate **AWS Lambda** for lightweight validation before ETL
* Add **Athena/Glue crawlers** for metadata catalog automation
* Expand alerting via **Slack or Microsoft Teams integrations**

---

## 👨‍💻 Author

**A. Yashwanth**
🎓 *Electronics & Communication Engineer* | ☁️ *Cloud & Data Enthusiast* | 💻 *AWS + Python Developer*

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?style=flat\&logo=linkedin)](YOUR_LINKEDIN_URL)
[![GitHub](https://img.shields.io/badge/GitHub-Portfolio-black?style=flat\&logo=github)](YOUR_GITHUB_URL)


⭐ *If you found this project helpful, consider giving it a star on GitHub!*


- `www.linkedin.com/in/yashwantharavanti` 



