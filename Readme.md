# 🛠️ End-to-End ETL Pipeline with Airflow, S3, and Snowflake

This project demonstrates a **modern data engineering pipeline** using open-source and cloud-native tools.  
It covers the **Bronze → Silver → Gold** architecture pattern, incremental loads, and orchestration with Airflow.

---

## 📑 Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Pipeline Flow](#pipeline-flow)
- [Incremental Load Strategy](#incremental-load-strategy)
- [Airflow DAG](#airflow-dag)
- [How to Run](#how-to-run)
- [Next Improvements](#next-improvements)

---

## 📖 Overview
This ETL project simulates a retail orders dataset and processes it through multiple layers:

- **Bronze (Raw):** CSV files generated locally and uploaded to S3.  
- **Silver (Cleaned):** Data copied into Snowflake staging and merged into an incremental Silver table.  
- **Gold (Analytics):** Fact and Dimension tables created in Snowflake using incremental loads.  

The pipeline is fully orchestrated using **Apache Airflow**.

---

## 🏗️ Architecture

<img width="1682" height="806" alt="image" src="https://github.com/user-attachments/assets/bb4c6b53-cb8f-443e-a8c8-8c925e03a444" />


---

## ⚙️ Tech Stack

- **Apache Airflow** – Orchestration, scheduling, monitoring  
- **AWS S3** – Data lake storage (Bronze layer)  
- **Snowflake** – Data warehouse (Silver + Gold layers)  
- **Python** – Custom CSV generator & S3 uploader  
- **SQL (Snowflake)** – Data transformations & MERGE logic  
- *(Optional extensions)*: dbt, Great Expectations, Terraform, Prometheus/Grafana  

---

## 🔄 Pipeline Flow

1. **Data Generation (Python script):**
   - Simulates retail orders with timestamp, product, customer, and payment details.
   - Writes CSV files to local `/data` folder.

2. **Bronze Layer (S3):**
   - Airflow uploads CSV files from `/data` into an S3 bucket.
   - Raw data is stored for auditing and reprocessing.

3. **Silver Layer (Snowflake):**
   - `COPY INTO` loads new CSV batch into `stg_orders`.
   - Incremental `MERGE` updates `orders` table (only rows with greater `order_timestamp` are inserted/updated).

4. **Gold Layer (Snowflake):**
   - **Dimensions:** Customer, Product, Payment method.
   - **Fact:** Orders fact table referencing dimensions.
   - Incremental `MERGE` ensures no duplicate inserts.

---

## ⏩ Incremental Load Strategy

- **Silver:**  
  `MERGE` compares incoming `stg_orders` with existing `orders` using `transaction_id` and `order_timestamp`.  
  - New rows → inserted  
  - Updated rows (later timestamp) → updated  

- **Gold:**  
  Each dimension and fact table uses `MERGE` to avoid duplicates:  
  - Customer by `customer_id`  
  - Product by hash of `(product_name, category, price)`  
  - Payment by hash of `(payment_method)`  
  - Fact Orders by `transaction_id`  

---

## 📊 Airflow DAG


<img width="2438" height="878" alt="image" src="https://github.com/user-attachments/assets/b7dcf96b-d065-45a1-b930-dc5ad367c6cc" />


---

## 🚀 How to Run

1. **Clone repo and setup Airflow**
   ```bash
   git clone https://github.com/<your-username>/etl-airflow-snowflake.git
   cd etl-airflow-snowflake
   docker-compose up -d
2. ***Configure Airflow Connections
  ```bash
  aws_default → AWS S3 credentials
  snowflake_conn → Snowflake account, warehouse, DB, schema
```
  
4. ***Trigger DAG
```bash
Open Airflow UI → Trigger etl_dag_init
Watch tasks: generate_csv → upload_s3 → load_silver → load_gold
```

5. ***Verify in Snowflake
```bash
SELECT COUNT(*) FROM RETAIL_DW.SILVER.orders;
SELECT COUNT(*) FROM RETAIL_DW.GOLD.fact_orders;
```
