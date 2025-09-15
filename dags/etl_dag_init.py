from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum

LOCAL_PATH = "/opt/airflow/data/transactions.csv"
BUCKET_NAME = "retail-transactions"

gold_incremental_sql = """

USE WAREHOUSE ETL_WH;
USE DATABASE RETAIL_DW;


MERGE INTO RETAIL_DW.GOLD.DIM_CUSTOMER tgt
USING (
    SELECT DISTINCT CUSTOMER_ID, CUSTOMER_NAME, STORE_LOCATION
    FROM RETAIL_DW.SILVER.ORDERS
) src
ON tgt.CUSTOMER_ID = src.CUSTOMER_ID
WHEN MATCHED AND (tgt.CUSTOMER_NAME <> src.CUSTOMER_NAME 
               OR tgt.STORE_LOCATION <> src.STORE_LOCATION) THEN
  UPDATE SET
    CUSTOMER_NAME = src.CUSTOMER_NAME,
    STORE_LOCATION = src.STORE_LOCATION
WHEN NOT MATCHED THEN
  INSERT (CUSTOMER_ID, CUSTOMER_NAME, STORE_LOCATION)
  VALUES (src.CUSTOMER_ID, src.CUSTOMER_NAME, src.STORE_LOCATION);



MERGE INTO RETAIL_DW.GOLD.DIM_PRODUCT tgt
USING (
    SELECT DISTINCT
        MD5(PRODUCT_NAME || PRODUCT_CATEGORY || PRICE) AS PRODUCT_ID,
        PRODUCT_NAME, PRODUCT_CATEGORY, PRICE
    FROM RETAIL_DW.SILVER.ORDERS
) src
ON tgt.PRODUCT_ID = src.PRODUCT_ID
WHEN NOT MATCHED THEN
  INSERT (PRODUCT_ID, PRODUCT_NAME, PRODUCT_CATEGORY, PRICE)
  VALUES (src.PRODUCT_ID, src.PRODUCT_NAME, src.PRODUCT_CATEGORY, src.PRICE);




MERGE INTO RETAIL_DW.GOLD.DIM_PAY tgt
USING (
    SELECT DISTINCT MD5(PAYMENT_METHOD) AS PAYMENT_ID, PAYMENT_METHOD
    FROM RETAIL_DW.SILVER.ORDERS
) src
ON tgt.PAYMENT_ID = src.PAYMENT_ID
WHEN NOT MATCHED THEN
  INSERT (PAYMENT_ID, PAYMENT_METHOD)
  VALUES (src.PAYMENT_ID, src.PAYMENT_METHOD);




MERGE INTO RETAIL_DW.GOLD.FACT_ORDERS tgt
USING (
    SELECT
        TRANSACTION_ID,
        ORDER_TIMESTAMP,
        CUSTOMER_ID,
        MD5(PRODUCT_NAME || PRODUCT_CATEGORY || PRICE) AS PRODUCT_ID,
        QUANTITY,
        TOTAL_AMOUNT,
        MD5(PAYMENT_METHOD) AS PAYMENT_ID
    FROM RETAIL_DW.SILVER.ORDERS
) src
ON tgt.TRANSACTION_ID = src.TRANSACTION_ID
WHEN NOT MATCHED THEN
  INSERT (TRANSACTION_ID, ORDER_TIMESTAMP, CUSTOMER_ID, PRODUCT_ID,
          QUANTITY, TOTAL_AMOUNT, PAYMENT_ID)
  VALUES (src.TRANSACTION_ID, src.ORDER_TIMESTAMP, src.CUSTOMER_ID,
          src.PRODUCT_ID, src.QUANTITY, src.TOTAL_AMOUNT, src.PAYMENT_ID);


          
"""



incremental_sql = """
USE WAREHOUSE ETL_WH;
USE DATABASE RETAIL_DW;

CREATE OR REPLACE TABLE RETAIL_DW.SILVER.stg_orders (
    transaction_id STRING,
    order_timestamp TIMESTAMP,
    customer_id STRING,
    customer_name STRING,
    product_name STRING,
    product_category STRING,
    quantity NUMBER,
    price FLOAT,
    total_amount FLOAT,
    payment_method STRING,
    store_location STRING
);

COPY INTO RETAIL_DW.SILVER.stg_orders
FROM @RETAIL_DW.SILVER.BRONZE_STAGE
FILE_FORMAT = (FORMAT_NAME = RETAIL_DW.SILVER.CSV_FF)
ON_ERROR = 'ABORT_STATEMENT';

MERGE INTO RETAIL_DW.SILVER.orders tgt
USING RETAIL_DW.SILVER.stg_orders src
ON tgt.transaction_id = src.transaction_id
WHEN MATCHED AND src.order_timestamp > tgt.order_timestamp THEN
    UPDATE SET
      customer_id = src.customer_id,
      customer_name = src.customer_name,
      product_name = src.product_name,
      product_category = src.product_category,
      quantity = src.quantity,
      price = src.price,
      total_amount = src.total_amount,
      payment_method = src.payment_method,
      store_location = src.store_location,
      order_timestamp = src.order_timestamp
WHEN NOT MATCHED THEN
    INSERT VALUES (src.transaction_id, src.order_timestamp, src.customer_id,
                   src.customer_name, src.product_name, src.product_category,
                   src.quantity, src.price, src.total_amount,
                   src.payment_method, src.store_location);
"""



with DAG(
    dag_id="etl_dag_init",
    start_date=pendulum.datetime(2025, 9, 14, tz="UTC"),
    schedule="None",
    catchup=False,
) as dag:

    # 1. generate CSV (simulated)
    t0 = BashOperator(
        task_id="install_dependencies",
        bash_command = "pip install -r /opt/airflow/scripts/requirements.txt"
    )
    t1 = BashOperator(
        task_id="generate_csv",
        bash_command="python /opt/airflow/scripts/csv_generator.py"
    )

    # 2. upload to S3 (simulated)
    t2 = LocalFilesystemToS3Operator(
        task_id="upload_s3",
        filename=LOCAL_PATH,
        dest_key='transactions.csv',
        dest_bucket=BUCKET_NAME,
        aws_conn_id="AWS",   # <-- this is the connection you created
        replace=True,
    )

    # 3. load into Snowflake Silver (simulated)
    t3 = SQLExecuteQueryOperator(
        task_id="load_incremental",
        sql=incremental_sql,
        conn_id="Snowflake",
        split_statements=True
    )

    # 4. transform to Gold (simulated)
    t4 = SQLExecuteQueryOperator(
        task_id="transform_gold",
        sql = gold_incremental_sql,
        conn_id="Snowflake",
        split_statements=True
    )

    # dependencies
    t0 >> t1 >> t2 >> t3 >> t4
