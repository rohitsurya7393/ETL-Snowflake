USE WAREHOUSE ETL_WH;


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


select * from RETAIL_DW.SILVER.STG_ORDERS;


-- Merging, if similar transaction id comes we check timestamp and update only if the time stamp is greater
-- if we get new transaction_id then we update without checking anything
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



select transaction_id,count(*) from RETAIL_DW.SILVER.ORDERS
group by transaction_id having count(*) > 0;

