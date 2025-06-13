-- Create stage table
    CREATE OR REPLACE TABLE tdp.test.tdp_all_sales_stage (
        data_type VARCHAR,
        source_system VARCHAR,
        source_system_id VARCHAR,
        unified_store_id INTEGER,
        order_id VARCHAR,
        order_item_id VARCHAR,
        order_created_datetime TIMESTAMP,
        order_completed_datetime TIMESTAMP,
        order_created_datetime_utc TIMESTAMP,
        order_completed_datetime_utc TIMESTAMP,
        accounting_date DATE,
        order_created_by VARCHAR,
        order_completed_by VARCHAR,
        order_fulfillment_method VARCHAR,
        mrw_flag VARCHAR,
        ecomm_order_id VARCHAR,
        customer_id VARCHAR,
        unified_customer_id NUMBER,
        product_id VARCHAR,
        unified_product_id NUMBER,
        batch VARCHAR,
        order_type VARCHAR,
        invoice_type VARCHAR,
        gross_sales FLOAT,
        discount_amount FLOAT,
        net_sales FLOAT,
        quantity FLOAT,
        cogs FLOAT,
        cost FLOAT,
        pull_datetime TIMESTAMP,
        pull_datetime_utc TIMESTAMP,
        glue_pull_datetime_utc TIMESTAMP
    );
    
    -- Copy directly from Parquet files
    COPY INTO tdp.test.tdp_all_sales_stage
    FROM @tdp.test.tdp_bronze_stage/tdp_all_sales_af/
    FILE_FORMAT = (
        TYPE = PARQUET
        BINARY_AS_TEXT = FALSE
        TRIM_SPACE = TRUE
    )
    PATTERN = '.*[.]parquet'
    ON_ERROR = CONTINUE
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
    
    -- Verify data loaded correctly
    SELECT 
        COUNT(*) as total_rows,
        COUNT_IF(data_type IS NOT NULL) as non_null_data_type,
        COUNT_IF(order_id IS NOT NULL) as non_null_order_id,
        COUNT_IF(gross_sales IS NOT NULL) as non_null_gross_sales
    FROM tdp.test.tdp_all_sales_stage;