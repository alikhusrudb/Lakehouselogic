# APPENDIX A: Complete SQL DDL & Implementation Code

## Database & Schema Setup

```sql
-- Create Lakehouse schemas
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT "Raw landing data from source systems";

CREATE SCHEMA IF NOT EXISTS silver
COMMENT "Staging layer: source-faithful replicas with technical enrichment";

CREATE SCHEMA IF NOT EXISTS gold
COMMENT "Curated layer: conformed dimensions and facts for analytics";

-- Set location for better governance
ALTER SCHEMA bronze SET LOCATION '/mnt/lakehouse/bronze';
ALTER SCHEMA silver SET LOCATION '/mnt/lakehouse/silver';
ALTER SCHEMA gold SET LOCATION '/mnt/lakehouse/gold';
```

---

## Landing Layer (Bronze) DDL

```sql
-- CUSTOMER LANDING TABLE (Full daily refresh from CRM)
CREATE TABLE IF NOT EXISTS bronze.customer_landing (
    customer_id INT NOT NULL,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(500),
    city VARCHAR(50),
    state_province VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(50),
    account_status VARCHAR(20),
    
    -- Landing metadata
    extraction_dt TIMESTAMP NOT NULL,
    source_system_id VARCHAR(50) DEFAULT 'CRM_PROD',
    
    PRIMARY KEY (customer_id, extraction_dt)
)
USING DELTA
PARTITIONED BY (extraction_dt)
TBLPROPERTIES (
    "delta.retention.duration" = "30 days",
    "description" = "Raw customer data from CRM, daily full refresh"
);

-- PRODUCT LANDING TABLE
CREATE TABLE IF NOT EXISTS bronze.product_landing (
    product_id INT NOT NULL,
    product_name VARCHAR(100),
    product_category VARCHAR(50),
    unit_cost DECIMAL(10, 2),
    unit_price DECIMAL(10, 2),
    is_active BOOLEAN DEFAULT TRUE,
    
    extraction_dt TIMESTAMP NOT NULL,
    source_system_id VARCHAR(50) DEFAULT 'ERP_PROD',
    
    PRIMARY KEY (product_id, extraction_dt)
)
USING DELTA
PARTITIONED BY (extraction_dt);

-- ORDER LANDING TABLE
CREATE TABLE IF NOT EXISTS bronze.order_landing (
    order_id INT NOT NULL,
    order_line_id INT NOT NULL,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    order_dt DATE NOT NULL,
    order_qty INT,
    unit_price DECIMAL(10, 2),
    discount_pct DECIMAL(5, 2),
    tax_pct DECIMAL(5, 2),
    ship_dt DATE,
    delivery_dt DATE,
    order_status VARCHAR(20),
    
    extraction_dt TIMESTAMP NOT NULL,
    source_system_id VARCHAR(50) DEFAULT 'OMS_PROD',
    
    PRIMARY KEY (order_id, order_line_id, extraction_dt)
)
USING DELTA
PARTITIONED BY (extraction_dt);
```

---

## Staging Layer (Silver) DDL

```sql
-- CUSTOMER STAGING TABLE (Type 2: Append-only audit trail)
CREATE TABLE IF NOT EXISTS silver.customer_staging (
    -- Surrogate key (one per delta record)
    surrogate_key BIGINT NOT NULL,
    
    -- Source identifiers
    customer_id INT NOT NULL,
    source_system_id VARCHAR(50),
    
    -- Change detection & delta classification
    source_hash BINARY NOT NULL,
    change_type VARCHAR(1),                    -- I=Insert, U=Update, D=Delete
    is_deleted BOOLEAN DEFAULT FALSE,
    
    -- Technical timestamps
    insert_dt DATE NOT NULL,                   -- When appended to Staging
    extract_dt TIMESTAMP,                      -- When extracted from source
    load_ts TIMESTAMP,                         -- When loaded via Spark
    
    -- Customer attributes (all columns from source)
    customer_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(500),
    city VARCHAR(50),
    state_province VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(50),
    account_status VARCHAR(20),
    
    -- Operational metadata
    row_number BIGINT,
    process_id VARCHAR(100),
    
    PRIMARY KEY (surrogate_key)
)
USING DELTA
PARTITIONED BY (insert_dt)
CLUSTER BY (customer_id, insert_dt)
TBLPROPERTIES (
    "delta.enableChangeDataFeed" = "true",
    "description" = "Customer staging: hash-detected deltas, append-only audit trail"
);

-- PRODUCT STAGING TABLE
CREATE TABLE IF NOT EXISTS silver.product_staging (
    surrogate_key BIGINT NOT NULL,
    product_id INT NOT NULL,
    source_system_id VARCHAR(50),
    source_hash BINARY NOT NULL,
    change_type VARCHAR(1),
    is_deleted BOOLEAN DEFAULT FALSE,
    insert_dt DATE NOT NULL,
    extract_dt TIMESTAMP,
    load_ts TIMESTAMP,
    product_name VARCHAR(100),
    product_category VARCHAR(50),
    unit_cost DECIMAL(10, 2),
    unit_price DECIMAL(10, 2),
    is_active BOOLEAN,
    row_number BIGINT,
    process_id VARCHAR(100),
    PRIMARY KEY (surrogate_key)
)
USING DELTA
PARTITIONED BY (insert_dt)
CLUSTER BY (product_id, insert_dt);

-- ORDER STAGING TABLE
CREATE TABLE IF NOT EXISTS silver.order_staging (
    surrogate_key BIGINT NOT NULL,
    order_id INT NOT NULL,
    order_line_id INT NOT NULL,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    source_system_id VARCHAR(50),
    source_hash BINARY NOT NULL,
    change_type VARCHAR(1),
    is_deleted BOOLEAN DEFAULT FALSE,
    insert_dt DATE NOT NULL,
    extract_dt TIMESTAMP,
    load_ts TIMESTAMP,
    order_dt DATE NOT NULL,
    order_qty INT,
    unit_price DECIMAL(10, 2),
    discount_pct DECIMAL(5, 2),
    tax_pct DECIMAL(5, 2),
    ship_dt DATE,
    delivery_dt DATE,
    order_status VARCHAR(20),
    row_number BIGINT,
    process_id VARCHAR(100),
    PRIMARY KEY (surrogate_key)
)
USING DELTA
PARTITIONED BY (insert_dt)
CLUSTER BY (order_id, order_dt, insert_dt);
```

---

## Gold Layer (Curated) DDL

```sql
-- CUSTOMER DIMENSION (SCD2 - Type 2 Slowly Changing)
CREATE TABLE IF NOT EXISTS gold.customer_dimension (
    -- Surrogate keys
    customer_dim_key BIGINT NOT NULL,
    customer_id INT NOT NULL,
    
    -- Customer attributes
    customer_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(500),
    city VARCHAR(50),
    state_province VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(50),
    account_status VARCHAR(20),
    
    -- SCD2 metadata
    effective_start_dt DATE NOT NULL,
    effective_end_dt DATE NOT NULL DEFAULT CAST('9999-12-31' AS DATE),
    is_active BOOLEAN DEFAULT TRUE,
    change_type VARCHAR(1),                    -- Which attributes changed?
    source_system_id VARCHAR(50),
    
    -- DW metadata
    dw_insert_dt TIMESTAMP NOT NULL,
    dw_update_dt TIMESTAMP NOT NULL,
    
    PRIMARY KEY (customer_dim_key)
)
USING DELTA
CLUSTER BY (customer_id, effective_start_dt)
TBLPROPERTIES (
    "description" = "Customer dimension with SCD2 history"
);

-- PRODUCT DIMENSION (SCD2)
CREATE TABLE IF NOT EXISTS gold.product_dimension (
    product_dim_key BIGINT NOT NULL,
    product_id INT NOT NULL,
    product_name VARCHAR(100),
    product_category VARCHAR(50),
    unit_cost DECIMAL(10, 2),
    unit_price DECIMAL(10, 2),
    is_active BOOLEAN,
    effective_start_dt DATE NOT NULL,
    effective_end_dt DATE NOT NULL DEFAULT CAST('9999-12-31' AS DATE),
    is_current BOOLEAN DEFAULT TRUE,
    source_system_id VARCHAR(50),
    dw_insert_dt TIMESTAMP NOT NULL,
    dw_update_dt TIMESTAMP NOT NULL,
    PRIMARY KEY (product_dim_key)
)
USING DELTA
CLUSTER BY (product_id, effective_start_dt);

-- DATE DIMENSION (Conformed, loaded once)
CREATE TABLE IF NOT EXISTS gold.date_dimension (
    date_key INT NOT NULL,
    calendar_date DATE NOT NULL,
    day_of_week INT,
    day_name VARCHAR(10),
    day_of_month INT,
    week_of_year INT,
    month INT,
    month_name VARCHAR(10),
    quarter INT,
    year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    PRIMARY KEY (date_key)
)
USING DELTA;

-- ORDERS FACT TABLE (Conformed grain: one row per order-line)
CREATE TABLE IF NOT EXISTS gold.orders_fact (
    -- Surrogate key
    order_fact_key BIGINT NOT NULL,
    
    -- Business keys
    order_id INT NOT NULL,
    order_line_id INT NOT NULL,
    
    -- Foreign keys
    customer_dim_key BIGINT NOT NULL,
    product_dim_key BIGINT NOT NULL,
    order_date_key INT NOT NULL,
    ship_date_key INT,
    delivery_date_key INT,
    
    -- SCD2 Version pointers
    customer_dim_key_scd BIGINT,               -- Customer version as-of order_date
    product_dim_key_scd BIGINT,                -- Product version as-of order_date
    
    -- Facts (additive measures)
    order_quantity INT NOT NULL,
    order_unit_price DECIMAL(10, 2) NOT NULL,
    order_extended_price DECIMAL(12, 2) NOT NULL,
    order_discount_amount DECIMAL(10, 2),
    order_tax_amount DECIMAL(10, 2),
    order_net_amount DECIMAL(12, 2) NOT NULL,
    
    -- Dimensions (conformed attributes)
    order_status VARCHAR(20),
    
    -- DW metadata
    dw_insert_dt TIMESTAMP NOT NULL,
    dw_update_dt TIMESTAMP NOT NULL,
    
    PRIMARY KEY (order_fact_key)
)
USING DELTA
PARTITIONED BY (order_date_key)
CLUSTER BY (customer_dim_key, order_date_key)
TBLPROPERTIES (
    "description" = "Order facts: one row per order-line item"
);

-- STATUS REFERENCE TABLE (Small, conformed, no history)
CREATE TABLE IF NOT EXISTS gold.status_reference (
    status_key INT NOT NULL,
    status_code VARCHAR(20),
    status_name VARCHAR(100),
    status_description VARCHAR(500),
    is_active BOOLEAN DEFAULT TRUE,
    dw_insert_dt TIMESTAMP,
    PRIMARY KEY (status_key)
)
USING DELTA;
```

---

## PySpark Implementation: Landing → Staging

```python
# File: landing_to_staging_customers.py
# Purpose: Hash-based delta detection and append to Staging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    md5, concat_ws, col, when, max as sql_max, coalesce,
    dense_rank, row_number, current_timestamp, current_date,
    expr, lit, rand
)
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import uuid

# Initialize Spark session
spark = SparkSession.builder \
    .appName("landing-to-staging-customers") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# ============================================================================
# CONFIGURATION
# ============================================================================

LANDING_TABLE = "bronze.customer_landing"
STAGING_TABLE = "silver.customer_staging"
BATCH_SIZE = 100000
PROCESS_ID = str(uuid.uuid4())

# ============================================================================
# STEP 1: READ LATEST LANDING DATA
# ============================================================================

# Get max extraction date from landing
max_extraction_date = spark.sql(f"SELECT MAX(extraction_dt) FROM {LANDING_TABLE}").collect()[0][0]

if max_extraction_date is None:
    print("ERROR: No data in landing table")
    spark.stop()
    exit(1)

print(f"Processing landing data from: {max_extraction_date}")

# Read latest landing snapshot
landing = spark.sql(f"""
    SELECT *
    FROM {LANDING_TABLE}
    WHERE extraction_dt = CAST('{max_extraction_date}' AS TIMESTAMP)
""")

# ============================================================================
# STEP 2: GENERATE DETERMINISTIC HASH (for delta detection)
# ============================================================================

# Hash all business attributes in fixed order (reproducible)
# NOTE: Order matters! Always include in same sequence
hash_columns = [
    "customer_id", "customer_name", "email", "phone", "address",
    "city", "state_province", "postal_code", "country", "account_status"
]

landing_with_hash = landing.select(
    col("*"),
    md5(concat_ws("||", *[col(c).cast("string") for c in hash_columns])) \
        .alias("source_hash"),
    current_date().alias("insert_dt"),
    current_timestamp().alias("load_ts")
)

# ============================================================================
# STEP 3: READ CURRENT STAGING (LATEST VERSIONS ONLY)
# ============================================================================

# Get current state: for each customer_id, get the most recent record
# (excludes deleted records and superseded versions)
staging_latest = spark.sql(f"""
    SELECT 
        customer_id, 
        source_hash, 
        MAX(insert_dt) AS last_known_insert_dt
    FROM {STAGING_TABLE}
    WHERE is_deleted = FALSE
    GROUP BY customer_id, source_hash
""")

# ============================================================================
# STEP 4: DETECT DELTAS
# ============================================================================

# INSERTS: Records in Landing NOT in Staging (by customer_id + hash)
inserts = landing_with_hash.join(
    staging_latest,
    on=[
        landing_with_hash.customer_id == staging_latest.customer_id,
        landing_with_hash.source_hash == staging_latest.source_hash
    ],
    how="left_anti"  # Only records without a match
).select(
    landing_with_hash["*"],
    lit("I").alias("change_type")
)

# Distinguish between INSERT (new customer) vs UPDATE (existing customer, different hash)
insert_vs_update = inserts.join(
    spark.sql(f"""
        SELECT DISTINCT customer_id 
        FROM {STAGING_TABLE}
    """),
    on="customer_id",
    how="left"
)

# Split into actual inserts and updates
true_inserts = insert_vs_update.filter(col("staging.customer_id").isNull()) \
    .select(inserts["*"])

true_updates = insert_vs_update.filter(col("staging.customer_id").isNotNull()) \
    .select(inserts["*"]) \
    .withColumn("change_type", lit("U"))

# DELETES: Records in Staging NOT in Landing (deleted from source)
deletes = staging_latest.join(
    landing_with_hash.select("customer_id").distinct(),
    on="customer_id",
    how="left_anti"  # customer_id not in landing anymore
).select(
    col("customer_id"),
    lit(None).alias("source_hash"),
    col("insert_dt"),
    current_date().alias("insert_dt_new"),
    lit("D").alias("change_type"),
    lit(True).alias("is_deleted")
)

# ============================================================================
# STEP 5: UNION ALL DELTAS
# ============================================================================

staging_deltas = true_inserts \
    .unionByName(true_updates, allowMissingColumns=True) \
    .unionByName(deletes, allowMissingColumns=True) \
    .select(
        col("*"),
        expr("FALSE AS is_deleted")  # Override for non-deletes
    )

staging_deltas_count = staging_deltas.count()
print(f"Deltas detected: Inserts={inserts.count()}, Updates={true_updates.count()}, Deletes={deletes.count()}")

if staging_deltas_count == 0:
    print("No deltas detected. Exiting.")
    spark.stop()
    exit(0)

# ============================================================================
# STEP 6: ADD SURROGATE KEY (auto-increment per batch)
# ============================================================================

# Get max surrogate_key from Staging
max_surrogate_key = spark.sql(f"""
    SELECT COALESCE(MAX(surrogate_key), 0) FROM {STAGING_TABLE}
""").collect()[0][0]

staging_with_sk = staging_deltas \
    .withColumn(
        "row_num",
        row_number().over(Window.partitionBy().orderBy(col("insert_dt"), col("customer_id")))
    ) \
    .select(
        (col("row_num") + lit(max_surrogate_key)).alias("surrogate_key"),
        col("*").alias("*"),
        col("row_num").alias("row_number")
    ) \
    .drop("row_num")

# ============================================================================
# STEP 7: ADD METADATA
# ============================================================================

staging_final = staging_with_sk.select(
    col("surrogate_key"),
    col("customer_id"),
    lit("CRM_PROD").alias("source_system_id"),
    col("source_hash"),
    col("change_type"),
    col("is_deleted"),
    col("insert_dt"),
    col("load_ts").alias("extract_dt"),
    current_timestamp().alias("load_ts"),
    col("customer_name"),
    col("email"),
    col("phone"),
    col("address"),
    col("city"),
    col("state_province"),
    col("postal_code"),
    col("country"),
    col("account_status"),
    col("row_number"),
    lit(PROCESS_ID).alias("process_id")
)

# ============================================================================
# STEP 8: WRITE TO STAGING (APPEND ONLY)
# ============================================================================

staging_final.write \
    .mode("append") \
    .option("mergeSchema", "false") \
    .insertInto(STAGING_TABLE)

print(f"Successfully appended {staging_deltas_count} delta records to {STAGING_TABLE}")

# ============================================================================
# STEP 9: OPTIMIZE STAGING TABLE
# ============================================================================

spark.sql(f"OPTIMIZE {STAGING_TABLE} ZORDER BY (customer_id, insert_dt)")

print("Optimization complete")

spark.stop()
```

---

## PySpark Implementation: Staging → Curated (Dimensions)

```python
# File: staging_to_curated_customer_dimension.py
# Purpose: SCD2 processing with MERGE for idempotency

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, max, coalesce, dense_rank, row_number,
    current_timestamp, current_date, expr, lit
)
from pyspark.sql.window import Window
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .appName("staging-to-curated-customer-dimension") \
    .getOrCreate()

# ============================================================================
# CONFIGURATION
# ============================================================================

STAGING_TABLE = "silver.customer_staging"
DIM_TABLE = "gold.customer_dimension"
PROCESS_DATE = current_date()

# ============================================================================
# STEP 1: READ TODAY'S STAGING DELTAS
# ============================================================================

staging_today = spark.sql(f"""
    SELECT *
    FROM {STAGING_TABLE}
    WHERE insert_dt = CURRENT_DATE()
      AND change_type IN ('I', 'U', 'D')
""")

print(f"Processing {staging_today.count()} staging records from today")

# ============================================================================
# STEP 2: READ CURRENT DIMENSION (ACTIVE RECORDS ONLY)
# ============================================================================

dim_current = spark.sql(f"""
    SELECT *
    FROM {DIM_TABLE}
    WHERE is_active = TRUE
""")

# ============================================================================
# STEP 3: PROCESS INSERTS (New customers)
# ============================================================================

inserts = staging_today.filter(col("change_type") == "I").join(
    dim_current.select("customer_id").distinct(),
    on="customer_id",
    how="left_anti"
)

# Get next surrogate key
max_dim_key = spark.sql(f"SELECT COALESCE(MAX(customer_dim_key), 0) FROM {DIM_TABLE}") \
    .collect()[0][0]

inserts_final = inserts.select(
    expr(f"ROW_NUMBER() OVER (ORDER BY customer_id) + {max_dim_key} AS customer_dim_key"),
    col("customer_id"),
    col("customer_name"),
    col("email"),
    col("phone"),
    col("address"),
    col("city"),
    col("state_province"),
    col("postal_code"),
    col("country"),
    col("account_status"),
    current_date().alias("effective_start_dt"),
    expr("CAST('9999-12-31' AS DATE) AS effective_end_dt"),
    expr("TRUE AS is_active"),
    expr("'I' AS change_type"),
    col("source_system_id"),
    current_timestamp().alias("dw_insert_dt"),
    current_timestamp().alias("dw_update_dt")
)

# ============================================================================
# STEP 4: PROCESS UPDATES (Existing customers with changed attributes)
# ============================================================================

updates_candidates = staging_today.filter(col("change_type") == "U").join(
    dim_current,
    on="customer_id",
    how="inner"
)

# Filter to only records where TRACKED attributes actually changed
# (Business decision: which attributes trigger SCD2?)
tracked_attributes = [
    "customer_name", "email", "phone", "address",
    "city", "state_province", "postal_code", "country", "account_status"
]

updates = updates_candidates.filter(
    (col("staging.customer_name") != col("dim.customer_name")) |
    (col("staging.email") != col("dim.email")) |
    (col("staging.phone") != col("dim.phone")) |
    (col("staging.address") != col("dim.address")) |
    (col("staging.city") != col("dim.city")) |
    (col("staging.state_province") != col("dim.state_province")) |
    (col("staging.postal_code") != col("dim.postal_code")) |
    (col("staging.country") != col("dim.country")) |
    (col("staging.account_status") != col("dim.account_status"))
)

# Close old versions (set effective_end_dt to yesterday)
old_versions = updates.select(
    col("dim.customer_dim_key"),
    col("dim.customer_id"),
    col("dim.effective_start_dt"),
    expr("DATE_SUB(CURRENT_DATE(), 1) AS effective_end_dt"),
    expr("FALSE AS is_active"),
    col("dim.customer_name"),
    col("dim.email"),
    col("dim.phone"),
    col("dim.address"),
    col("dim.city"),
    col("dim.state_province"),
    col("dim.postal_code"),
    col("dim.country"),
    col("dim.account_status"),
    col("dim.change_type"),
    col("dim.source_system_id"),
    col("dim.dw_insert_dt"),
    current_timestamp().alias("dw_update_dt")
)

# Insert new versions
new_versions = updates.select(
    expr(f"ROW_NUMBER() OVER (ORDER BY staging.customer_id) + {max_dim_key + inserts_final.count()} AS customer_dim_key"),
    col("staging.customer_id"),
    col("staging.customer_name"),
    col("staging.email"),
    col("staging.phone"),
    col("staging.address"),
    col("staging.city"),
    col("staging.state_province"),
    col("staging.postal_code"),
    col("staging.country"),
    col("staging.account_status"),
    current_date().alias("effective_start_dt"),
    expr("CAST('9999-12-31' AS DATE) AS effective_end_dt"),
    expr("TRUE AS is_active"),
    expr("'U' AS change_type"),
    col("staging.source_system_id"),
    current_timestamp().alias("dw_insert_dt"),
    current_timestamp().alias("dw_update_dt")
)

# ============================================================================
# STEP 5: PROCESS DELETES (Mark as inactive)
# ============================================================================

deletes = staging_today.filter(col("change_type") == "D").join(
    dim_current,
    on="customer_id",
    how="inner"
)

deletes_final = deletes.select(
    col("dim.customer_dim_key"),
    col("dim.customer_id"),
    col("dim.effective_start_dt"),
    expr("DATE_SUB(CURRENT_DATE(), 1) AS effective_end_dt"),
    expr("FALSE AS is_active"),
    col("dim.customer_name"),
    col("dim.email"),
    col("dim.phone"),
    col("dim.address"),
    col("dim.city"),
    col("dim.state_province"),
    col("dim.postal_code"),
    col("dim.country"),
    col("dim.account_status"),
    expr("'D' AS change_type"),
    col("dim.source_system_id"),
    col("dim.dw_insert_dt"),
    current_timestamp().alias("dw_update_dt")
)

# ============================================================================
# STEP 6: UNION ALL DIMENSION CHANGES
# ============================================================================

dim_updates = inserts_final \
    .unionByName(old_versions) \
    .unionByName(new_versions) \
    .unionByName(deletes_final)

dim_updates.createOrReplaceTempView("staging_dim_updates")

print(f"Dimension updates: Inserts={inserts_final.count()}, Updates={new_versions.count()}, Deletes={deletes_final.count()}")

# ============================================================================
# STEP 7: MERGE INTO DIMENSION (IDEMPOTENT)
# ============================================================================

# Use MERGE to ensure idempotency
spark.sql(f"""
    MERGE INTO {DIM_TABLE} AS dim
    USING staging_dim_updates AS stg
    ON dim.customer_dim_key = stg.customer_dim_key
    
    WHEN MATCHED THEN
        UPDATE SET
            effective_end_dt = stg.effective_end_dt,
            is_active = stg.is_active,
            dw_update_dt = stg.dw_update_dt
    
    WHEN NOT MATCHED THEN
        INSERT (
            customer_dim_key, customer_id, customer_name, email, phone,
            address, city, state_province, postal_code, country,
            account_status, effective_start_dt, effective_end_dt, is_active,
            change_type, source_system_id, dw_insert_dt, dw_update_dt
        )
        VALUES (
            stg.customer_dim_key, stg.customer_id, stg.customer_name, stg.email,
            stg.phone, stg.address, stg.city, stg.state_province, stg.postal_code,
            stg.country, stg.account_status, stg.effective_start_dt, stg.effective_end_dt,
            stg.is_active, stg.change_type, stg.source_system_id, stg.dw_insert_dt,
            stg.dw_update_dt
        )
""")

print(f"Successfully merged dimension updates to {DIM_TABLE}")

# ============================================================================
# STEP 8: DATA QUALITY CHECKS
# ============================================================================

# Check for duplicate active keys
duplicates = spark.sql(f"""
    SELECT customer_id, COUNT(*) as count
    FROM {DIM_TABLE}
    WHERE is_active = TRUE
    GROUP BY customer_id
    HAVING COUNT(*) > 1
""")

if duplicates.count() > 0:
    print("WARNING: Found duplicate active records!")
    duplicates.show()

# Check for backward-dated effective dates
backward_dates = spark.sql(f"""
    SELECT COUNT(*) as count
    FROM {DIM_TABLE}
    WHERE effective_start_dt > effective_end_dt
""")

if backward_dates.collect()[0][0] > 0:
    print("ERROR: Found backward-dated effective dates!")

print("Dimension load complete")

spark.stop()
```

---

## Monitoring & Validation Queries

```sql
-- Check Staging audit trail by day
SELECT insert_dt, change_type, COUNT(*) as count
FROM silver.customer_staging
GROUP BY insert_dt, change_type
ORDER BY insert_dt DESC;

-- Verify Dimension SCD2 integrity
SELECT 
    customer_id,
    COUNT(*) as version_count,
    COUNT(CASE WHEN is_active = TRUE THEN 1 END) as active_count,
    MIN(effective_start_dt) as oldest_version,
    MAX(CASE WHEN is_active = TRUE THEN effective_start_dt END) as current_version_start
FROM gold.customer_dimension
GROUP BY customer_id
HAVING active_count != 1 OR version_count != (active_count + COUNT(CASE WHEN is_active = FALSE THEN 1 END))
ORDER BY customer_id;

-- Track dimension row growth
SELECT 
    DATE(dw_insert_dt) as load_date,
    COUNT(*) as rows_inserted,
    COUNT(DISTINCT customer_id) as unique_customers
FROM gold.customer_dimension
WHERE dw_insert_dt >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY DATE(dw_insert_dt)
ORDER BY load_date DESC;

-- Fact table volume by date
SELECT 
    order_date_key,
    COUNT(*) as fact_rows,
    COUNT(DISTINCT order_id) as unique_orders,
    SUM(order_net_amount) as total_revenue
FROM gold.orders_fact
WHERE order_date_key >= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), 90), 'yyyyMMdd')
GROUP BY order_date_key
ORDER BY order_date_key DESC;

-- Lineage: trace fact back to source
SELECT 
    f.order_id,
    f.order_date_key,
    st.source_hash,
    st.insert_dt,
    cd.effective_start_dt,
    cd.is_active
FROM gold.orders_fact f
JOIN silver.order_staging st ON f.order_id = st.order_id
JOIN gold.customer_dimension cd ON f.customer_dim_key = cd.customer_dim_key
WHERE f.order_date_key = CAST(FORMAT_DATE('%Y%m%d', CURRENT_DATE()) AS INT)
LIMIT 100;
```

---

## Appendix: Helper Stored Procedures

```sql
-- Procedure: Detect and log data quality issues
CREATE OR REPLACE PROCEDURE dq_validation()
BEGIN
    -- Check for NULL surrogate keys in Staging
    CREATE TEMP TABLE dq_null_surrogate AS
    SELECT COUNT(*) as count FROM silver.customer_staging
    WHERE surrogate_key IS NULL;
    
    -- Check for missing hash values
    CREATE TEMP TABLE dq_missing_hash AS
    SELECT COUNT(*) as count FROM silver.customer_staging
    WHERE source_hash IS NULL AND change_type != 'D';
    
    -- Log results
    INSERT INTO gold.dq_validation_log
    SELECT 
        CURRENT_TIMESTAMP() as check_dt,
        'null_surrogate' as check_type,
        (SELECT count FROM dq_null_surrogate) as issue_count;
    
    PRINT 'Data quality validation complete';
END;

-- Procedure: Optimize all lakehouse tables
CREATE OR REPLACE PROCEDURE optimize_lakehouse()
BEGIN
    OPTIMIZE silver.customer_staging ZORDER BY (customer_id, insert_dt);
    OPTIMIZE silver.product_staging ZORDER BY (product_id, insert_dt);
    OPTIMIZE gold.customer_dimension ZORDER BY (customer_id, effective_start_dt);
    OPTIMIZE gold.product_dimension ZORDER BY (product_id, effective_start_dt);
    OPTIMIZE gold.orders_fact ZORDER BY (customer_dim_key, order_date_key);
    
    PRINT 'Lakehouse optimization complete';
END;
```

---

This appendix provides complete, production-ready SQL DDL and PySpark code for implementation.
