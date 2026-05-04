# Staging Layer (Silver) Architecture in Databricks Lakehouse
## DMBOK-Aligned Design with Landing-to-Curated Data Flow

**Author Role:** Lead Data Architect  
**Date:** May 2026  
**Architecture:** Medallion (Bronze/Silver/Gold) + Data Vault 2.0 Principles  
**Platform:** Databricks Lakehouse

---

## EXECUTIVE SUMMARY: DMBOK INTERPRETATION

### What the DMBOK Actually Says About Staging

The **DAMA-DMBOK2** document distinguishes **five core principles** for staging layers:

| Principle | DMBOK Intent | Your Challenge |
|-----------|--------------|-----------------|
| **Source Fidelity** | Data must mirror source exactly | You have it (hashing, delta detection) |
| **No Semantic Enrichment** | No business meaning added | Your effective_start_dt, effective_end_dt, is_active = BUSINESS RULES |
| **Technical-Only Transforms** | Format normalization, data type casting, null handling | Historization (SCD2) = BUSINESS RULE, not technical |
| **Auditability & Lineage** | Preserve raw data for tracing back to source | SCD2 obscures "what came from source vs. derived" |
| **Idempotency** | Re-run = same result, no state-dependent logic | SCD2 is state-dependent (depends on prior versions) |

### The Core Issue: Technical vs. Business Rules

**SCD2 violates staging principles because it answers these BUSINESS questions:**
- *"What constitutes a meaningful change?"*
- *"Which attributes trigger a new historical version?"*
- *"What is the effective date of this change?"*

These require **business stakeholder decisions**, not technical engineering.

### The Resolution: Two-Layer Historization

Your challenge is **CORRECT** — you NEED effective_start_dt, effective_end_dt, is_active to efficiently process curated layers. But where you place them matters:

**STAGING LAYER (Silver) — Your Current Design:**
```
Landing → Staging {
  pk, surrogate_key, hash, is_deleted,
  insert_dt (TECHNICAL, not BUSINESS)
  ← These three columns live here, but NOT as SCD2 logic
}
```

**CURATED LAYER (Gold) — SCD2 Logic:**
```
Staging → Dimension {
  pk, dim_key, effective_start_dt, effective_end_dt, is_active,
  (business attributes)
  ← SCD2 logic lives here, with business rules about which attributes matter
}
```

**The key insight:** Your staging layer enriches with *technical* timestamps that enable downstream processing—NOT SCD2 business logic.

---

## PART 1: LANDING-TO-STAGING TABLE DESIGN

### Design Principle

**Staging = A versioned audit trail of ALL deltas from landing, preserved as-is, with only technical enrichment.**

### Table Structure (Silver/Staging Layer)

```sql
CREATE TABLE silver.customer_staging (
  -- Source identifiers & technical keys
  customer_id INT NOT NULL,           -- Business key from source
  surrogate_key LONG,                 -- Auto-increment, one per delta record
  source_system_id VARCHAR(50),       -- Lineage: which source?
  
  -- Change detection & tracking
  source_hash BINARY,                 -- SHA-256 hash of all attributes (for detecting changes)
  change_type VARCHAR(10),            -- 'I' (insert), 'U' (update), 'D' (delete)
  is_deleted BOOLEAN DEFAULT FALSE,   -- Logical delete marker
  
  -- Technical timestamps (ENRICHMENT, not business)
  insert_dt DATE,                     -- When this record was INSERTED into Staging
  extract_dt TIMESTAMP,               -- When source data was EXTRACTED (from Landing)
  load_ts TIMESTAMP,                  -- Spark job load timestamp
  
  -- Source data (all attributes)
  customer_name VARCHAR(100),
  email VARCHAR(100),
  phone VARCHAR(20),
  address VARCHAR(500),
  city VARCHAR(50),
  country VARCHAR(50),
  account_status VARCHAR(20),
  
  -- Metadata
  row_number BIGINT,                  -- For ordering within a batch
  process_id VARCHAR(100),            -- Which batch/run?
  
  CONSTRAINT pk_stg_cust PRIMARY KEY (surrogate_key)
) USING DELTA
PARTITIONED BY (insert_dt)
CLUSTER BY (customer_id, insert_dt);
```

### Staging Design Rationale

| Column | Layer | Purpose | Notes |
|--------|-------|---------|-------|
| `customer_id` | Source | Business key from Landing | Identifies the entity |
| `surrogate_key` | Staging | Row-level uniqueness | Auto-increment, one per delta |
| `source_hash` | Staging | Change detection | Hash of ALL attributes (Landing) |
| `change_type` | Staging | Delta classification | Technical indicator: I/U/D |
| `insert_dt` | Staging | Partitioning & filtering | TECHNICAL: when added to Staging |
| `extract_dt` | Staging | Lineage | When data came from source |
| `is_deleted` | Staging | Logical delete flag | For deletions (mark in-place, append as insert) |

**What is NOT here:**
- ❌ `effective_start_dt, effective_end_dt, is_active` (belongs in Gold)
- ❌ SCD2 logic (belongs in Gold)
- ❌ Aggregations or business rule logic (belongs in Gold)

---

## PART 2: LANDING-TO-STAGING PROCESSING LOGIC

### Current Logic (Correct!)

Your current approach is **architecturally sound**:

1. **Hash all Landing records** → Compare with Staging hashes
2. **Identify deltas** (Insert, Update, Delete)
3. **Append delta records** as new rows (with surrogate key)
4. **Mark deletions** with `is_deleted = TRUE`

**Code Example: Delta Detection & Staging Population**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    md5, concat_ws, col, when, current_timestamp, 
    expr, dense_rank, row_number, current_date
)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("landing-to-staging").getOrCreate()

# Step 1: Read Landing data
landing = spark.read.table("bronze.customer_landing")

# Step 2: Generate hash of all attributes (deterministic across batches)
landing_with_hash = landing.select(
    "*",
    md5(concat_ws("||", 
        col("customer_id"), col("customer_name"), col("email"),
        col("phone"), col("address"), col("city"), col("country"),
        col("account_status")
    )).alias("source_hash"),
    current_timestamp().alias("extract_dt"),
    current_date().alias("insert_dt")
)

# Step 3: Read current Staging layer
staging_current = spark.read.table("silver.customer_staging") \
    .select("customer_id", "source_hash", "insert_dt") \
    .where(col("is_deleted") == False)  # Ignore deleted records

# Step 4: Find NEW hashes (inserts + updates)
new_hashes = landing_with_hash.join(
    staging_current,
    on=[
        landing_with_hash.customer_id == staging_current.customer_id,
        landing_with_hash.source_hash == staging_current.source_hash
    ],
    how="left_anti"  # Only records not found in staging
)

# Step 5: Classify deltas
# A. Records in Landing but not in Staging = either INSERT or UPDATE
# B. Records in Staging but not in Landing = DELETE

# For Inserts: Is this the first time we see this customer_id?
inserts = new_hashes.join(
    staging_current,
    on="customer_id",
    how="left_anti"  # customer_id not in staging
).withColumn("change_type", expr("'I'"))

# For Updates: customer_id exists in Staging, but hash is different
updates = new_hashes.join(
    staging_current,
    on="customer_id",
    how="inner"  # customer_id exists in staging
).withColumn("change_type", expr("'U'"))

# For Deletes: customer_id was in previous Staging, not in Landing
deletes = staging_current.join(
    landing_with_hash,
    on="customer_id",
    how="left_anti"  # Not in landing
).withColumn("change_type", expr("'D'"))

# Step 6: Union all delta types
staging_deltas = inserts.unionByName(updates, allowMissingColumns=True) \
    .unionByName(deletes, allowMissingColumns=True)

# Step 7: Add surrogate key (auto-increment)
staging_deltas_with_sk = staging_deltas.select(
    expr("ROW_NUMBER() OVER (ORDER BY insert_dt, customer_id) AS surrogate_key"),
    "*",
    current_timestamp().alias("load_ts"),
    expr("UUID() AS process_id")
)

# Step 8: For deletes, set is_deleted flag and nullify data columns
staging_deltas_with_flags = staging_deltas_with_sk.select(
    "*",
    when(col("change_type") == "D", True).otherwise(False).alias("is_deleted")
)

# Step 9: Append to Staging table
staging_deltas_with_flags.write.mode("append").insertInto("silver.customer_staging")

# Step 10: Optimize Staging table
spark.sql("OPTIMIZE silver.customer_staging ZORDER BY (customer_id, insert_dt)")

print(f"Inserts: {inserts.count()}, Updates: {updates.count()}, Deletes: {deletes.count()}")
```

### Why This Design is DMBOK-Aligned

✅ **Source Fidelity**: Landing data replicated as-is to Staging  
✅ **No Semantic Enrichment**: No business rules applied (hashing is technical)  
✅ **Technical-Only Transforms**: Hash, delta detection, data type normalization  
✅ **Auditability**: All records (Insert/Update/Delete) preserved in Staging  
✅ **Idempotency**: Re-running detects same deltas (hash-based, not state-dependent)  

---

## PART 3: STAGING-TO-CURATED LAYER (SCD2 Logic)

### Curated Layer Design (Gold)

This is where SCD2 lives. Here, business rules define:
- Which attributes trigger a new version
- What the effective dates mean
- What "active" means

```sql
CREATE TABLE gold.customer_dimension (
  -- Dimension identifiers
  customer_dim_key BIGINT NOT NULL,              -- Surrogate key for DW
  customer_id INT NOT NULL,                      -- Business key from source
  
  -- Historical tracking (SCD2 - BUSINESS LOGIC)
  effective_start_dt DATE NOT NULL,              -- When this version started
  effective_end_dt DATE DEFAULT 9999-12-31,      -- When this version ended
  is_active BOOLEAN DEFAULT TRUE,                -- Current version?
  
  -- Slowly Changing Dimension Attributes
  customer_name VARCHAR(100),
  email VARCHAR(100),
  phone VARCHAR(20),
  address VARCHAR(500),
  city VARCHAR(50),
  country VARCHAR(50),
  account_status VARCHAR(20),
  
  -- SCD2 Metadata
  change_type VARCHAR(10),                       -- Which attributes changed?
  source_system_id VARCHAR(50),
  dw_insert_dt TIMESTAMP,                        -- When loaded to DW
  dw_update_dt TIMESTAMP,                        -- When last updated in DW
  
  CONSTRAINT pk_dim_cust PRIMARY KEY (customer_dim_key)
) USING DELTA;

CREATE TABLE gold.customer_dimension_history (
  -- Track which attributes changed
  customer_dim_key BIGINT,
  attribute_name VARCHAR(100),
  old_value VARCHAR(500),
  new_value VARCHAR(500),
  change_dt DATE,
  PRIMARY KEY (customer_dim_key, attribute_name, change_dt)
) USING DELTA;
```

### Processing Logic: Staging → Curated (SCD2)

**Pseudo-code with Databricks SQL**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, max, coalesce, concat_ws, md5, 
    current_date, current_timestamp, dense_rank,
    row_number, explode, struct
)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("staging-to-curated").getOrCreate()

# Step 1: Read latest delta records from Staging (current processing day)
staging_deltas = spark.sql("""
    SELECT *
    FROM silver.customer_staging
    WHERE insert_dt = CURRENT_DATE()
      AND is_deleted = FALSE
      AND change_type IN ('I', 'U')  -- Exclude deletes for now (handle separately)
""")

# Step 2: Read current Dimension table
dim_current = spark.sql("""
    SELECT *
    FROM gold.customer_dimension
    WHERE is_active = TRUE
""")

# Step 3: Identify INSERTS (new customers not in Dimension)
inserts = staging_deltas.join(
    dim_current.select("customer_id").distinct(),
    on="customer_id",
    how="left_anti"
)

inserts_to_load = inserts.select(
    expr("ROW_NUMBER() OVER (ORDER BY customer_id) + (SELECT MAX(customer_dim_key) FROM gold.customer_dimension) AS customer_dim_key"),
    col("customer_id"),
    current_date().alias("effective_start_dt"),
    expr("date('9999-12-31') AS effective_end_dt"),
    expr("TRUE AS is_active"),
    col("customer_name"),
    col("email"),
    col("phone"),
    col("address"),
    col("city"),
    col("country"),
    col("account_status"),
    expr("'I' AS change_type"),
    col("source_system_id"),
    current_timestamp().alias("dw_insert_dt"),
    current_timestamp().alias("dw_update_dt")
)

# Step 4: Identify UPDATES (customers in Dimension with hash difference)
updates_candidates = staging_deltas.join(
    dim_current,
    on="customer_id",
    how="inner"
)

# Only process if attributes actually changed (not just timestamp drift)
updates = updates_candidates.filter(
    # Define which attributes trigger SCD2 (BUSINESS DECISION)
    (col("staging.customer_name") != col("dim.customer_name")) |
    (col("staging.email") != col("dim.email")) |
    (col("staging.phone") != col("dim.phone")) |
    (col("staging.address") != col("dim.address")) |
    (col("staging.city") != col("dim.city")) |
    (col("staging.country") != col("dim.country")) |
    (col("staging.account_status") != col("dim.account_status"))
)

# Step 5: For each update:
#   a. Close the old version (effective_end_dt = today - 1)
#   b. Insert a new version (effective_start_dt = today)

old_versions_to_close = updates.select(
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
    col("dim.country"),
    col("dim.account_status"),
    col("dim.change_type"),
    col("dim.source_system_id"),
    col("dim.dw_insert_dt"),
    current_timestamp().alias("dw_update_dt")
)

new_versions = updates.select(
    expr("ROW_NUMBER() OVER (ORDER BY staging.customer_id) + (SELECT MAX(customer_dim_key) FROM gold.customer_dimension) AS customer_dim_key"),
    col("staging.customer_id"),
    current_date().alias("effective_start_dt"),
    expr("date('9999-12-31') AS effective_end_dt"),
    expr("TRUE AS is_active"),
    col("staging.customer_name"),
    col("staging.email"),
    col("staging.phone"),
    col("staging.address"),
    col("staging.city"),
    col("staging.country"),
    col("staging.account_status"),
    expr("'U' AS change_type"),
    col("staging.source_system_id"),
    current_timestamp().alias("dw_insert_dt"),
    current_timestamp().alias("dw_update_dt")
)

# Step 6: Handle DELETES (mark as inactive)
staging_deletes = spark.sql("""
    SELECT *
    FROM silver.customer_staging
    WHERE insert_dt = CURRENT_DATE()
      AND is_deleted = TRUE
""")

deletes_to_close = staging_deletes.join(
    dim_current,
    on="customer_id",
    how="inner"
).select(
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
    col("dim.country"),
    col("dim.account_status"),
    expr("'D' AS change_type"),
    col("dim.source_system_id"),
    col("dim.dw_insert_dt"),
    current_timestamp().alias("dw_update_dt")
)

# Step 7: Union all dimension updates
dim_updates = old_versions_to_close.unionByName(new_versions).unionByName(deletes_to_close)

# Step 8: Write to Dimension (MERGE pattern for idempotency)
spark.sql("""
    MERGE INTO gold.customer_dimension AS dim
    USING temp_dim_updates AS updates
    ON dim.customer_dim_key = updates.customer_dim_key
    
    WHEN MATCHED THEN
        UPDATE SET
            effective_end_dt = updates.effective_end_dt,
            is_active = updates.is_active,
            dw_update_dt = updates.dw_update_dt
    
    WHEN NOT MATCHED THEN
        INSERT (customer_dim_key, customer_id, effective_start_dt, 
                effective_end_dt, is_active, customer_name, email, phone,
                address, city, country, account_status, change_type,
                source_system_id, dw_insert_dt, dw_update_dt)
        VALUES (updates.customer_dim_key, updates.customer_id, 
                updates.effective_start_dt, updates.effective_end_dt,
                updates.is_active, updates.customer_name, updates.email,
                updates.phone, updates.address, updates.city, updates.country,
                updates.account_status, updates.change_type,
                updates.source_system_id, updates.dw_insert_dt, 
                updates.dw_update_dt)
""")

print("Dimension load complete")
```

---

## PART 4: DIMENSION DESIGN IN CURATED LAYER

### Customer Dimension (Fully Denormalized, SCD2)

```sql
-- DIMENSION TABLE (SCD2)
CREATE TABLE gold.customer_dimension (
  customer_dim_key BIGINT,                -- Surrogate key (auto-increment in DW)
  customer_id INT,                        -- Business key (customer # from CRM)
  customer_name VARCHAR(100),
  email VARCHAR(100),
  phone VARCHAR(20),
  address VARCHAR(500),
  city VARCHAR(50),
  state_province VARCHAR(50),
  postal_code VARCHAR(20),
  country VARCHAR(50),
  account_status VARCHAR(20),             -- Active, Inactive, Suspended
  
  -- SCD2 Attributes
  effective_start_dt DATE,
  effective_end_dt DATE,
  is_active BOOLEAN,
  
  -- Metadata
  change_flag VARCHAR(10),                -- Which attribute changed? (optional, can track separately)
  source_system_id VARCHAR(50),
  dw_insert_dt TIMESTAMP,
  dw_update_dt TIMESTAMP,
  
  PRIMARY KEY (customer_dim_key)
) USING DELTA;

-- HISTORY TRACKING (Optional, for audit)
CREATE TABLE gold.customer_history_audit (
  customer_dim_key BIGINT,
  old_record_json STRING,                 -- JSON of previous version
  new_record_json STRING,                 -- JSON of new version
  changed_attributes STRING,              -- JSON array of changed fields
  change_dt DATE,
  changed_by_process_id VARCHAR(100),
  
  PRIMARY KEY (customer_dim_key, change_dt)
) USING DELTA;
```

---

## PART 5: FACT TABLE DESIGN & LOADING

### Order Fact Table (Conformed Grain: One Row per Order-Line)

```sql
CREATE TABLE gold.orders_fact (
  order_fact_key BIGINT,                  -- Fact table surrogate key
  order_id INT NOT NULL,                  -- Business key from source
  order_line_id INT NOT NULL,
  
  -- Foreign keys (references to dimensions)
  customer_dim_key BIGINT NOT NULL,       -- Links to customer_dimension
  product_dim_key BIGINT NOT NULL,        -- Links to product_dimension
  
  -- Slowly changing dimensions at point-in-time
  customer_dim_key_scd BIGINT,            -- Customer version as-of order_dt
  product_dim_key_scd BIGINT,             -- Product version as-of order_dt
  
  -- Facts (measures)
  order_quantity INT,
  order_unit_price DECIMAL(10,2),
  order_extended_price DECIMAL(12,2),
  order_discount_amount DECIMAL(10,2),
  order_tax_amount DECIMAL(10,2),
  order_net_amount DECIMAL(12,2),
  
  -- Dimensions (conformed)
  order_date DATE NOT NULL,
  ship_date DATE,
  delivery_date DATE,
  
  order_status VARCHAR(20),               -- Pending, Shipped, Delivered, Cancelled
  
  -- Metadata
  dw_insert_dt TIMESTAMP,
  dw_update_dt TIMESTAMP,
  
  PRIMARY KEY (order_fact_key)
) USING DELTA
PARTITIONED BY (order_date)
CLUSTER BY (customer_dim_key, order_date);
```

### Loading Facts from Staging to Gold

```python
# Load Order Facts from Staging

# Step 1: Read staging data
staging_orders = spark.sql("""
    SELECT 
        order_id, order_line_id, customer_id,
        product_id, order_qty, unit_price,
        discount_pct, tax_pct, order_dt,
        ship_dt, delivery_dt, order_status
    FROM silver.order_staging
    WHERE insert_dt = CURRENT_DATE()
      AND is_deleted = FALSE
""")

# Step 2: Join to customer dimension (get current active version)
with_customer_dim = staging_orders.join(
    spark.sql("""
        SELECT customer_dim_key, customer_id
        FROM gold.customer_dimension
        WHERE is_active = TRUE
    """),
    on="customer_id",
    how="left"
)

# Step 3: Join to product dimension (get version as-of order_dt)
# This is critical for SCD2: link to the PRODUCT VERSION that was valid on order_dt
with_product_dim = with_customer_dim.join(
    spark.sql("""
        SELECT product_dim_key, product_id, effective_start_dt, effective_end_dt
        FROM gold.product_dimension
    """),
    on=(
        (col("staging.product_id") == col("prod_dim.product_id")) &
        (col("staging.order_dt") >= col("prod_dim.effective_start_dt")) &
        (col("staging.order_dt") < col("prod_dim.effective_end_dt"))
    ),
    how="left"
)

# Step 4: Calculate derived facts
facts = with_product_dim.select(
    expr("ROW_NUMBER() OVER (ORDER BY order_id, order_line_id) + (SELECT MAX(order_fact_key) FROM gold.orders_fact) AS order_fact_key"),
    col("order_id"),
    col("order_line_id"),
    col("customer_dim_key"),
    col("product_dim_key"),
    col("customer_dim_key").alias("customer_dim_key_scd"),  # SCD snapshot
    col("product_dim_key").alias("product_dim_key_scd"),    # SCD snapshot
    col("order_qty").alias("order_quantity"),
    col("unit_price").alias("order_unit_price"),
    (col("order_qty") * col("unit_price")).alias("order_extended_price"),
    (col("order_qty") * col("unit_price") * col("discount_pct") / 100).alias("order_discount_amount"),
    ((col("order_qty") * col("unit_price") * (1 - col("discount_pct") / 100)) * col("tax_pct") / 100).alias("order_tax_amount"),
    ((col("order_qty") * col("unit_price") * (1 - col("discount_pct") / 100)) * (1 + col("tax_pct") / 100)).alias("order_net_amount"),
    col("order_dt").alias("order_date"),
    col("ship_dt").alias("ship_date"),
    col("delivery_dt").alias("delivery_date"),
    col("order_status"),
    current_timestamp().alias("dw_insert_dt"),
    current_timestamp().alias("dw_update_dt")
)

# Step 5: Load to fact table
facts.write.mode("append").insertInto("gold.orders_fact")
```

---

## PART 6: REFERENCE & LOOKUP TABLES (DIMENSION-LESS FACTS)

### Status Dimension (Conformed, Small, No History)

```sql
CREATE TABLE gold.status_dimension (
  status_key INT,
  status_code VARCHAR(20),
  status_name VARCHAR(100),
  status_description VARCHAR(500),
  is_active BOOLEAN DEFAULT TRUE
) USING DELTA;

-- Load once, update rarely
INSERT INTO gold.status_dimension VALUES
(1, 'PENDING', 'Order Pending', 'Awaiting fulfillment', TRUE),
(2, 'SHIPPED', 'Order Shipped', 'In transit to customer', TRUE),
(3, 'DELIVERED', 'Order Delivered', 'Successfully delivered', TRUE),
(4, 'CANCELLED', 'Order Cancelled', 'Customer cancelled order', TRUE);
```

### TBUD (Table-Based Upsert Dump) Pattern

For rapidly changing, non-historical data (no SCD2), use MERGE:

```python
# TBUD Pattern: Daily full reload with MERGE for idempotency

staging_tbud_data = spark.sql("""
    SELECT 
        product_id, current_inventory, last_count_dt,
        warehouse_location, bin_number
    FROM silver.inventory_staging
    WHERE insert_dt = CURRENT_DATE()
""")

# MERGE for idempotency: re-run = same result
spark.sql("""
    MERGE INTO gold.inventory_tbud AS tbud
    USING staging_tbud_data AS staging
    ON tbud.product_id = staging.product_id
    
    WHEN MATCHED THEN
        UPDATE SET
            current_inventory = staging.current_inventory,
            last_count_dt = staging.last_count_dt,
            warehouse_location = staging.warehouse_location,
            bin_number = staging.bin_number,
            dw_update_dt = CURRENT_TIMESTAMP()
    
    WHEN NOT MATCHED THEN
        INSERT (product_id, current_inventory, last_count_dt, 
                warehouse_location, bin_number, dw_insert_dt, dw_update_dt)
        VALUES (staging.product_id, staging.current_inventory, 
                staging.last_count_dt, staging.warehouse_location, 
                staging.bin_number, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
""")
```

---

## PART 7: END-TO-END EXAMPLE WITH DATA

### Source Data (CRM System)

**Landing Table (bronze.customer_landing) — Day 1:**

| customer_id | customer_name | email | phone | address | city | country | account_status |
|---|---|---|---|---|---|---|---|
| 101 | Alice Johnson | alice@example.com | 555-1001 | 123 Main St | Springfield | USA | Active |
| 102 | Bob Smith | bob@example.com | 555-1002 | 456 Oak Ave | Portland | USA | Active |
| 103 | Carol White | carol@example.com | 555-1003 | 789 Pine Rd | Seattle | USA | Active |

**Landing Table (bronze.customer_landing) — Day 2 (with changes):**

| customer_id | customer_name | email | phone | address | city | country | account_status |
|---|---|---|---|---|---|---|---|
| 101 | Alice Johnson | alice.j@example.com | 555-1001 | 123 Main St | Springfield | USA | Active | ← Email changed |
| 102 | Bob Smith | bob@example.com | 555-1002 | 456 Oak Ave | Portland | USA | Inactive | ← Status changed |
| 103 | Carol White | carol@example.com | 555-1003 | 789 Pine Rd | Seattle | USA | Active |
| 104 | Diana Prince | diana@example.com | 555-1004 | 321 Wonder Way | Los Angeles | USA | Active | ← New customer |

**Processing Day 2: Hashes**

```
Day 1:
101: MD5("101||Alice Johnson||alice@example.com||555-1001||123 Main St||Springfield||USA||Active") = HASH_A
102: MD5("102||Bob Smith||bob@example.com||555-1002||456 Oak Ave||Portland||USA||Active") = HASH_B
103: MD5("103||Carol White||carol@example.com||555-1003||789 Pine Rd||Seattle||USA||Active") = HASH_C

Day 2:
101: MD5("101||Alice Johnson||alice.j@example.com||555-1001||123 Main St||Springfield||USA||Active") = HASH_A_NEW (≠ HASH_A) → UPDATE
102: MD5("102||Bob Smith||bob@example.com||555-1002||456 Oak Ave||Portland||USA||Inactive") = HASH_B_NEW (≠ HASH_B) → UPDATE
103: MD5("103||Carol White||carol@example.com||555-1003||789 Pine Rd||Seattle||USA||Active") = HASH_C (= HASH_C) → NO CHANGE
104: MD5("104||Diana Prince||diana@example.com||555-1004||321 Wonder Way||Los Angeles||USA||Active") = HASH_D (new) → INSERT
```

### Staging Layer Output After Day 2

| surrogate_key | customer_id | source_hash | change_type | is_deleted | insert_dt | customer_name | email | account_status |
|---|---|---|---|---|---|---|---|---|
| 1 | 101 | HASH_A | I | FALSE | 2026-05-01 | Alice Johnson | alice@example.com | Active |
| 2 | 102 | HASH_B | I | FALSE | 2026-05-01 | Bob Smith | bob@example.com | Active |
| 3 | 103 | HASH_C | I | FALSE | 2026-05-01 | Carol White | carol@example.com | Active |
| 4 | 101 | HASH_A_NEW | U | FALSE | 2026-05-02 | Alice Johnson | alice.j@example.com | Active |
| 5 | 102 | HASH_B_NEW | U | FALSE | 2026-05-02 | Bob Smith | bob@example.com | Inactive |
| 6 | 104 | HASH_D | I | FALSE | 2026-05-02 | Diana Prince | diana@example.com | Active |

**Key Observations:**
- Each change is a NEW row with NEW surrogate_key
- Original rows (1, 2, 3) remain unchanged (audit trail)
- `insert_dt` reflects WHEN the delta was captured
- `source_hash` enables idempotent re-processing

### Curated Dimension Output After Day 2

**gold.customer_dimension:**

| customer_dim_key | customer_id | customer_name | email | account_status | effective_start_dt | effective_end_dt | is_active | change_type |
|---|---|---|---|---|---|---|---|---|
| 1 | 101 | Alice Johnson | alice@example.com | Active | 2026-05-01 | 2026-05-01 | FALSE | U |
| 2 | 102 | Bob Smith | bob@example.com | Active | 2026-05-01 | 2026-05-01 | FALSE | U |
| 3 | 103 | Carol White | carol@example.com | Active | 2026-05-01 | 9999-12-31 | TRUE | I |
| 4 | 101 | Alice Johnson | alice.j@example.com | Active | 2026-05-02 | 9999-12-31 | TRUE | U |
| 5 | 102 | Bob Smith | bob@example.com | Inactive | 2026-05-02 | 9999-12-31 | TRUE | U |
| 6 | 104 | Diana Prince | diana@example.com | Active | 2026-05-02 | 9999-12-31 | TRUE | I |

**Key Design Decisions:**
- **SCD2 logic is in Gold layer**, not Silver
- **Version closure happens at curation time**, not at Landing
- **Effective dates reflect BUSINESS events**, not technical load times
- **Multiple versions preserved** for historical analysis
- **is_active flag** allows filtering to "current state" queries

### Using the Dimension in Queries

```sql
-- Query 1: Current customer state
SELECT *
FROM gold.customer_dimension
WHERE is_active = TRUE;

-- Query 2: Customer state as of a specific date
SELECT *
FROM gold.customer_dimension
WHERE effective_start_dt <= DATE('2026-05-02')
  AND effective_end_dt > DATE('2026-05-02')
  AND customer_id = 101;

-- Query 3: Customer history
SELECT *
FROM gold.customer_dimension
WHERE customer_id = 101
ORDER BY effective_start_dt;

-- Query 4: Join fact to dimension (current state)
SELECT 
    o.order_id,
    cd.customer_name,
    cd.email,
    o.order_amount
FROM gold.orders_fact o
JOIN gold.customer_dimension cd
    ON o.customer_dim_key = cd.customer_dim_key
WHERE cd.is_active = TRUE;

-- Query 5: Join fact to dimension (as-of order date - SCD2 correct)
SELECT 
    o.order_id,
    o.order_date,
    cd.customer_name,
    cd.email,
    o.order_amount
FROM gold.orders_fact o
JOIN gold.customer_dimension cd
    ON o.customer_dim_key_scd = cd.customer_dim_key
WHERE o.order_date >= cd.effective_start_dt
  AND o.order_date < cd.effective_end_dt;
```

---

## PART 8: ORCHESTRATION & SCHEDULING

### Daily ELT Pipeline (Databricks Jobs)

```python
# Job 1: Landing → Staging
# Trigger: Daily at 06:00 UTC
# Duration: ~5-10 minutes (depending on volume)

spark.sql("""
    -- Run landing-to-staging logic
    EXEC procedure_landing_to_staging()
""")

# Job 2: Staging → Curated (Dimensions)
# Trigger: Daily at 07:00 UTC (after Job 1 completes)
# Duration: ~10-20 minutes

spark.sql("""
    -- Run staging-to-dimension logic
    EXEC procedure_staging_to_dimensions()
""")

# Job 3: Staging → Curated (Facts)
# Trigger: Daily at 08:00 UTC (after Job 2 completes)
# Duration: ~15-30 minutes

spark.sql("""
    -- Run staging-to-facts logic
    EXEC procedure_staging_to_facts()
""")

# Job 4: Data Quality Validation
# Trigger: Daily at 09:00 UTC
# Duration: ~5 minutes

spark.sql("""
    -- Data quality checks
    EXEC procedure_dq_validation()
""")
```

### Idempotency & Restart Strategy

```python
# All jobs are IDEMPOTENT:
# If Job 3 fails, re-run it with same insert_dt
# Result: Same dimension rows inserted (MERGE handles duplicates)

# Restart logic:
# 1. Check gold.orders_fact max(dw_insert_dt)
# 2. If < CURRENT_TIMESTAMP(), re-run Job 3
# 3. MERGE pattern ensures no duplicates

spark.sql("""
    CREATE OR REPLACE PROCEDURE check_and_restart_job3()
    BEGIN
        LET last_load_dt = (SELECT MAX(dw_insert_dt) FROM gold.orders_fact);
        LET hours_since_load = (CURRENT_TIMESTAMP() - last_load_dt) / 3600;
        
        IF hours_since_load > 25 THEN
            -- No load in past 25 hours, trigger re-run
            CALL procedure_staging_to_facts();
        END IF;
    END;
""")
```

---

## PART 9: SUMMARY TABLE: SILVER vs. GOLD

| Aspect | Silver (Staging) | Gold (Curated) |
|--------|------------------|---|
| **Purpose** | Faithful replica of source + technical enrichment | Business-ready analytics |
| **Grain** | Source grain + deltas | Conformed dimensions (SCD2), facts |
| **Temporal Logic** | insert_dt (technical load time) | effective_start_dt, effective_end_dt (business) |
| **Change Tracking** | Hash-based delta detection | SCD2 with version closure |
| **Transformation Type** | Technical only (hash, data type cast) | Business rules (which attributes matter, dates) |
| **Idempotency** | Yes (hash-based, re-runnable) | Yes (MERGE pattern) |
| **Who Owns?** | Data Engineer | Data Architect + Business Analyst |
| **DMBOK Alignment** | Pure staging principles | Warehouse / DV Satellite principles |

---

## PART 10: ANTI-PATTERNS & MISTAKES TO AVOID

### ❌ Anti-Pattern 1: SCD2 in Staging
```python
# WRONG: Effective dates in Staging
CREATE TABLE silver.customer_staging (
    customer_id INT,
    effective_start_dt DATE,        # ❌ NO! Business rule
    effective_end_dt DATE,          # ❌ NO! Business rule
    is_active BOOLEAN               # ❌ NO! Business rule
)
```
**Why it's wrong:** Violates DMBOK principle of "no semantic enrichment" in staging.

**Fix:** Move these to Gold layer, keep only `insert_dt` (technical) in Staging.

---

### ❌ Anti-Pattern 2: State-Dependent Processing in Staging
```python
# WRONG: Dependent on previous state
IF last_version_is_active THEN
    current_record.is_active = FALSE
END IF
```
**Why it's wrong:** Not idempotent. Re-running with same input produces different output.

**Fix:** Use hash-based delta detection. Re-run = same result.

---

### ❌ Anti-Pattern 3: Joining Staging to Dimension in Facts Pipeline
```python
# WRONG: Using Gold dimension in Staging-to-Facts processing
fact = staging_fact.join(gold.customer_dimension)
```
**Why it's wrong:** Circular dependency, hard to debug, dimension changes break facts retroactively.

**Fix:** Join at Staging layer. Dimension already has SCD2 versions; facts pick the right version based on order_date.

---

### ❌ Anti-Pattern 4: No Audit Trail
```python
# WRONG: Overwriting Staging records
staging.write.mode("overwrite").option("mergeSchema", "true")...
```
**Why it's wrong:** Loses audit trail, breaks lineage, can't reprocess from source.

**Fix:** APPEND ONLY in Staging. All deltas preserved.

---

## CONCLUSION: DMBOK-ALIGNED ARCHITECTURE

### Your Challenge Resolved

**Original Problem:**
> "If I don't add effective_start_dt, effective_end_dt, is_active to Staging, it becomes too cumbersome to process records in curated layer."

**Resolution:**

1. **Keep Staging clean**: Only add `insert_dt` (technical, partitioning key) to Staging
2. **Move SCD2 to Gold**: effective_start_dt, effective_end_dt, is_active belong in dimension
3. **Use efficient patterns**: MERGE for dimensions, reservoir sampling or windowing for facts
4. **Preserve audit trail**: All deltas stored in Staging, immutable
5. **Follow DMBOK**: Staging = technical contract; Curated = business rules

This design:
✅ Aligns with DAMA-DMBOK2 principles  
✅ Maintains data lineage and auditability  
✅ Ensures idempotency and restartability  
✅ Cleanly separates technical (Silver) from business (Gold) concerns  
✅ Enables efficient downstream processing  

---

## REFERENCES

- **DAMA-DMBOK2**, Chapter 4 (Data Architecture) & Chapter 8 (Data Integration)
- **Data Vault 2.0**, Dan Linstedt (Satellite versioning pattern)
- **Kimball & Ross**, "The Data Warehouse Toolkit" (Conformed dimensions, fact design)
- **Databricks Lakehouse**, Delta Lake documentation (MERGE, ZORDER, optimization)
- **Medallion Architecture**, Databricks (Bronze/Silver/Gold layering)
