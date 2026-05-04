# QUICK REFERENCE: DMBOK-Aligned Staging Layer Architecture

**Document Purpose:** Quick reference guide for Lead Data Architects and Design Authority discussions

---

## THE CHALLENGE & RESOLUTION (One-Pager)

### Your Challenge
> "Adding effective_start_dt, effective_end_dt, is_active to Staging looks like SCD2, which violates DMBOK staging principles. But without them, Gold layer processing becomes too cumbersome."

### The Resolution (Architecture Pattern)

```
┌─────────────────────────────────────────────────────────────────────────┐
│  LANDING (Bronze)                                                       │
│  ├─ Raw data from CRM, ERP, OMS                                        │
│  ├─ Full daily refresh                                                 │
│  └─ No transformation                                                  │
└──────────────────────┬──────────────────────────────────────────────────┘
                       │
                       │ Hash all attributes → Detect deltas (I/U/D)
                       ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STAGING (Silver) — YOUR LAYER                                          │
│  ├─ Append-only audit trail of deltas                                  │
│  ├─ Technical enrichment: insert_dt, source_hash, change_type         │
│  ├─ NO business logic (NO SCD2, NO effective dates)                   │
│  ├─ Hash-based delta detection (idempotent)                            │
│  └─ DMBOK-compliant: source fidelity + technical transforms only      │
└──────────────────────┬──────────────────────────────────────────────────┘
                       │
                       │ Apply business rules: SCD2 logic here
                       ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  CURATED (Gold) — BUSINESS LAYER                                        │
│  ├─ Conformed dimensions (SCD2 with versions)                          │
│  ├─ Business enrichment: effective_start_dt, effective_end_dt, is_active │
│  ├─ SCD2 logic: which attributes trigger versioning?                   │
│  ├─ MERGE-based loading (idempotent)                                   │
│  └─ Serves queries: current state + historical analysis                │
└─────────────────────────────────────────────────────────────────────────┘
```

### Key Insight

**SCD2 is NOT a technical transform, it's a BUSINESS RULE.**

| Aspect | Technical | Business |
|--------|-----------|----------|
| **Example** | Hash calculation, data type casting, null handling | Which attributes are "tracked"? What makes a version change? |
| **Decided By** | Data Engineer | Business Stakeholder + Data Architect |
| **Can be re-executed identically** | Yes (deterministic) | Only if business rules don't change |
| **DMBOK Home** | Staging (Silver) | Warehouse (Gold) / DV Satellite |

---

## DMBOK INTERPRETATION (For Design Authority)

### What DMBOK Says Staging Must Be

**DAMA-DMBOK2, Chapter 8 (Data Integration):**

| Principle | Your Staging ✓ | Violates With SCD2 ✗ |
|-----------|---|---|
| **Source Fidelity** | Hash validates all Landing attributes are present | No violation here |
| **No Semantic Enrichment** | insert_dt is technical, not business | effective_start_dt = business rule |
| **Technical-Only Transforms** | Hash, delta detection are deterministic | SCD2 versioning is state-dependent |
| **Auditability** | All deltas preserved, lineage clear | SCD2 obscures "what came from source?" |
| **Idempotency** | Re-run detects same deltas (hash-based) | SCD2 depends on prior state |

### DMBOK Reference
- **Chapter 4:** Data Architecture — separation of technical vs. business layers
- **Chapter 8:** Data Integration — staging layer principles
- **Chapter 11:** Data Warehousing — where SCD2 belongs
- **Chapter 14:** Data Governance — business rules ownership

---

## ARCHITECTURE DECISION RECORD (ADR)

### Decision: Where SCD2 Lives in Lakehouse

**Context:**  
Medallion Architecture with Bronze (Landing) → Silver (Staging) → Gold (Curated)

**Decision:**  
- ✅ **SCD2 versioning logic lives in GOLD (Curated) layer**
- ✅ **Staging layer adds ONLY technical timestamps (insert_dt, load_ts)**
- ✅ **Hash-based delta detection drives Staging updates**
- ✅ **MERGE pattern ensures idempotent Gold loading**

**Rationale:**  
1. **DMBOK Compliance**: Staging = technical contract with source system
2. **Auditability**: Deltas preserved in Staging, business rules applied downstream
3. **Replayability**: If business rules change, can re-process staging data
4. **Single Responsibility**: Staging = "what changed?", Gold = "what's the business impact?"

**Consequences:**
- ✓ Design Authority approval likely
- ✓ Staging tables remain lean (fewer columns)
- ✓ Easier to handle schema changes in source (only hash recalculation)
- ✓ Reusable staging data for multiple Gold models
- ✗ Gold layer needs more sophisticated MERGE logic
- ✗ Data engineers need SQL/PySpark expertise for dimension processing

---

## TABLE STRUCTURE COMPARISON

### ❌ WRONG: SCD2 in Staging (Violates DMBOK)

```sql
CREATE TABLE silver.customer_staging (
    customer_id INT,
    surrogate_key BIGINT,
    source_hash BINARY,
    change_type VARCHAR(1),
    -- ❌ Business rules in Staging:
    effective_start_dt DATE,        -- WRONG
    effective_end_dt DATE,          -- WRONG
    is_active BOOLEAN,              -- WRONG
    -- Technical metadata
    insert_dt DATE
);
```

**Issues:**
- Answers business question: "which version is active now?"
- Not idempotent (depends on prior versions)
- Violates DMBOK "no semantic enrichment" principle
- Design Authority will challenge

---

### ✅ CORRECT: Technical Enrichment Only in Staging (DMBOK-Aligned)

```sql
CREATE TABLE silver.customer_staging (
    -- Core identifiers
    customer_id INT NOT NULL,
    surrogate_key BIGINT NOT NULL,
    source_system_id VARCHAR(50),
    
    -- Delta detection (technical)
    source_hash BINARY NOT NULL,           -- Hash of all attributes
    change_type VARCHAR(1),                -- I/U/D classification
    is_deleted BOOLEAN DEFAULT FALSE,      -- Deletion marker
    
    -- Technical timestamps (for filtering & partitioning)
    insert_dt DATE NOT NULL,               -- When added to Staging (partition key)
    extract_dt TIMESTAMP,                  -- When extracted from Landing (lineage)
    load_ts TIMESTAMP,                     -- Spark job timestamp
    
    -- Source attributes (faithfully preserved)
    customer_name VARCHAR(100),
    email VARCHAR(100),
    ... (all source columns),
    
    -- Metadata
    row_number BIGINT,
    process_id VARCHAR(100),
    
    PRIMARY KEY (surrogate_key)
);
```

**Design Rationale:**
- ✓ No SCD2 logic (belongs in Gold)
- ✓ Technical enrichment only
- ✓ Hash-based delta detection (idempotent)
- ✓ DMBOK-compliant
- ✓ Meets Design Authority requirements

---

### ✅ SCD2 Logic Belongs HERE: Gold Dimension

```sql
CREATE TABLE gold.customer_dimension (
    -- Surrogate keys
    customer_dim_key BIGINT NOT NULL,      -- DW key
    customer_id INT NOT NULL,               -- Business key
    
    -- Attributes (source-faithful, no transformations)
    customer_name VARCHAR(100),
    email VARCHAR(100),
    ... (all tracked attributes),
    
    -- ✅ SCD2 metadata (business rules)
    effective_start_dt DATE NOT NULL,       -- When this version started
    effective_end_dt DATE NOT NULL,         -- When this version ended
    is_active BOOLEAN DEFAULT TRUE,         -- Current version?
    change_type VARCHAR(1),                 -- Which attributes changed?
    
    -- DW metadata
    dw_insert_dt TIMESTAMP NOT NULL,
    dw_update_dt TIMESTAMP NOT NULL,
    
    PRIMARY KEY (customer_dim_key)
);
```

**Business Rules Applied Here:**
- Which attributes trigger a new version? (Tracked vs. ignored)
- What is "effective_start_dt"? (Source timestamp? Load date? Business event date?)
- Who defines "active"? (Current version? Account status?)

---

## PROCESSING FLOW (High-Level)

### Daily Execution (Idempotent)

```
TIME: 06:00 UTC
┌─────────────────────────────────────────────────────────┐
│ Job 1: Landing → Staging                               │
│ • Read Landing (full daily refresh)                     │
│ • Hash all attributes                                   │
│ • Detect deltas (I/U/D) by comparing hashes            │
│ • Append to Staging (insert_dt = today)                │
│ • Runtime: ~5-10 minutes                                │
└──────────────┬──────────────────────────────────────────┘

TIME: 07:00 UTC
┌──────────────┬──────────────────────────────────────────┐
│ Job 2: Staging → Gold (Dimensions)                     │
│ • Read Staging deltas from today (insert_dt = today)  │
│ • Join to current dimension (is_active = TRUE)        │
│ • Classify: new customers (I), changed attrs (U),     │
│   deleted customers (D)                                │
│ • Close old versions (effective_end_dt = today - 1)   │
│ • Insert new versions (effective_start_dt = today)    │
│ • MERGE into Dimension (idempotent)                    │
│ • Runtime: ~10-20 minutes                              │
└──────────────┬──────────────────────────────────────────┘

TIME: 08:00 UTC
┌──────────────┬──────────────────────────────────────────┐
│ Job 3: Staging → Gold (Facts)                          │
│ • Read Staging order deltas from today               │
│ • Join to customer dimension (current + SCD2 versions)│
│ • Join to product dimension (as-of order_date)       │
│ • Calculate derived measures (extended_price, etc.)  │
│ • MERGE into Facts (idempotent)                       │
│ • Runtime: ~15-30 minutes                             │
└──────────────┬──────────────────────────────────────────┘

TIME: 09:00 UTC
┌──────────────┬──────────────────────────────────────────┐
│ Job 4: Data Quality Validation                        │
│ • Check for duplicate active records in Dimension    │
│ • Verify SCD2 date logic (start < end)              │
│ • Validate fact volume vs. expected                  │
│ • Runtime: ~5 minutes                                 │
└─────────────────────────────────────────────────────────┘
```

### Idempotency Pattern

**If Job 3 fails and you re-run on the same day:**

```python
# Job 3 reads TODAY's staging deltas (insert_dt = CURRENT_DATE())
staging_today = spark.sql("""
    SELECT *
    FROM silver.order_staging
    WHERE insert_dt = CURRENT_DATE()  # Same deltas
""")

# MERGE into Facts
spark.sql("""
    MERGE INTO gold.orders_fact AS fact
    USING staging_today
    ON fact.order_id = staging_today.order_id
      AND fact.order_line_id = staging_today.order_line_id
    
    WHEN MATCHED THEN UPDATE ...  # Updates same rows
    WHEN NOT MATCHED THEN INSERT ...  # No duplicates (PK prevents it)
""")

# Result: Re-run = same final state (idempotent)
```

---

## IMPLEMENTATION CHECKLIST

### Phase 1: Design & Approval (Week 1)
- [ ] Present architecture to Design Authority
- [ ] Align on staging layer scope (what layers do we need?)
- [ ] Define SCD2 business rules (which attributes trigger versioning?)
- [ ] Get sign-off on technical vs. business rule separation

### Phase 2: Schema & Infrastructure (Week 2)
- [ ] Create bronze, silver, gold schemas
- [ ] Define column lists for each staging table
- [ ] Create surrogate key sequences
- [ ] Set up Delta table properties (retention, clustering)
- [ ] Configure table-level access controls (ACLs)

### Phase 3: Staging Layer Implementation (Week 3)
- [ ] Implement landing-to-staging job (PySpark)
- [ ] Test hash-based delta detection
- [ ] Validate surrogate key uniqueness
- [ ] Monitor Staging table growth
- [ ] Set up Staging table optimization schedule (ZORDER)

### Phase 4: Curated Layer Implementation (Week 4-5)
- [ ] Implement staging-to-dimension job (with SCD2 logic)
- [ ] Implement staging-to-facts job
- [ ] Create MERGE logic for idempotency
- [ ] Test restart scenarios (re-run same day job)
- [ ] Validate dimension versioning logic

### Phase 5: Validation & Monitoring (Week 6)
- [ ] Set up data quality checks (null checks, duplicate checks, date logic)
- [ ] Create monitoring dashboards (row counts, processing time)
- [ ] Test end-to-end lineage (trace fact back to source)
- [ ] Load test with historical data
- [ ] Document runbooks for common failure scenarios

### Phase 6: Production Deployment (Week 7)
- [ ] Schedule daily jobs in Databricks Workflows
- [ ] Configure alerting (job failure, SLA breach)
- [ ] Create rollback procedures
- [ ] Document architecture in confluence/wiki
- [ ] Train data engineers on maintenance

---

## DECISION MATRIX: Technical Issues & Resolutions

### Issue 1: "How do we know which staging records to process in Gold?"

**Problem:** Staging has millions of rows. Dimension job should only process today's deltas.

**Solution:**
```python
# Filter to TODAY's deltas only (technical metadata)
staging_today = spark.sql("""
    SELECT *
    FROM silver.customer_staging
    WHERE insert_dt = CURRENT_DATE()  # Technical partition key
""")
```

**Key:** `insert_dt` is technical (when added to staging), not business.

---

### Issue 2: "What if an attribute changes multiple times in one day?"

**Scenario:** Customer email changes from `alice@old.com` → `alice@new.com` → `alice@final.com`

**Staging Result:**
```
insert_dt=2026-05-04, hash=HASH1 (original)
insert_dt=2026-05-04, hash=HASH2 (change 1)
insert_dt=2026-05-04, hash=HASH3 (change 2)
```

**Gold Dimension:**
```
effective_start_dt=2026-05-04, effective_end_dt=2026-05-04, email=alice@old.com
effective_start_dt=2026-05-04, effective_end_dt=2026-05-04, email=alice@new.com
effective_start_dt=2026-05-04, effective_end_dt=9999-12-31, email=alice@final.com
```

**Answer:** Dimension captures every hash change as a version (business can decide if intermediate versions matter).

---

### Issue 3: "How do we handle deletions?"

**Landing Day 1:** Customer 101 exists  
**Landing Day 2:** Customer 101 deleted from source

**Staging:**
```
insert_dt=2026-05-01, customer_id=101, change_type='I', is_deleted=FALSE
insert_dt=2026-05-02, customer_id=101, change_type='D', is_deleted=TRUE
```

**Gold Dimension:**
```
customer_dim_key=1, customer_id=101, effective_end_dt=2026-05-01, is_active=FALSE (old version)
customer_dim_key=2, customer_id=101, effective_end_dt=2026-05-01, is_active=FALSE (deletion marked)
```

**Business Logic:** Is the delete permanent, or can they re-activate?

---

### Issue 4: "What about slowly-changing reference tables that don't need SCD2?"

**Example:** Status codes (Active, Inactive, Suspended)

**Staging:** Not needed (reference data, manually maintained)  
**Gold:**
```sql
CREATE TABLE gold.status_reference (
    status_key INT,
    status_code VARCHAR(20),
    status_name VARCHAR(100),
    is_active BOOLEAN
) USING DELTA;
```

**Load:** Once (or UPSERT if it ever changes)

---

## QUESTIONS FOR DESIGN AUTHORITY

**Ask these to get buy-in:**

1. **Scope:** Which entities need SCD2 in Gold? (Customer, Product, Supplier, ...?)
2. **Tracked Attributes:** For each dimension, which attributes trigger versioning? (All? Only "key" ones?)
3. **Effective Date Definition:** What timestamp represents the "business event"? (Source system? Load date? Business date?)
4. **Retention:** How long do we keep historical versions? (7 years? Indefinitely?)
5. **Re-processing:** If SCD2 rules change, do we re-process staging data? (Major effort!)
6. **Performance:** Acceptable latency from landing to curated? (Real-time? Daily? Weekly?)

---

## GO/NO-GO CHECKLIST FOR DESIGN AUTHORITY

Before production deployment, Design Authority should confirm:

- [ ] **Staging Scope:** Silver layer adds only technical metadata (insert_dt, hash, change_type)
- [ ] **SCD2 Home:** Effective dates and version logic live in Gold, not Silver
- [ ] **DMBOK Alignment:** Staging = technical contract; Gold = business rules
- [ ] **Idempotency:** Re-running same-day job produces same result
- [ ] **Auditability:** All deltas preserved in Staging for lineage
- [ ] **Business Rules Defined:** Which attributes trigger versioning? (documented)
- [ ] **Monitoring:** Data quality checks and alerting configured
- [ ] **Documentation:** Runbooks and architecture documented

**Signature:** __________________  
**Date:** __________________  
**Authority:** Design Review Board  

---

## FURTHER READING

**DMBOK References:**
- DAMA International, "Data Management Body of Knowledge" (2nd ed.), Chapter 4, 8, 11

**Architecture References:**
- **Medallion Architecture:** https://databricks.com/blog/2022/06/24/how-to-build-a-robust-data-lakehouse-architecture.html
- **Data Vault 2.0:** Dan Linstedt, "Building a Scalable Data Warehouse with Data Vault 2.0"
- **Kimball DWH:** Ralph Kimball & Margy Ross, "The Data Warehouse Toolkit"

**Databricks Resources:**
- Delta Lake Optimization: https://docs.databricks.com/delta/best-practices.html
- MERGE Pattern: https://docs.databricks.com/delta/merge.html
- Change Data Feed: https://docs.databricks.com/delta/change-data-feed.html

---

**Last Updated:** May 2026  
**Version:** 1.0 (Final)  
**Owner:** Lead Data Architect  
**Audience:** Design Authority, Data Architects, Lead Data Engineers
