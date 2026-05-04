# VISUAL REFERENCE GUIDE: Staging Layer Architecture

---

## FIGURE 1: Layered Architecture Overview

```
LANDING (Bronze) → STAGING (Silver) → CURATED (Gold) → BI/Analytics
┌──────────────────────────────────────────────────────────────────────┐
│                                                                      │
│  Landing              Staging              Curated                   │
│  (Bronze)            (Silver)              (Gold)                    │
│  ────────────────────────────────────────────────────────────────   │
│                                                                      │
│  ✓ Raw data          ✓ Audit trail        ✓ Conformed               │
│  ✓ No transform      ✓ Technical only     ✓ Business rules          │
│  ✓ Daily refresh     ✓ Hash-based deltas  ✓ SCD2 dimensions         │
│  ✓ Source fidelity   ✓ Append-only        ✓ Facts & dimensions      │
│                      ✓ Idempotent         ✓ Query-optimized         │
│                                                                      │
│  DMBOK:             DMBOK:               DMBOK:                     │
│  "Technical         "Technical only"     "Business rules"           │
│  replica"           "Source faithful"    "Conformed dims"           │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

---

## FIGURE 2: Data Flow with Timestamps

```
Source System (CRM/ERP/OMS)
         │
         │ extraction_dt = 2026-05-04 10:30:00
         ▼
    ┌─────────────────┐
    │  Landing        │
    │  (Full daily)   │ Record: [101, Alice, alice@new.com]
    │  customer_id,   │
    │  name, email    │
    └────────┬────────┘
             │
      1. Hash all attributes:
         MD5(...) = HASH_NEW
             │
      2. Compare hash to Staging:
         Last known hash = HASH_OLD
         HASH_NEW ≠ HASH_OLD → UPDATE delta
             │
             ▼
    ┌─────────────────────────────────┐
    │  Staging (Silver)               │ Timestamp: insert_dt = 2026-05-04
    │  ─────────────────────────────  │ (Technical: when added to Staging)
    │  PK: surrogate_key = 456        │
    │  ├─ customer_id = 101           │
    │  ├─ source_hash = HASH_NEW      │
    │  ├─ change_type = 'U'           │
    │  ├─ is_deleted = FALSE          │
    │  ├─ insert_dt = 2026-05-04      │ ← Technical (Staging load time)
    │  ├─ email = alice@new.com       │
    │  └─ process_id = UUID-12345     │
    └────────┬────────────────────────┘
             │
      3. Apply SCD2 business logic:
         - Close old version (end_dt = 2026-05-03)
         - Insert new version (start_dt = 2026-05-04)
             │
             ▼
    ┌──────────────────────────────────┐
    │  Gold Dimension (Customer)       │
    │  ───────────────────────────────  │
    │  ├─ customer_dim_key = 45       │  Version 1 (old):
    │  ├─ customer_id = 101           │  ├─ effective_start_dt = 2026-05-01
    │  ├─ email = alice@old.com       │  ├─ effective_end_dt = 2026-05-03
    │  ├─ effective_start_dt = 2026...│  ├─ is_active = FALSE
    │  ├─ effective_end_dt = 2026-... │  │
    │  ├─ is_active = TRUE/FALSE      │  └─ Version 2 (current):
    │  └─ change_type = 'U'           │  ├─ effective_start_dt = 2026-05-04
    │                                 │  ├─ effective_end_dt = 9999-12-31
    │                                 │  └─ is_active = TRUE
    └──────────────────────────────────┘
             │
             ▼
        Business Queries
        ┌─ What was the customer's email on 2026-05-02?
        │  → Query version with 2026-05-02 between start/end dates
        │
        └─ What is the current customer state?
           → Query version with is_active = TRUE
```

---

## FIGURE 3: Staging Table Grain (Append-Only Audit Trail)

```
Customer 101 History in Staging:

Day 1 (2026-05-01):
┌──────────────────────────────────────────────────────────┐
│ surrogate_key │ change_type │ insert_dt  │ source_hash   │
├──────────────┼─────────────┼────────────┼───────────────┤
│ 1            │ I           │ 2026-05-01 │ HASH_ORIGINAL │
│              │ (INSERT)    │            │ (alice@...    │
│              │             │            │  account=ok)  │
└──────────────────────────────────────────────────────────┘

Day 2 (2026-05-02):  (email changed)
┌──────────────────────────────────────────────────────────┐
│ surrogate_key │ change_type │ insert_dt  │ source_hash   │
├──────────────┼─────────────┼────────────┼───────────────┤
│ 1            │ I           │ 2026-05-01 │ HASH_ORIGINAL │
│ 2            │ U           │ 2026-05-02 │ HASH_EMAIL_CHG│
│              │ (UPDATE)    │            │ (alice.j@...  │
│              │             │            │  account=ok)  │
└──────────────────────────────────────────────────────────┘

Day 3 (2026-05-03):  (status changed)
┌──────────────────────────────────────────────────────────┐
│ surrogate_key │ change_type │ insert_dt  │ source_hash   │
├──────────────┼─────────────┼────────────┼───────────────┤
│ 1            │ I           │ 2026-05-01 │ HASH_ORIGINAL │
│ 2            │ U           │ 2026-05-02 │ HASH_EMAIL_CHG│
│ 3            │ U           │ 2026-05-03 │ HASH_STATUS_CHG
│              │ (UPDATE)    │            │ (alice.j@...  │
│              │             │            │  account=susp)│
└──────────────────────────────────────────────────────────┘

Day 4 (2026-05-04):  (no change)
┌──────────────────────────────────────────────────────────┐
│ surrogate_key │ change_type │ insert_dt  │ source_hash   │
├──────────────┼─────────────┼────────────┼───────────────┤
│ 1            │ I           │ 2026-05-01 │ HASH_ORIGINAL │
│ 2            │ U           │ 2026-05-02 │ HASH_EMAIL_CHG│
│ 3            │ U           │ 2026-05-03 │ HASH_STATUS_CHG
│ (no new rows │             │            │               │
│  for day 4)  │             │            │               │
└──────────────────────────────────────────────────────────┘

KEY INSIGHTS:
✓ Each delta = new row with new surrogate_key
✓ Original rows (row 1) NEVER modified (audit trail)
✓ insert_dt tells us WHEN we detected this change
✓ source_hash proves WHAT changed (idempotent)
✓ Re-running same day = detects same hash = same deltas
```

---

## FIGURE 4: Delta Classification Logic (Decision Tree)

```
                    ┌─── Landing Data for Customer 101
                    │
        ┌───────────┴──────────────┐
        │                          │
    Customer 101 in          Customer 101
    Staging?                 NOT in Staging?
        │                          │
        ▼                          ▼
    ┌─────────────────┐       ┌─────────────────┐
    │ Has hash        │       │ NEVER been      │
    │ CHANGED?        │       │ in Staging      │
    └─────────────────┘       │                 │
        │                     └─────────────────┘
    ┌───┴───┐                     │
    │       │                     ▼
   YES      NO               ┌─────────────┐
    │       │                │ INSERT (I)  │
    ▼       ▼                │ New customer│
  ┌──┐  ┌──┐                └─────────────┘
  │UP│  │NO│
  │DA│  │CH│                Example:
  │TE│  │AN│                Customer 104 appears
  │ │   │GE│                in Landing for first time
  │(U)  │(─)│                → Append to Staging with
  └──┘  └──┘                   change_type='I'
   │      │
   │      └──► Keep processing, detect
   │          other deltas
   │
   └──► Example:
       Customer 101 hash changed
       from HASH_A → HASH_B
       → Append to Staging with
         change_type='U'

                    ┌─── Staging Data
                    │
        ┌───────────┴──────────────┐
        │                          │
    Customer 101 in          Customer 101
    Landing?                 NOT in Landing?
        │                          │
        ▼                          ▼
    (already handled           ┌─────────────┐
     above)                    │ DELETE (D)  │
                               │ Customer    │
                               │ removed from│
                               │ source      │
                               └─────────────┘

                               Example:
                               Customer 101 was
                               in Staging but
                               NOT in today's
                               Landing
                               → Append to Staging with
                                  change_type='D'
```

---

## FIGURE 5: Staging vs. Gold - Responsibility Matrix

```
┌─────────────────────────────────────────────────────────────────────┐
│                    WHO OWNS WHAT?                                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  QUESTION                         ANSWERED IN    OWNER             │
│  ─────────────────────────────────────────────────────────         │
│                                                                     │
│  What data changed?               Staging        Data Engineer     │
│  (Detected via hash)              silver.*.      (Technical)       │
│  ├─ change_type = I/U/D           source_hash                      │
│  ├─ is_deleted = TRUE             is_deleted                       │
│  └─ insert_dt = when found                                         │
│                                                                     │
│  Which version is                 Gold Dimension Data Architect    │
│  currently active?                effective_end_dt (Business)      │
│  What was the state               is_active                        │
│  on a specific date?              effective_start_dt               │
│  When did the change              (all SCD2 logic)                 │
│  become effective?                                                 │
│                                                                     │
│  Which attributes                 Gold Dimension Data Architect +  │
│  are "tracked"?                   (Business Rules) Business Analyst│
│  (trigger a version)              change_type (document)           │
│                                   (which attr define version?)     │
│                                                                     │
│  How should deletes               Gold Dimension Data Architect +  │
│  be handled?                      (Governance) Data Steward        │
│  ├─ Hard delete?                                                   │
│  ├─ Soft delete (flag)?                                            │
│  └─ Logical end date?                                              │
│                                                                     │
│  How long keep                    Gold Table Properties Data       │
│  historical versions?             (Retention Policy)  Governance   │
│  ├─ 7 years?                      retention = "7 years"            │
│  ├─ Indefinitely?                                                  │
│  └─ Until account closes?                                          │
│                                                                     │
│  Can we replay from                Yes (if raw Staging             Data Architect
│  source if rules                   data preserved)                 (Replayability)
│  change?                           → Re-process with new           Architecture
│                                     SCD2 rules applied
│
└─────────────────────────────────────────────────────────────────────┘

KEY PRINCIPLE:
🔑 Staging answers TECHNICAL questions (what, when, how much)
🔑 Gold answers BUSINESS questions (effective dates, versions, active state)
```

---

## FIGURE 6: Daily Job Execution Timeline

```
                            IDEMPOTENT JOBS

Time        │  Job 1           Job 2          Job 3           Job 4
──────────────┼──────────────────────────────────────────────────────────

06:00 UTC   │  ▲
            │  │ Landing → Staging
            │  │ • Read Landing (full refresh)
            │  │ • Hash all attrs
            │  │ • Detect I/U/D deltas
            │  │ • Append to Staging
            │  │ • ZORDER optimize
            │  ▼
            │  5-10 min

07:00 UTC   │           ▲
            │           │ Staging → Dimension
            │           │ (Today's deltas only: WHERE insert_dt=TODAY)
            │           │ • Read deltas from insert_dt=TODAY
            │           │ • Classify I/U/D
            │           │ • Close old versions (end_dt=yesterday)
            │           │ • Insert new versions (start_dt=today)
            │           │ • MERGE into Dimension (idempotent)
            │           ▼
            │           10-20 min

08:00 UTC   │                   ▲
            │                   │ Staging → Facts
            │                   │ (Today's deltas: WHERE insert_dt=TODAY)
            │                   │ • Read order deltas from insert_dt=TODAY
            │                   │ • Join to dim (current + SCD2 versions)
            │                   │ • Calculate measures
            │                   │ • MERGE into Facts (idempotent)
            │                   ▼
            │                   15-30 min

09:00 UTC   │                           ▲
            │                           │ DQ Validation
            │                           │ • Check dup active records
            │                           │ • Verify SCD2 date logic
            │                           │ • Validate row counts
            │                           │ • Alert if failures
            │                           ▼
            │                           5 min

09:30 UTC   │                                 ✓ All jobs done
            │                                 ✓ Reports refresh
            │                                 ✓ BI dashboards ready


FAILURE SCENARIO (Job 3 fails at 08:15):

08:15 UTC   │                   ✗ ERROR: Fact join failed
            │                   Restart interval: 1 hour
            │                   
09:15 UTC   │                   RETRY: Job 3 re-runs
            │                   • Same staging deltas (insert_dt=TODAY)
            │                   • MERGE re-processes (no duplicates)
            │                   • Same final fact rows
            │                   • Result: IDEMPOTENT
            │
09:30 UTC   │                   ✓ Job 3 succeeds
            │                   ✓ Manual intervention: NO
            │                   ✓ Data: CONSISTENT
```

---

## FIGURE 7: Hash-Based Delta Detection (Idempotency Proof)

```
SCENARIO: Re-run Landing → Staging job on same day (May 4)

First Run (May 4, 06:00):
┌─ Landing [101, Alice, alice@new.com, 555-1001, ...]
│
├─ Hash: MD5(...) = HASH_ABC123
│
├─ Compare to Staging historical records:
│  └─ Staging [101, HASH_XYZ789, insert_dt=2026-05-03]
│     HASH_ABC123 ≠ HASH_XYZ789 → UPDATE detected
│
└─ Result: Append to Staging
   [surrogate_key=456, customer_id=101, source_hash=HASH_ABC123,
    change_type='U', insert_dt=2026-05-04]

─────────────────────────────────────────────────────────────────────

EXACT SAME JOB RE-RUN (May 4, 08:00 - e.g., after failure):

First Run (May 4, 08:00):
┌─ Landing [101, Alice, alice@new.com, 555-1001, ...]
│  (Unchanged - full daily refresh always has same source data)
│
├─ Hash: MD5(...) = HASH_ABC123 ← SAME HASH
│
├─ Compare to Staging:
│  └─ Now includes [surrogate_key=456, customer_id=101, 
│                   source_hash=HASH_ABC123, insert_dt=2026-05-04]
│     HASH_ABC123 = HASH_ABC123 → NO CHANGE (no new delta)
│
└─ Result: NO NEW ROWS appended to Staging
   (or if we use LEFT_ANTI join, existing hash is filtered out)

─────────────────────────────────────────────────────────────────────

CONCLUSION:
✓ Hash is deterministic (same input → same hash)
✓ Re-run with same Landing data → same hash → same deltas detected
✓ IDEMPOTENT: Multiple runs = same Staging rows appended
✓ No duplicates (hash-match prevents re-appending)
✓ Can safely retry failed jobs without manual cleanup
```

---

## FIGURE 8: Dimension Versioning Example (Full Trace)

```
CUSTOMER 101: THREE-VERSION HISTORY

Day 1 (2026-05-01) - Initial Insert:
┌─ Landing: [101, Alice Johnson, alice@example.com, Active]
├─ Staging: [sk=1, change_type='I', insert_dt=2026-05-01]
└─ Dimension:
   ┌────────────┬─────────┬─────────┬──────────┐
   │ dim_key    │ name    │ email   │ version  │
   ├────────────┼─────────┼─────────┼──────────┤
   │ 1          │ Alice   │ alice@  │ active   │
   │            │ Johnson │ example │ ✓        │
   │            │         │ .com    │          │
   └────────────┴─────────┴─────────┴──────────┘
   effective_start_dt: 2026-05-01
   effective_end_dt: 9999-12-31
   is_active: TRUE

Day 2 (2026-05-02) - Email Update:
┌─ Landing: [101, Alice Johnson, alice.j@example.com, Active]
├─ Staging: [sk=2, change_type='U', insert_dt=2026-05-02]
│           (hash changed: alice@ vs alice.j@)
└─ Dimension: (SCD2 logic applied)
   ┌────────────┬─────────┬──────────────┬──────────┐
   │ dim_key    │ name    │ email        │ status   │
   ├────────────┼─────────┼──────────────┼──────────┤
   │ 1          │ Alice   │ alice@       │ CLOSED   │
   │ (old v)    │ Johnson │ example.com  │ ✗        │
   ├────────────┼─────────┼──────────────┼──────────┤
   │ 2          │ Alice   │ alice.j@     │ active   │
   │ (new v)    │ Johnson │ example.com  │ ✓        │
   └────────────┴─────────┴──────────────┴──────────┘
   
   Version 1: effective_end_dt = 2026-05-01, is_active = FALSE
   Version 2: effective_start_dt = 2026-05-02, is_active = TRUE

Day 3 (2026-05-03) - Status Update:
┌─ Landing: [101, Alice Johnson, alice.j@example.com, Inactive]
├─ Staging: [sk=3, change_type='U', insert_dt=2026-05-03]
│           (hash changed: Active → Inactive)
└─ Dimension:
   ┌────────────┬─────────┬──────────────┬──────────┐
   │ dim_key    │ name    │ email        │ status   │
   ├────────────┼─────────┼──────────────┼──────────┤
   │ 1          │ Alice   │ alice@       │ CLOSED   │
   │ (v1)       │ Johnson │ example.com  │ ✗        │
   ├────────────┼─────────┼──────────────┼──────────┤
   │ 2          │ Alice   │ alice.j@     │ CLOSED   │
   │ (v2)       │ Johnson │ example.com  │ ✗        │
   ├────────────┼─────────┼──────────────┼──────────┤
   │ 3          │ Alice   │ alice.j@     │ active   │
   │ (v3)       │ Johnson │ example.com  │ ✓        │
   └────────────┴─────────┴──────────────┴──────────┘
   
   V1: eff_start = 2026-05-01, eff_end = 2026-05-01, is_active = FALSE
   V2: eff_start = 2026-05-02, eff_end = 2026-05-02, is_active = FALSE
   V3: eff_start = 2026-05-03, eff_end = 9999-12-31, is_active = TRUE

HISTORICAL QUERIES:

Q1: "What was Alice's email on May 2?"
→ SELECT * WHERE customer_id=101
  AND effective_start_dt <= '2026-05-02'
  AND effective_end_dt >= '2026-05-02'
→ Result: alice.j@example.com (Version 2, updated May 2)

Q2: "Who was active as of May 3?"
→ SELECT * WHERE effective_start_dt <= '2026-05-03'
  AND effective_end_dt >= '2026-05-03'
  AND is_active = TRUE
→ Result: alice.j@example.com, Status=Inactive (Version 3)

Q3: "Show me current customer state"
→ SELECT * WHERE is_active = TRUE
→ Result: alice.j@example.com, Status=Inactive (Version 3)

Q4: "Show me customer's complete change history"
→ SELECT * WHERE customer_id=101 ORDER BY effective_start_dt
→ Result: All 3 versions showing evolution
```

---

## FIGURE 9: Column Mapping: Source → Staging → Gold

```
Source System        Landing              Staging              Gold
(CRM)               (Bronze)             (Silver)             (Dimension)
─────────────────────────────────────────────────────────────────────────

cust_id         ──►  customer_id     ──► customer_id      ──► customer_id
                                        (PK from source)      (business key)

cust_name       ──►  customer_name   ──► customer_name    ──► customer_name
                                        (attribute)           (attribute)

email           ──►  email           ──► email            ──► email
                                        (attribute)           (attribute)

address         ──►  address         ──► address          ──► address
                                        (attribute)           (attribute)

cust_status     ──►  account_status  ──► account_status   ──► account_status
                                        (attribute)           (dimension)

last_update_ts  ──► extraction_dt    ──► extract_dt       ──► (not in gold)
                     (metadata)          (metadata)           (use eff dates)

                     (─)             ──► source_hash      ──► (not in gold)
                                        (technical)           (audit only)

                     (─)             ──► source_hash      ──► (not in gold)
                                        (technical)           (tracking)

                     (─)             ──► change_type      ──► change_type
                                        (I/U/D detected)      (U/D indicator)

                     (─)             ──► is_deleted       ──► (soft deleted)
                                        (logical flag)        via eff_end_dt

                     (─)             ──► insert_dt        ──► (not in gold)
                                        (PARTITION KEY)       (use eff dates)

                     (─)             ──► surrogate_key    ──► customer_dim_key
                                        (audit trail PK)      (DW PK)

                                         (─)              ──► effective_start_dt
                                                              (SCD2, business)

                                         (─)              ──► effective_end_dt
                                                              (SCD2, business)

                                         (─)              ──► is_active
                                                              (SCD2, business)

                                         (─)              ──► dw_insert_dt
                                                              (DW metadata)

                                         (─)              ──► dw_update_dt
                                                              (DW metadata)

LEGEND:
──►  Direct pass-through (same value, possibly renamed)
(─)  Generated/enriched in that layer
```

---

## FIGURE 10: Troubleshooting Decision Tree

```
PROBLEM: Staging row count unexpected

                          ┌─────────────────┐
                          │ Check Staging   │
                          │ row count for   │
                          │ today           │
                          └────────┬────────┘
                                   │
                  ┌────────────────┼────────────────┐
                  │                │                │
            Lower than      Higher than      Much higher
            expected        expected         than expected
                  │                │                │
                  ▼                ▼                ▼

        Landing may have    More deltas       Possible
        fewer changes       detected than     duplicate
        than normal         expected          rows in
                                              Staging
                  │                │                │
                  ├─ Check Landing ├─ Verify       └─ Check for
                  │  row count     │  hash logic     surrogate_key
                  │                │  (email        duplicates
                  └─ Verify        │   changes,
                     hash logic    │   status
                     hasn't        │   changes)
                     changed       │
                                   └─ Did source
                                      system bulk
                                      update?


PROBLEM: Dimension is showing duplicate active records

                    ┌──────────────────────┐
                    │ SELECT customer_id,  │
                    │ COUNT(*) as cnt      │
                    │ FROM gold.customer   │
                    │ WHERE is_active=TRUE │
                    │ GROUP BY customer_id │
                    │ HAVING cnt > 1       │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ For EACH duplicate:  │
                    │ Check if this is a   │
                    │ MERGE bug or logic   │
                    │ error in close-old   │
                    │ version step         │
                    └──────────┬───────────┘
                               │
                ┌──────────────┴──────────────┐
                │                             │
            Multiple        Old version
            new inserts     NOT closed
            same day        properly
                │                │
                ▼                ▼

        Staging has    Go back to
        multiple       Staging-to-Gold
        INSERT deltas  job log, verify:
        for same       ├─ OLD version
        customer       │  got effective_
                       │  end_dt = yesterday
                       ├─ NEW version got
                       │  effective_start_dt
                       │  = today
                       └─ Re-run dimension
                          job with fix


PROBLEM: Fact table has NULL dimension keys

                    ┌──────────────────────┐
                    │ SELECT COUNT(*) cnt  │
                    │ FROM gold.orders_fact│
                    │ WHERE customer_dim_key │
                    │       IS NULL        │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Check if customer    │
                    │ exists in Dimension  │
                    │ as active (is_active │
                    │ = TRUE)              │
                    └──────────┬───────────┘
                               │
                ┌──────────────┴──────────────┐
                │                             │
         Customer exists    Customer does
         in Dimension       NOT exist in
         but join failed    Dimension
                │                │
                ▼                ▼

        JOIN logic error:  Data quality
        ├─ Check join     issue:
        │  condition      ├─ Order has
        │  (should be     │  customer_id
        │  on customer_id)│  that doesn't
        └─ Verify both   │  exist in
           tables have    │  source CRM
           matching       └─ Add validation
           customer_id       rule: orders
           datatype          MUST have
                             valid customer


PROBLEM: Can't re-run yesterday's job (idempotency broken)

                    ┌──────────────────────┐
                    │ Re-run Job 3 with    │
                    │ insert_dt = yesterday │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Check if MERGE       │
                    │ prevented duplicates │
                    │ (should be idempotent│
                    │ result)              │
                    └──────────┬───────────┘
                               │
                ┌──────────────┴──────────────┐
                │                             │
            MERGE worked    Duplicate fact
            (same          rows found
            result)        (merge didn't work)
                │                │
                ▼                ▼

        ✓ Idempotency    Check MERGE
          preserved      logic:
        ✓ Can safely     ├─ ON clause
          retry          │  (what's PK?)
                         ├─ WHEN MATCHED
                         │  should UPDATE
                         └─ Check if
                            order_fact_key
                            auto-increment
                            prevents
                            duplicates
```

---

## FIGURE 11: Performance Tuning Checklist

```
STAGING LAYER OPTIMIZATION:

┌─ Partitioning
│  ├─ PARTITIONED BY (insert_dt)
│  │  └─ Enables partition pruning (filter WHERE insert_dt = TODAY)
│  └─ Verify: SELECT count(DISTINCT insert_dt) → should be incremental
│
├─ Clustering
│  ├─ CLUSTER BY (customer_id, insert_dt)
│  │  └─ Co-locates all changes for same customer → faster lookups
│  └─ Verify: DESCRIBE FORMATTED silver.customer_staging
│
├─ Optimization
│  ├─ Daily: spark.sql("OPTIMIZE silver.customer_staging ZORDER BY (customer_id, insert_dt)")
│  │  └─ Compacts small files, optimizes Z-order
│  └─ Monitor: SELECT count(*) FROM silver.customer_staging
│
├─ Data Quality
│  ├─ Monitor: Staging row count by insert_dt (trend)
│  └─ Alert: If insert_dt > 10x expected deltas
│
└─ Retention
   ├─ Default: delta.retention.duration = "30 days" (keep old versions)
   └─ Rationale: Can replay/re-process if needed


DIMENSION LAYER OPTIMIZATION:

┌─ Indexing Strategy
│  ├─ No explicit indexes (Delta handles)
│  └─ Rely on: CLUSTER BY (customer_id, effective_start_dt)
│
├─ Common Queries
│  ├─ "Current state" → Index on is_active
│  │  └─ WHERE is_active = TRUE
│  ├─ "As-of date" → Index on effective_start/end_dt
│  │  └─ WHERE eff_start <= date AND eff_end > date
│  └─ "Full history" → Index on customer_id
│      └─ WHERE customer_id = ?
│
├─ View Creation
│  ├─ CREATE VIEW gold.vw_customer_current AS
│  │  SELECT * FROM gold.customer_dimension
│  │  WHERE is_active = TRUE
│  └─ Simplifies downstream queries
│
├─ Materialized History
│  ├─ CREATE TABLE gold.customer_history_mv AS
│  │  SELECT customer_id, effective_start_dt, changed_attribute, ...
│  │  FROM gold.customer_dimension_history
│  └─ Denormalized view of changes
│
└─ Monitoring
   ├─ Track: Dimension growth (versions per customer)
   │  └─ SELECT customer_id, COUNT(*) FROM gold.customer_dimension GROUP BY 1
   ├─ Track: Active ratio (% of rows is_active=TRUE)
   │  └─ SELECT COUNT(*)/SUM(1.0) FROM gold.customer_dimension WHERE is_active=TRUE
   └─ Alert: If any customer has > 100 versions (possible bug)


FACT TABLE OPTIMIZATION:

┌─ Partitioning
│  ├─ PARTITIONED BY (order_date_key)
│  │  └─ Time-series queries most common
│  └─ Verify: WHERE order_date_key >= 20260501
│
├─ Clustering
│  ├─ CLUSTER BY (customer_dim_key, order_date_key)
│  │  └─ Enables fast "orders by customer & date range"
│  └─ Trade-off: Single clustering key vs. multi-column
│
├─ Measure-Column Encoding
│  ├─ Use smallest numeric types
│  │  ├─ order_quantity: INT (not LONG)
│  │  ├─ order_extended_price: DECIMAL(12, 2) (not DECIMAL(20, 4))
│  │  └─ order_tax_amount: DECIMAL(10, 2)
│  └─ Saves storage, improves compression
│
├─ Fact Granularity
│  ├─ Confirm: One row per (order_id, order_line_id)
│  │  └─ SELECT COUNT(*), COUNT(DISTINCT order_id || order_line_id) → should match
│  └─ Prevents fact multiplication in joins
│
└─ Z-ORDER
   ├─ After loading: OPTIMIZE gold.orders_fact ZORDER BY (customer_dim_key, order_date_key)
   └─ Re-order columns for faster multi-dimensional queries
```

---

**END OF VISUAL REFERENCE GUIDE**

Use this guide as a companion to the main architecture document for quick visual understanding and troubleshooting.
