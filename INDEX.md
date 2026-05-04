# MASTER INDEX: Staging Layer Architecture Guidance

**Date Created:** May 4, 2026  
**Audience:** Lead Data Architects, Design Authority, Data Engineering Teams  
**Platform:** Databricks Lakehouse (Medallion Architecture)  
**Alignment:** DAMA-DMBOK2 Principles

---

## DOCUMENT OVERVIEW

This comprehensive guidance package addresses the architectural challenge of designing a DMBOK-compliant Staging (Silver) layer in a Databricks Lakehouse environment, with focus on resolving the tension between technical purity and operational efficiency.

### The Core Challenge

Your current approach adds `effective_start_dt`, `effective_end_dt`, and `is_active` to the Staging layer to simplify downstream curated layer processing. However, Design Authority challenges this as violating DMBOK principles of "no semantic enrichment" in staging.

### The Resolution

**SCD2 versioning logic belongs in the Gold (Curated) layer**, not in Silver (Staging). Staging adds only **technical metadata** (timestamps, hashes, change classification) that enables efficient delta detection and audit trail preservation—not business rules about effective dates.

---

## DOCUMENT SET (4 Files, ~3,200 lines, 128 KB)

### 📋 **1. QUICK REFERENCE** (467 lines)
**File:** `QUICK_REFERENCE.md`  
**Read Time:** 15-20 minutes  
**Audience:** Design Authority, Architects, Decision-Makers  

**Contains:**
- One-page architecture pattern (Bronze → Silver → Gold)
- DMBOK interpretation (what the standards actually say)
- Comparison: Wrong vs. Correct table structures
- Decision matrix: Technical vs. business rules
- Implementation checklist (6-week phased approach)
- Design Authority approval form

**When to Use:**
- Executive briefings
- Design Authority review meetings
- Quick reference during architecture discussions
- Onboarding new team members

**Key Takeaway:** SCD2 is a BUSINESS RULE, not a technical transformation. It belongs in Gold, not Silver.

---

### 📚 **2. MAIN ARCHITECTURE GUIDE** (983 lines)
**File:** `SCD2_Staging_Architecture_Guide.md`  
**Read Time:** 1-2 hours (detailed, in-depth)  
**Audience:** Data Architects, Lead Data Engineers  

**Contains:**
- **Part 1:** Landing-to-Staging table design (with rationale for every column)
- **Part 2:** Landing-to-Staging processing logic (Python code with comments)
- **Part 3:** Staging-to-Curated SCD2 processing (detailed pseudocode)
- **Part 4:** Dimension design in curated layer (full examples)
- **Part 5:** Fact table design & loading (conformed grain pattern)
- **Part 6:** Reference & lookup tables (TBUD pattern)
- **Part 7:** End-to-end example with data (Day 1 vs. Day 2 processing)
- **Part 8:** Orchestration & scheduling (daily job pipeline)
- **Part 9:** Summary tables (Silver vs. Gold comparison matrix)
- **Part 10:** Anti-patterns & mistakes to avoid (with explanations)
- **Conclusion:** How this resolves your challenge

**When to Use:**
- Detailed design reviews
- Team training sessions
- Architecture documentation
- Reference during implementation

**Key Insight:** The guide shows exactly how to implement all three layers without embedding business logic in Staging, using realistic examples with actual data transformations.

---

### 💻 **3. IMPLEMENTATION APPENDIX** (934 lines)
**File:** `APPENDIX_A_DDL_and_Code.md`  
**Read Time:** 1-2 hours (hands-on technical)  
**Audience:** Data Engineers, SQL/PySpark developers  

**Contains:**
- **Section 1:** Database & schema setup (SQL)
- **Section 2:** Landing layer DDL (bronze.customer_landing, bronze.product_landing, etc.)
- **Section 3:** Staging layer DDL (silver.customer_staging with all columns explained)
- **Section 4:** Gold layer DDL (dimensions, facts, reference tables)
- **Section 5:** PySpark implementation for Landing → Staging
  - Hash generation with deterministic column ordering
  - Delta classification (I/U/D detection)
  - Surrogate key assignment
  - Complete working code with error handling
- **Section 6:** PySpark implementation for Staging → Dimension (SCD2)
  - Insert processing (new customers)
  - Update processing (changed attributes)
  - Delete processing (soft-delete logic)
  - MERGE for idempotency
- **Section 7:** Monitoring & validation queries
  - Row count trends
  - SCD2 integrity checks (no duplicate active records)
  - Lineage tracing (fact back to source)
- **Section 8:** Helper stored procedures

**When to Use:**
- Code reference during development
- Copy/paste starting points
- Testing & validation
- Production deployment
- Troubleshooting

**Key Feature:** Production-ready code with comments explaining architectural decisions.

---

### 🎨 **4. VISUAL REFERENCE** (791 lines)
**File:** `VISUAL_REFERENCE.md`  
**Read Time:** 30-45 minutes (skim-friendly)  
**Audience:** All levels (visual learners)  

**Contains:**
- **Figure 1:** Layered architecture overview (ASCII diagrams)
- **Figure 2:** Data flow with timestamps (showing what happens at each layer)
- **Figure 3:** Staging table grain (append-only audit trail example)
- **Figure 4:** Delta classification logic (decision tree)
- **Figure 5:** Responsibility matrix (who owns what—Staging vs. Gold)
- **Figure 6:** Daily job execution timeline (idempotent job orchestration)
- **Figure 7:** Hash-based delta detection proof of idempotency
- **Figure 8:** Dimension versioning example (3-version customer trace)
- **Figure 9:** Column mapping (Source → Landing → Staging → Gold)
- **Figure 10:** Troubleshooting decision trees (common problems & solutions)
- **Figure 11:** Performance tuning checklist (optimization strategies)

**When to Use:**
- Whiteboard explanations
- Training presentations
- Architecture walkthroughs
- Troubleshooting guides
- Documentation

**Key Benefit:** ASCII diagrams that can be copied into Confluence/wiki without image dependencies.

---

## NAVIGATION BY ROLE

### 👔 **Design Authority / Executive**
1. Read: **QUICK_REFERENCE** → Section "DMBOK INTERPRETATION"
2. Review: **QUICK_REFERENCE** → "GO/NO-GO CHECKLIST FOR DESIGN AUTHORITY"
3. Share: **VISUAL_REFERENCE** → Figure 1 (Layered Architecture)
4. Approve: Decision matrix from QUICK_REFERENCE

**Time Commitment:** 30 minutes

---

### 🏗️ **Data Architect**
1. Read: **MAIN GUIDE** → Full document (Parts 1-10)
2. Reference: **APPENDIX_A** → DDL for table structure
3. Visual: **VISUAL_REFERENCE** → Figure 5 (Responsibility Matrix) & Figure 8 (Versioning)
4. Design: Use anti-patterns section to validate your design

**Time Commitment:** 2-3 hours

---

### 👨‍💻 **Data Engineer (Implementation)**
1. Skim: **QUICK_REFERENCE** → Architecture pattern
2. Deep Dive: **APPENDIX_A** → Complete PySpark code (Landing→Staging & Staging→Gold)
3. Copy: SQL DDL from **APPENDIX_A** for your environment
4. Test: Use monitoring queries from **APPENDIX_A** → Section 7
5. Troubleshoot: **VISUAL_REFERENCE** → Figure 10 (Decision trees)

**Time Commitment:** 3-4 hours (implementation) + 1-2 hours (testing)

---

### 📊 **Data Engineer (Operations/Monitoring)**
1. Reference: **APPENDIX_A** → Monitoring & validation queries
2. Setup: Stored procedures from **APPENDIX_A** → Section 8
3. Troubleshoot: **VISUAL_REFERENCE** → Figure 10
4. Alert: Row count trends and SCD2 integrity checks

**Time Commitment:** 1-2 hours (setup) + 30 min/day (monitoring)

---

### 🎓 **New Team Member (Onboarding)**
1. Start: **QUICK_REFERENCE** → Full document
2. Watch: **VISUAL_REFERENCE** → Figures 1-4 (mental model building)
3. Code: **APPENDIX_A** → Sections 1-4 (understand DDL)
4. Learn: **MAIN_GUIDE** → Part 7 (end-to-end example with real data)
5. Practice: Build staging tables in dev environment

**Time Commitment:** 4-5 hours

---

## FAQ: WHICH DOCUMENT ANSWERS YOUR QUESTION?

| Question | Document | Section |
|----------|----------|---------|
| *"What does DMBOK actually say about staging?"* | QUICK_REF | DMBOK Interpretation |
| *"How do I structure the Staging table?"* | MAIN_GUIDE | Part 1 |
| *"Can you show me the actual code?"* | APPENDIX_A | Sections 2-6 |
| *"Why is effective_start_dt in Gold, not Silver?"* | VISUAL_REF | Figure 5 (Responsibility Matrix) |
| *"How do I detect deltas?"* | MAIN_GUIDE | Part 2 + APPENDIX_A Section 5 |
| *"How do I process updates in the dimension?"* | APPENDIX_A | Section 6 |
| *"What's the daily job schedule?"* | VISUAL_REF | Figure 6 |
| *"My dimension has duplicate active records. How do I fix it?"* | VISUAL_REF | Figure 10 (Troubleshooting) |
| *"Can I re-run a failed job safely?"* | VISUAL_REF | Figure 7 (Idempotency Proof) |
| *"What metrics should I monitor?"* | APPENDIX_A | Section 7 |
| *"What are anti-patterns to avoid?"* | MAIN_GUIDE | Part 10 |
| *"How much time will this take to implement?"* | QUICK_REF | Implementation Checklist |

---

## PHASED IMPLEMENTATION ROADMAP

### **Phase 1: Design & Approval (Week 1)**
**Deliverables:**
- Architecture diagram approved by Design Authority
- Table structure finalized
- SCD2 business rules documented

**Documents to Use:**
- QUICK_REFERENCE (show to Design Authority)
- MAIN_GUIDE Part 1 (detailed design)
- VISUAL_REFERENCE Figure 1 (present to stakeholders)

---

### **Phase 2: Schema & Infrastructure (Week 2)**
**Deliverables:**
- Bronze, Silver, Gold schemas created
- Table DDL deployed
- Surrogate key sequences configured

**Documents to Use:**
- APPENDIX_A Section 1-4 (DDL)
- MAIN_GUIDE Part 1 (column rationale)

---

### **Phase 3: Staging Layer Implementation (Week 3)**
**Deliverables:**
- Landing → Staging job implemented
- Delta detection tested
- Surrogate key uniqueness validated

**Documents to Use:**
- APPENDIX_A Section 5 (complete PySpark code)
- MAIN_GUIDE Part 2 (logic explanation)
- VISUAL_REFERENCE Figure 3-4 (understand delta logic)

---

### **Phase 4: Curated Layer Implementation (Week 4-5)**
**Deliverables:**
- Staging → Dimension job (SCD2 logic)
- Staging → Fact job
- MERGE idempotency tested

**Documents to Use:**
- APPENDIX_A Section 6 (complete PySpark code)
- MAIN_GUIDE Part 3-5 (design rationale)
- VISUAL_REFERENCE Figure 5-8 (understand versioning)

---

### **Phase 5: Validation & Monitoring (Week 6)**
**Deliverables:**
- Data quality checks implemented
- Monitoring dashboards created
- Runbooks documented

**Documents to Use:**
- APPENDIX_A Section 7 (validation queries)
- APPENDIX_A Section 8 (procedures)
- VISUAL_REFERENCE Figure 10 (troubleshooting)

---

### **Phase 6: Production Deployment (Week 7)**
**Deliverables:**
- Daily job schedule configured
- Alerting active
- Team trained

**Documents to Use:**
- MAIN_GUIDE Part 8 (orchestration)
- VISUAL_REFERENCE Figure 6 (job timeline)
- QUICK_REFERENCE (team training)

---

## KEY ARCHITECTURAL DECISIONS

### **Decision 1: Where Does SCD2 Live?**
- ✅ **Gold Layer (Curated)** - Business rules, version closure, effective dates
- ❌ **NOT in Silver Layer (Staging)** - Violates DMBOK "no semantic enrichment"

**Supporting Reference:** MAIN_GUIDE Parts 3-4 + VISUAL_REFERENCE Figure 5

---

### **Decision 2: What Gets Added to Staging?**
- ✅ `insert_dt` - Technical: when appended to Staging (partition key)
- ✅ `source_hash` - Technical: for idempotent delta detection
- ✅ `change_type` (I/U/D) - Technical: delta classification
- ✅ `is_deleted` - Technical: deletion marker
- ❌ `effective_start_dt` - Business rule (not technical)
- ❌ `effective_end_dt` - Business rule (not technical)
- ❌ `is_active` - Business rule (not technical)

**Supporting Reference:** MAIN_GUIDE Part 1 + QUICK_REFERENCE Table Structure Comparison

---

### **Decision 3: How Is Delta Detection Idempotent?**
- **Hash-based:** Same Landing data → same hash → same deltas detected
- **Result:** Re-running job produces same Staging rows (no duplicates)
- **Benefit:** Can safely retry failed jobs without manual cleanup

**Supporting Reference:** VISUAL_REFERENCE Figure 7 + APPENDIX_A Section 5

---

### **Decision 4: How Is Dimension Loading Idempotent?**
- **MERGE pattern:** Detects matches by primary key (customer_dim_key)
- **WHEN MATCHED:** Updates old versions (close them)
- **WHEN NOT MATCHED:** Inserts new versions
- **Result:** Re-running job produces same dimension state

**Supporting Reference:** MAIN_GUIDE Part 3 + APPENDIX_A Section 6

---

## VALIDATION CHECKLIST

Before taking this architecture to Design Authority:

- [ ] **DMBOK Alignment:** Staging adds only technical metadata, not business rules
- [ ] **Table Design:** Every column justified as either "source faithful" or "technical"
- [ ] **Delta Detection:** Hash-based approach enables re-processing
- [ ] **Idempotency:** Both Staging and Gold layers can safely be re-run
- [ ] **Lineage:** Raw data preserved in Staging for audit trail
- [ ] **Business Rules:** SCD2 decisions documented (which attributes trigger versioning?)
- [ ] **Monitoring:** Data quality checks and alerting strategy defined
- [ ] **Performance:** Partitioning and clustering strategy documented
- [ ] **Scalability:** Can the approach handle 10x data growth?

**Use:** QUICK_REFERENCE → "GO/NO-GO CHECKLIST"

---

## COMMON MISTAKES TO AVOID

| Mistake | Why It's Wrong | Fix |
|---------|----------------|-----|
| Adding effective dates to Staging | Violates DMBOK; state-dependent | Move to Gold layer |
| Using INSERT OVERWRITE in Staging | Loses audit trail; breaks lineage | Use APPEND ONLY |
| Joining Staging to Gold Dimension | Circular dependency; hard to debug | Join at Staging level |
| No surrogate keys in Staging | Can't distinguish multiple deltas | Add row_number / auto-increment |
| Not hashing all attributes | Misses updates; false negatives | Hash all tracked columns |

**Use:** MAIN_GUIDE Part 10 + VISUAL_REFERENCE Figure 10

---

## QUICK ANSWERS TO COMMON QUESTIONS

**Q: "If I don't add effective_start_dt to Staging, won't it be cumbersome to process in Gold?"**

A: No. The Gold layer's MERGE logic is designed to efficiently handle this. You read today's Staging deltas, classify them (I/U/D), and MERGE into the dimension. The logic is slightly more complex, but that's where it belongs. See APPENDIX_A Section 6 for complete code.

---

**Q: "How do I know which records to process from Staging each day?"**

A: Filter by `insert_dt = CURRENT_DATE()`. This is a technical metadata column, not a business rule. See MAIN_GUIDE Part 2.

---

**Q: "What if the same customer changes multiple times in one day?"**

A: Staging captures every delta as a new row. Each hash change = new record. The dimension will have multiple versions on the same day (all with effective_start_dt = that day). This is correct behavior. See VISUAL_REFERENCE Figure 8 for example.

---

**Q: "Can I re-run a failed job the next day?"**

A: Yes, but it depends on implementation. If you filter by `insert_dt = CURRENT_DATE()`, re-running tomorrow will pick up no records. Best practice: retry within same day. If must re-run next day, use MERGE to prevent duplicates. See VISUAL_REFERENCE Figure 6.

---

**Q: "How do I handle deletes?"**

A: Mark with `is_deleted = TRUE` in Staging. The Gold layer interprets this and sets `effective_end_dt = yesterday`. This is a soft delete (preserves history). See MAIN_GUIDE Part 7 for example.

---

## RELATED STANDARDS & FRAMEWORKS

**This architecture aligns with:**
- ✅ DAMA-DMBOK2 (Chapters 4, 8, 11)
- ✅ Data Vault 2.0 (Hub/Link/Satellite pattern)
- ✅ Kimball Dimensional Modeling (conformed dimensions)
- ✅ Databricks Medallion Architecture (Bronze/Silver/Gold)
- ✅ Delta Lake best practices

**Complements:**
- Data governance frameworks (Data Stewardship Council approval of business rules)
- Master data management (MDM) for reference dimensions
- Data quality management (DQM) for validation rules

---

## MAINTENANCE & EVOLUTION

**As your data platform grows, consider:**

### **Near-term (Months 1-3)**
- Implement core customer, product, order dimensions
- Monitor Staging growth rates
- Establish SLA for daily job completion

### **Medium-term (Months 4-6)**
- Add more dimensions (supplier, employee, location)
- Implement more complex facts (with slowly-changing references)
- Create materialized views for common queries

### **Long-term (Months 6-12)**
- Consider Data Vault 2.0 fully (Hub/Link/Satellite for multiple sources)
- Implement streaming into Staging (Kafka → Delta)
- Add real-time dimension serving (API against Gold)

---

## VERSION HISTORY

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | May 4, 2026 | Initial comprehensive guidance |

---

## DOCUMENT MAINTENANCE

**Owner:** Lead Data Architect  
**Last Updated:** May 4, 2026  
**Review Schedule:** Quarterly (or after major architecture changes)  
**Repository:** [Confluence/Wiki link]  

---

## SUPPORT & QUESTIONS

**For questions about:**
- **DMBOK alignment:** See QUICK_REFERENCE + MAIN_GUIDE Part 10
- **Implementation details:** See APPENDIX_A with code
- **Troubleshooting:** See VISUAL_REFERENCE Figure 10
- **Architecture decisions:** See MAIN_GUIDE Parts 1-4

**For team training:**
- Use QUICK_REFERENCE for 30-min overview
- Use VISUAL_REFERENCE for 1-hour workshop
- Use APPENDIX_A for hands-on coding lab

---

## FINAL SUMMARY

### The Challenge ✓ Resolved
Your Staging layer now adds **only technical metadata** (insert_dt, source_hash, change_type, is_deleted), enabling efficient delta detection and audit trail preservation, while keeping **all business logic** (effective dates, versioning rules) in the Gold layer where it belongs.

### Design Authority ✓ Approval
This architecture is DMBOK-compliant: Staging = technical contract with source system; Gold = business contract with analytics consumers.

### Implementation ✓ Ready
Complete, production-ready code provided (SQL DDL + PySpark) for all three layers.

### Go Build! 🚀
You have all the guidance, code, examples, and visual references needed to implement this architecture successfully.

---

**Questions?** Review the navigation by role above, or use the FAQ cross-reference table.

**Ready to present to Design Authority?** Start with QUICK_REFERENCE and VISUAL_REFERENCE Figure 1.

**Ready to code?** Start with APPENDIX_A Section 1 (DDL) and Section 5 (PySpark).
