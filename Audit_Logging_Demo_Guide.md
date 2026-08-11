# Audit Logging Demo Guide

This guide describes the two audit logs used by the pipeline.  It is written so that the sections headed **Confluence Page** can be copied into two separate Confluence pages.

## The simple picture

| Log | Table | Main question it answers | Written by |
|---|---|---|---|
| Table-processing audit | `audit_table_run_stats` | What happened while a particular table was read, transformed, and written? | The calling pipeline notebook, once per completed table attempt |
| Job and task snapshot | `audit_job_run_stats` | What happened to the Databricks job and task that ran the pipeline? | The separate `audit_job_run_stats_logger` notebook, after the task has completed |

The two tables are complementary rather than duplicates.

* One **task run** can process several tables. Therefore, it can have many rows in `audit_table_run_stats` and one matching row in `audit_job_run_stats`.
* The common link is `workspace_id` plus `task_run_id`.
* `audit_job_run_stats` does **not** depend on `audit_table_run_stats`. It reads Databricks Lakeflow system tables, so it can still show a failed job or task even when table processing stopped before a table-audit row could be written.

---

# Confluence Page 1 — Table-processing audit: `audit_table_run_stats`

## 1. Purpose

`audit_table_run_stats` is the table-level audit log. It records the final outcome of one attempt to process one target table.

For example, when the `landing_to_staging` task processes `student`, it can record:

* which source was used;
* which target table was written;
* how many rows were read, inserted, updated, deleted, stubbed, inferred, or quarantined;
* whether the table was successful, skipped, or failed;
* how long that table took; and
* any handled error or business reason.

The reusable file is named `audit_batch_run_logger.py`, but its current purpose is table-level logging. Its target table is `audit_table_run_stats`.

## 2. Plain-English flow

1. The calling notebook starts processing a table and stores the UTC start time.
2. The notebook reads, transforms, validates, and writes the table as normal.
3. While it does that work, the notebook captures the meaningful results: `rows_read`, `rows_inserted`, `rows_updated`, `rows_deleted`, `rows_stubbed`, `rows_inferred`, `rows_quarantined`, and duplicate count where relevant.
4. The notebook decides the final outcome: `success`, `skipped`, or `failed`.
5. The notebook calls `log_table_audit_record(...)` **once**, after all the figures and final status are known.
6. The logger detects the environment, creates `audit_table_run_stats` if it does not yet exist, adds its own audit timestamp and row ID, calculates duration, and appends one Delta row.
7. The written row can later be linked to the matching Databricks task snapshot using `workspace_id` and `task_run_id`.

## 3. Responsibility split

This is the most important design point for calling notebooks.

| The calling notebook must do | The reusable logger does |
|---|---|
| Process the source and target table | Create the audit schema/table when missing |
| Capture the actual figures produced by the load or transformation | Detect `env` using `detect_env()` |
| Set the final status, reason, and any handled error text | Default `source_object_type` to `file` when a path/pattern is supplied; otherwise `table` |
| Supply `workspace_id`, `task_run_id`, target table, and table start time | Capture the table end time when the caller does not supply it |
| Call the logger once per completed table attempt | Calculate `duration_ms` and readable `duration_hms` |
| Re-raise a caught error after logging when the task should still fail | Generate `created_utc_ts`, Sydney display time, and sortable `row_id` |
|  | Append one record to `audit_table_run_stats` and return that one-row DataFrame |

The logger does **not** calculate row counts for the calling notebook. It only writes the values passed to it. This keeps the audit row aligned with the actual load logic instead of guessing what each transformation means.

## 4. Inputs that matter most

### Required context

The logger requires these values:

| Input | Why it is needed |
|---|---|
| `spark` | Used to write the Delta audit record. |
| `workspace_id` | Identifies the Databricks workspace. |
| `task_run_id` | Links the table row to the job/task snapshot. |
| `target_table` | The table being processed. |
| `run_status` | `success`, `skipped`, or `failed`. |
| `table_start_utc_ts` | Starting point for the duration. |

The logger raises a clear error if any required context is blank. This is intentional: a table audit record without its target, status, task ID, or start time would not be trustworthy.

### Metrics supplied by the calling notebook

Use zero when a metric does not apply. The normal metrics are:

| Metric | Meaning |
|---|---|
| `rows_read` | Source rows considered by the processing logic. |
| `rows_inserted` | New target rows added. |
| `rows_updated` | Existing target rows changed. |
| `rows_deleted` | Target rows removed or marked deleted. |
| `rows_stubbed` | Placeholder reference/dimension rows created because a required record was not yet available. |
| `rows_inferred` | Rows or values derived by approved processing rules. |
| `rows_quarantined` | Rows kept out of the target and sent for review. |
| `duplicate_records` | Duplicate source rows found under the relevant business or primary key rule. |

Useful values for `action` include `merged`, `insert_overwrite`, and `create_or_replace`. Useful reason codes include `no_recs`, `processing_error`, and `null_recs_found`.

## 5. What the logger adds automatically

The caller does not need to calculate these values.

| Field | How it is populated |
|---|---|
| `env` | Detected by `detect_env()`. |
| `source_object_type` | Uses the supplied value; otherwise becomes `file` when a file path or pattern is present, or `table` otherwise. |
| `table_end_utc_ts` | Captured at logging time when not supplied. |
| `duration_ms` and `duration_hms` | Calculated from the table start and end timestamps. `duration_hms` is shown as days:hours:minutes:seconds:milliseconds, for example `0:00:01:30:123`. |
| `created_utc_ts` | UTC time at which the audit row is written. |
| `created_ast` | Readable Australia/Sydney time at which the audit row is written. It automatically reflects AEST or AEDT. |
| `row_id` | A time-based, sortable identifier generated immediately before the append. |

`table_start_utc_ts`, `table_end_utc_ts`, and `created_utc_ts` are all UTC timestamps. `created_ast` is provided only to make local-time reading easier.

## 6. Copyable example — normal table load

This is a simple pattern for a successful table transformation. The metric assignments are deliberately visible: replace the example figures with the values produced by the real load logic.

```python
from udf_shared.audit_batch_run_logger import log_table_audit_record
from udf_shared.generic_utilities import spark_now

# These values should already be available to the task, for example as job parameters.
workspace_id = dbutils.widgets.get("workspace_id")
task_run_id = dbutils.widgets.get("task_run_id")

target_table = "cat_dev_r2_darestaging_01.esd.student"
table_start_utc_ts = spark_now(spark)

# 1. Process the table.
source_df = spark.table("cat_dev_r2_darelanding_01.erbi_mft_dev_student.student")
rows_read = source_df.count()

# Run the actual validation, transformation, and merge here.
# Capture the figures from that processing; the logger does not calculate them.
rows_inserted = 250
rows_updated = 18
rows_deleted = 0
rows_stubbed = 0
rows_inferred = 0
rows_quarantined = 3
duplicate_records = 2

# 2. Call the logger once, after the table outcome is known.
log_table_audit_record(
    spark,
    workspace_id=workspace_id,
    task_run_id=task_run_id,
    stage_name="landing_to_staging",
    source_system="ERBI",
    source_object_type="table",
    source_object_name="cat_dev_r2_darelanding_01.erbi_mft_dev_student.student",
    target_table=target_table,
    action="merged",
    run_status="success",
    rows_read=rows_read,
    rows_inserted=rows_inserted,
    rows_updated=rows_updated,
    rows_deleted=rows_deleted,
    rows_stubbed=rows_stubbed,
    rows_inferred=rows_inferred,
    rows_quarantined=rows_quarantined,
    duplicate_records=duplicate_records,
    table_start_utc_ts=table_start_utc_ts,
)
```

For a file-based load, pass `file_source_path` and `source_search_pattern`. The logger will identify the source as a file when `source_object_type` is omitted.

```python
log_table_audit_record(
    spark,
    workspace_id=workspace_id,
    task_run_id=task_run_id,
    stage_name="raw_to_landing",
    source_system="MFT",
    file_source_path="abfss://erbi@storage.dfs.core.windows.net/mft/student",
    source_search_pattern="student_*.csv",
    source_object_name="student_20260810.csv",
    target_table="cat_dev_r2_darelanding_01.erbi_mft_dev_student.student",
    action="merged",
    run_status="success",
    rows_read=1250,
    rows_inserted=1247,
    rows_quarantined=3,
    table_start_utc_ts=table_start_utc_ts,
)
```

## 7. Copyable example — no records to process

Use a skipped record when the pipeline ran correctly but there was nothing to load. This is different from a failure.

```python
from udf_shared.audit_batch_run_logger import log_table_audit_record
from udf_shared.generic_utilities import spark_now

table_start_utc_ts = spark_now(spark)
source_df = spark.table("cat_dev_r2_darelanding_01.erbi_mft_dev_student.student")
rows_read = source_df.count()

# Start every optional metric at zero. The normal-processing branch below
# replaces them with the actual values produced by the load.
rows_inserted = 0
rows_updated = 0
rows_deleted = 0

if rows_read == 0:
    run_status = "skipped"
    reason = "no_recs"
    action = "merged"
else:
    # Run the normal table processing here and set the real counts.
    run_status = "success"
    reason = None
    action = "merged"
    rows_inserted = 250  # Replace with the value from the real load.
    rows_updated = 18    # Replace with the value from the real load.

# One logging call after either branch.
log_table_audit_record(
    spark,
    workspace_id=workspace_id,
    task_run_id=task_run_id,
    stage_name="landing_to_staging",
    source_system="ERBI",
    source_object_name="cat_dev_r2_darelanding_01.erbi_mft_dev_student.student",
    target_table="cat_dev_r2_darestaging_01.esd.student",
    action=action,
    run_status=run_status,
    rows_read=rows_read,
    rows_inserted=rows_inserted,
    rows_updated=rows_updated,
    rows_deleted=rows_deleted,
    table_start_utc_ts=table_start_utc_ts,
    reason=reason,
)
```

## 8. Copyable example — handled error, logged once, then re-raised

This pattern is useful when the team wants both an audit record and a failed Databricks task. The calling notebook catches the error long enough to record it, calls the logger once, then raises the original error again.

```python
from udf_shared.audit_batch_run_logger import log_table_audit_record
from udf_shared.generic_utilities import spark_now

table_start_utc_ts = spark_now(spark)

# Set safe defaults before processing starts.
rows_read = 0
rows_inserted = 0
rows_updated = 0
rows_deleted = 0
rows_stubbed = 0
rows_inferred = 0
rows_quarantined = 0
duplicate_records = 0
run_status = "success"
reason = None
error_message = None
captured_error = None

try:
    source_df = spark.table("cat_dev_r2_darelanding_01.erbi_mft_dev_student.student")
    rows_read = source_df.count()

    # Run the normal transformation and write here.
    # Set the real counts returned by the process.
    rows_inserted = 250
    rows_updated = 18

except Exception as error:
    captured_error = error
    run_status = "failed"
    reason = "processing_error"
    error_message = f"{type(error).__name__}: {error}"

# This is the only audit call for this table attempt.
log_table_audit_record(
    spark,
    workspace_id=workspace_id,
    task_run_id=task_run_id,
    stage_name="landing_to_staging",
    source_system="ERBI",
    source_object_type="table",
    source_object_name="cat_dev_r2_darelanding_01.erbi_mft_dev_student.student",
    target_table="cat_dev_r2_darestaging_01.esd.student",
    action="merged",
    run_status=run_status,
    rows_read=rows_read,
    rows_inserted=rows_inserted,
    rows_updated=rows_updated,
    rows_deleted=rows_deleted,
    rows_stubbed=rows_stubbed,
    rows_inferred=rows_inferred,
    rows_quarantined=rows_quarantined,
    duplicate_records=duplicate_records,
    table_start_utc_ts=table_start_utc_ts,
    reason=reason,
    error_message=error_message,
)

# Preserve the failed task result after the audit row has been saved.
if captured_error is not None:
    raise captured_error
```

Important: errors are captured in `audit_table_run_stats` only when the calling script handles the error and reaches the logging call. If a notebook stops before that point, its table-level record may be absent; the separate job snapshot can still show the failed task.

## 9. Reading the table audit record

| Field group | Fields to show in a demo | What they tell the audience |
|---|---|---|
| Identity and link | `row_id`, `workspace_id`, `task_run_id`, `env` | Which workspace/task created this table record. |
| What was processed | `stage_name`, `source_system`, `source_object_name`, `target_table`, `action` | The movement of data from source to target. |
| Result | `run_status`, `reason`, `error_message` | Whether the table succeeded, was intentionally skipped, or failed. |
| Volumes | `rows_read`, `rows_inserted`, `rows_updated`, `rows_deleted` | Main movement of records. |
| Data-quality/curation volumes | `rows_stubbed`, `rows_inferred`, `rows_quarantined`, `duplicate_records` | Special handling applied during processing. |
| Timing | `table_start_utc_ts`, `table_end_utc_ts`, `duration_ms`, `duration_hms` | How long the table took. |
| Audit-write time | `created_utc_ts`, `created_ast` | When the audit record itself was written. |

## 10. Suggested table-log demo narrative

> “This row is the business record for one table load. The pipeline captured the actual counts while it processed the table, then made one logger call at the end. The logger adds the environment, timestamps, duration, and a link to the Databricks task. This means we can see both the data outcome and, through the task ID, the technical execution outcome.”

---

# Confluence Page 2 — Job and task execution snapshot: `audit_job_run_stats`

## 1. Purpose

`audit_job_run_stats` is the operational companion to the table audit. It creates an append-only snapshot of completed Databricks Lakeflow task executions.

It records job and task names, IDs, start/end times, duration, trigger type, final state, termination details, nested-job information, and available error details. It includes successful, failed, cancelled, skipped, timed-out, blocked, and repaired executions once they are complete.

It is produced by the `audit_job_run_stats_logger` notebook. This is a separate job from the data-processing jobs it observes.

## 2. Plain-English flow

1. The snapshot job reads its widgets: workspace ID, overlap window, and excluded job names.
2. It sets the Spark session to UTC and creates `audit_job_run_stats` if the table is missing.
3. It reads the last successful snapshot time from a property stored on the target table.
4. It builds a safe refresh window. On later runs, it goes back to the last successful time minus the overlap window.
5. It reads only completed job events from `system.lakeflow.job_run_timeline`. Running jobs are not included.
6. It reads the matching task timeline from `system.lakeflow.job_task_run_timeline`, combines timeline slices, and keeps only new terminal task runs.
7. It reads the matching job timeline again to calculate job-level start/end times, duration, outcome, trigger, and nested-job details.
8. It removes excluded job names, including the audit logger itself by default.
9. For recent unsuccessful rows, it makes one bounded Jobs API attempt to collect useful error detail. An API problem never prevents the core audit record from being written.
10. It appends all prepared rows once and advances the last-successful-refresh property only after that append succeeds.
11. It prints a concise summary and displays the appended records.

## 3. Inputs and default settings

| Widget | Default in the supplied notebook | Purpose |
|---|---|---|
| `workspace_id` | `1149936693371449` | Workspace being audited. In a scheduled job, set this to `{{workspace.id}}`. |
| `lookback_hours` | `24` | Hours of overlap used after a successful snapshot. It protects against late-arriving Lakeflow system-table rows. |
| `excluded_job_names` | `job_generic_system_audit_table_logs`, `dev_job_generic_system_audit_table_logs` | Comma-separated job names to keep out of this audit result. Matching is case-insensitive. |

The default exclusions prevent the audit job from writing audit rows about itself. Add any other job names that should not be reported through the widget rather than hard-coding them into the SQL.

## 4. Refresh window and duplicate protection

### First snapshot

There is no checkpoint on a brand-new target table. The logger reads all completed Lakeflow history that is currently available in the system tables. The availability period depends on Databricks system-table retention.

### Later snapshots

The target table stores the property:

```text
audit_snapshot.last_successful_refresh_utc_ts
```

On each later run, the logger starts from:

```text
last successful refresh time − lookback_hours
```

For example, with a 24-hour overlap, a snapshot that last completed at 10:00 on Monday starts reading from 10:00 on Sunday. Some already-seen history is intentionally read again.

Before new rows are prepared, the logger creates a small list of existing `(workspace_id, task_run_id)` keys from `audit_job_run_stats`. It removes those keys from the incoming data. The overlap therefore protects completeness without creating duplicate task rows.

The checkpoint is updated only after the append succeeds. When no completed job events, no new terminal tasks, or no prepared rows are found, the notebook exits without writing rows and without moving the checkpoint.

## 5. How completed jobs and tasks are selected

### Completed job events

The logger reads `system.lakeflow.job_run_timeline` and selects a job event only when:

* it belongs to the requested workspace;
* `result_state` is populated, which indicates a completed job event;
* its end time is before the snapshot start time; and
* it falls inside the first-run history or later refresh window.

This includes failed jobs as well as successful jobs. It deliberately excludes jobs that are still running.

### Completed task runs

The logger joins `system.lakeflow.job_task_run_timeline` to those completed job events. For each terminal task run, it:

* combines the timeline slices for a long-running task;
* keeps the earliest task start and latest task end;
* adds the recorded active duration;
* takes the latest available task result and termination information; and
* removes a task run already stored in the audit table.

The task snapshot is append-only. A retry or repair has a new `task_run_id`, so it is captured as another row instead of replacing the original task execution.

### Job-level information

For only the required job completions, the logger re-reads the job timeline and combines its timeline slices. It captures the original job start time, completion time, cumulative active duration, trigger type, result state, termination details, and nested-job IDs. It also takes the latest available job name from `system.lakeflow.jobs`.

Because the name comes from the latest job definition available at snapshot time, a historical row can show a job name that was changed after the execution took place. The IDs remain the reliable identifiers.

## 6. Repairs and long-running jobs

| Situation | How the snapshot treats it |
|---|---|
| A job spans several system-table timeline slices | It aggregates slices so start time, end time, duration, and slice count remain meaningful. |
| A job is repaired | Databricks keeps the original `job_run_id`, but the repaired execution has a new `task_run_id`. The repaired task is appended as a new audit row. The original task row is preserved. |
| A job launches another job | `source_task_run_id` identifies the task that launched it and `root_task_run_id` identifies the first task in the launch chain. |
| A normal task belongs to its containing job | `parent_run_id` normally equals `job_run_id`; in that case `parent_job_id` and `parent_job_name` are populated. |

`job_timeline_duration_s` is recorded active time from the timeline slices. It does not include idle time between an original execution and a later repair.

## 7. Error-detail enrichment

The system tables remain the source of truth for the core completion record. The Jobs API is used only to add extra diagnostics for unsuccessful rows.

| Case | What the logger does | `error_capture_status` |
|---|---|---|
| Job and task both succeeded | Makes no API call. | `NOT_APPLICABLE` |
| Unsuccessful task completed within about 60 days | Makes one bounded API attempt for error details. Job metadata is cached once per job run. | `CAPTURED`, `NO_DETAIL_AVAILABLE`, or `API_ERROR` |
| Unsuccessful task older than about 60 days | Does not make an API request because detailed run output is normally no longer retained. | `EXPIRED` |
| API request cannot be completed | Keeps the core audit row and records the retrieval issue. | `API_ERROR` |

The API request is configured to avoid a long retry loop: it uses a one-second retry window and a 30-second HTTP timeout. There is no maximum number of unsuccessful rows; each eligible row receives its one attempt. A permission issue, expired run output, or API outage does not stop the append of the main job/task audit record.

The optional diagnostic fields are `job_state_message`, `task_state_message`, `task_error_message`, `task_error_trace`, `job_run_page_url`, `task_run_page_url`, `error_captured_utc_ts`, `error_capture_status`, and `error_capture_api_message`.

## 8. What is written to `audit_job_run_stats`

| Field group | Key fields | What they tell the audience |
|---|---|---|
| Snapshot identity | `row_id`, `created_utc_ts`, `created_ast`, `workspace_id` | When this snapshot row was inserted and which workspace it represents. |
| Job/task identity | `job_id`, `job_name`, `task_name`, `task_run_id`, `job_run_id`, `parent_run_id` | Which reusable job definition and which specific execution ran. |
| Parent/nested context | `parent_job_id`, `parent_job_name`, `source_task_run_id`, `root_task_run_id` | How a task relates to its containing job or a job-launch chain. |
| Job timing | `job_start_utc_ts`, `job_end_utc_ts`, `job_timeline_duration_s`, `job_timeline_duration_hms`, `job_timeline_slice_count`, `job_execution_count` | When the job started/ended and how much active time it used. |
| Task timing | `task_start_utc_ts`, `task_end_utc_ts`, `task_timeline_duration_s`, `task_timeline_duration_hms`, `task_timeline_slice_count`, `task_terminal_event_count` | When this specific task execution started/ended and how it was represented in timeline slices. |
| Outcome | `task_result_state`, `task_termination_code`, `task_termination_type`, `job_result_state`, `job_termination_code`, `job_termination_type` | Whether the job/task finished successfully and the platform reason if it did not. |
| Trigger | `job_trigger_type`, `job_run_type` | What started the job and the Databricks run classification. |
| Diagnostics | Error fields listed in section 7 | Extra platform error information when it was available. |

All start/end and snapshot timestamps are UTC. The corresponding `*_duration_hms` fields are formatted as `DD:HH:MM:SS`, for example `00:01:02:05` for zero days, one hour, two minutes, and five seconds.

## 9. Write behaviour and safeguards

The job snapshot is deliberately simple and safe to re-run:

* It creates the target table only when it is absent.
* It does one append after every selected row has been fully prepared.
* It does not use `MERGE`, `UPDATE`, automatic schema evolution, or repair logic for existing rows.
* It does not overwrite an original task row with a repair; each repaired task execution remains a separate historical record.
* It moves the checkpoint only after the append succeeds.

If a future column is needed, add it with a separate `ALTER TABLE` change and add the matching value to the prepared DataFrame before the append.

## 10. Suggested job-snapshot demo narrative

> “The table audit tells us what the pipeline did to data. This snapshot tells us what Databricks did with the job and task that ran it. The snapshot only takes completed tasks, includes failures, preserves repairs as separate executions, and uses a small overlap window so late system-table records are not missed. It then links back to the table audit through the workspace ID and task run ID.”

---

# Demonstrating both logs together

For a single task that processed one or more tables, use the table audit as the starting point and join to the job snapshot. This shows the data result and the Databricks execution result on one screen.

```sql
SELECT
    table_audit.target_table,
    table_audit.run_status AS table_run_status,
    table_audit.rows_read,
    table_audit.rows_inserted,
    table_audit.rows_updated,
    table_audit.rows_deleted,
    table_audit.rows_quarantined,
    table_audit.duration_hms AS table_duration,
    job_audit.job_name,
    job_audit.task_name,
    job_audit.job_result_state,
    job_audit.task_result_state,
    job_audit.task_termination_code,
    job_audit.task_error_message,
    job_audit.task_run_page_url
FROM cat_dev_r2_darecurated_01.db_audit_tracking.audit_table_run_stats table_audit
LEFT JOIN cat_dev_r2_darecurated_01.db_audit_tracking.audit_job_run_stats job_audit
  ON table_audit.workspace_id = job_audit.workspace_id
 AND table_audit.task_run_id = job_audit.task_run_id
WHERE table_audit.task_run_id = '<task_run_id_to_demo>'
ORDER BY table_audit.table_start_utc_ts;
```

Replace `dev` with the required environment and replace `<task_run_id_to_demo>` with the task run being demonstrated.

## Recommended live-demo sequence

1. Open one `audit_table_run_stats` row and identify the source, target, outcome, counts, and duration.
2. Point out that those figures came from the calling transformation, followed by one logger call.
3. Show the matching `audit_job_run_stats` row using `workspace_id` and `task_run_id`.
4. Explain the job/task state, trigger, start/end time, and any error diagnostic.
5. If useful, show a skipped table row and a failed/repaired task row to demonstrate that the logs retain the history rather than hiding it.
