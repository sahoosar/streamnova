# Dataflow Job Metadata: Status After Cloud Run Expires

When the **execute** flow uses `result.waitUntilFinish()`, the HTTP request blocks until the Dataflow job completes. If **Cloud Run** (or any gateway) times out (e.g. 1 hour), the client gets a 504 and never sees the outcome—but the **Dataflow job keeps running** on GCP. This design adds a **metadata table** so status can be updated and queried even after the request expires.

---

## Goal

- **Record** every submitted Dataflow job (run_id + dataflow_job_id) as soon as `pipeline.run()` returns.
- **Update** status to DONE/FAILED when:
  - the same request completes (`waitUntilFinish()` returns), or
  - a **background poller** calls the Dataflow API and sees the job finished (so we get correct status even if the Cloud Run request already timed out).

Clients can then **GET status by run_id or dataflow_job_id** from this table (or an API that reads it) instead of relying on the execute response.

---

## How tracking works

We track each run with a single id, **run_id**. The same id is used when submitting the job, when writing the metadata row, and when the client checks status later.

### Who creates run_id?

When the client calls **POST /api/agent/execute**:

- **If the client sends `executionRunId`** in the request body (e.g. `"exec-abc-123"`), the server uses that as **run_id**.
- **If the client does not send it**, the server generates one (e.g. `"exec-" + UUID`).

So run_id is either **client-supplied** or **server-generated**. The controller must pass this run_id into the runner so the same value is stored in the metadata table.

### Where is it stored?

As soon as the job is submitted, we **INSERT** a row into **`dataflow_job_metadata`** with that run_id, the GCP dataflow_job_id, and status (e.g. RUNNING). That row is the tracking record. Later, when the job finishes (in the same request or via the poller), we **UPDATE** that row to DONE/FAILED.

### How does the client get run_id for "later check"?

- **Option A — Client sends it (recommended when the request may time out)**  
  The client generates an id (e.g. `"exec-" + UUID`) and sends it as `executionRunId` in the execute request. The server uses it as run_id and stores it in the metadata table. **Even if the request times out (504), the client already has run_id** — they created it. They can then call **GET .../dataflow-job-status?runId=exec-abc-123** and the server looks up the row and returns status.

- **Option B — Server returns it (only when the request completes)**  
  If the client does not send `executionRunId`, the server generates run_id and returns it in the execute response body when the call finishes. The client can then poll GET status with that runId. **If the request times out**, the client never gets the response, so they do not have run_id unless we also return the response before blocking (async submit).

**Recommendation:** For reliable tracking after timeouts, have the client **send `executionRunId`** and use that value for status polling. Also return runId in the execute response so clients that don’t send it still get an id when the request completes.

### End-to-end sequence

```
CLIENT                              SERVER                         METADATA TABLE
  |                                    |                                    |
  |  POST execute                       |                                    |
  |  { "executionRunId": "exec-1",      |  run_id = "exec-1" (from body)      |
  |    "candidate": {...} }             |  pipeline.run() -> dataflow job id  |
  | ---------------------------------> |  INSERT run_id=exec-1, status=RUNNING
  |                                    | ----------------------------------> |
  |                                    |  waitUntilFinish() ...               |
  |      (e.g. 1h later: 504)          |                                    |
  | <--------------------------------- (timeout, no body)                    |
  |  Client still has "exec-1"          |                                    |
  |                                    |                                    |
  |  GET status?runId=exec-1            |  SELECT ... WHERE run_id='exec-1'    |
  | ---------------------------------> | ----------------------------------> |
  |                                    | <---------------------------------- |
  |  { "runId": "exec-1",               |  (row: status DONE, finished_at)    |
  |    "status": "DONE" }               |                                    |
  | <--------------------------------- |                                    |
```

---

## Schema

Table: **`dataflow_job_metadata`**

| Column              | Purpose |
|---------------------|--------|
| `run_id`            | Primary key; same as controller runId (from request `executionRunId` or server-generated e.g. `exec-<uuid>`). Must be passed from controller to runner so the client can poll by this id. |
| `dataflow_job_id`   | GCP Dataflow job id from `PipelineResult` / `DataflowPipelineJob.getJobId()`. |
| `status`            | `SUBMITTED` \| `RUNNING` \| `DONE` \| `FAILED` \| `UNKNOWN` \| `REQUEST_TIMEOUT`. |
| `submitted_at`      | When we submitted the job. |
| `updated_at`        | Last time we updated this row (our app or poller). |
| `finished_at`       | When the job reached a terminal state (DONE/FAILED); null until then. |
| `message`           | Optional error or status detail (e.g. from Dataflow API). |
| `execution_run_id`  | Optional link to agent execution (e.g. same as run_id). |
| `source_type`, `schema_name`, `table_name` | Optional pipeline context. |
| `machine_type`, `worker_count`, `shard_count` | Optional candidate context. |

Full DDL: [DATAFLOW_JOB_METADATA_SCHEMA.sql](../src/main/resources/agent/metrics/DATAFLOW_JOB_METADATA_SCHEMA.sql).

---

## Flow

### 1. On submit (in DataflowRunnerService, right after `pipeline.run()`)

- Controller passes **run_id** (from request `executionRunId` or generated `"exec-" + UUID`) into the runner. Runner must accept and use this value.
- Get `DataflowPipelineJob` / job id from `pipeline.run()`.
- **INSERT** into `dataflow_job_metadata`:
  - `run_id` = the controller’s run_id (so the client can poll with the same id they sent or will receive in the response).
  - `dataflow_job_id` = value from `dataflowJob.getJobId()`.
  - `status` = `SUBMITTED` or `RUNNING`.
  - `submitted_at` = now; optional context columns as needed.
- Then either:
  - **Option A (async/self-service):** Return the job id to the client and **do not** call `waitUntilFinish()`. Only the poller (or a separate completion callback) updates status.
  - **Option B (keep blocking):** Call `waitUntilFinish()`. When it returns, **UPDATE** the row to `DONE` or `FAILED` with `finished_at` and `message`. If the request times out before that, the row stays `SUBMITTED`/`RUNNING` until the poller updates it.

### 2. Poller (runs outside the request, e.g. scheduled job or Cloud Scheduler)

- **Select** rows where `status IN ('SUBMITTED', 'RUNNING')` (and optionally `updated_at` not too old).
- For each row, call **Dataflow API** `projects.locations.jobs.get` (or equivalent) with `dataflow_job_id` (and project/region).
- Map Dataflow state to our status:
  - `JOB_STATE_DONE` → `DONE`
  - `JOB_STATE_FAILED`, `JOB_STATE_CANCELLED` → `FAILED`
  - `JOB_STATE_RUNNING`, etc. → `RUNNING` (and refresh `updated_at`)
  - Unknown / missing job → `UNKNOWN` or leave as-is and set `message`.
- **UPDATE** the row: `status`, `finished_at` (if terminal), `message`, `updated_at`.

So even if the Cloud Run session expired, the poller will eventually set `DONE` or `FAILED` and clients can query the table (or a status API that reads it).

### 3. Optional: mark request timeout

If you know the request was terminated by a timeout (e.g. Cloud Run 504), you could set `status = 'REQUEST_TIMEOUT'` in a separate path (e.g. when the client reports 504 or a server-side timeout handler runs). The poller can still later overwrite this with `DONE`/`FAILED` when the job actually finishes. So `REQUEST_TIMEOUT` is only a hint that “the HTTP request ended before we got the result.”

---

## Integration points

| Where | Change |
|-------|--------|
| **ExecutionController / ExecutionEngineService** | Pass controller runId (from request executionRunId or generated) into the runner. Include runId in ExecutionResult so the response body returns it to the client. |
| **DataflowRunnerService** | Accept runId from caller. After `pipeline.run()`, if result is `DataflowPipelineJob`: insert into `dataflow_job_metadata` with run_id = that runId, dataflow_job_id, status SUBMITTED/RUNNING, submitted_at. Before returning (or when `waitUntilFinish()` returns), update the same row to DONE/FAILED with finished_at and message. |
| **Persistence** | New repository or service that writes/updates `dataflow_job_metadata` (JDBC or existing store). |
| **Status API** | New endpoint, e.g. `GET /api/agent/dataflow-job-status?runId=...` or `?dataflowJobId=...`, that reads from `dataflow_job_metadata` and returns status, finished_at, message. |
| **Poller** | New scheduled job (e.g. Spring `@Scheduled`, or Cloud Scheduler + HTTP trigger) that selects non-terminal rows, calls Dataflow API, and updates the table. |

---

## Summary

- **Tracking** uses a single **run_id** (client-supplied `executionRunId` or server-generated). The same id is stored in the metadata table and used for GET status; for reliable tracking after timeouts, the client should send `executionRunId` and use it when polling.
- **Metadata table** stores run_id, dataflow_job_id, status, timestamps, and optional context.
- **Written** as soon as the job is submitted; **updated** when the same request sees completion or when a **poller** sees completion via the Dataflow API.
- So **Dataflow job completion is reflected in the table even when the Cloud Run session has expired.**

Schema file: `src/main/resources/agent/metrics/DATAFLOW_JOB_METADATA_SCHEMA.sql`.
