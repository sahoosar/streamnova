# StreamNova — React → Cloud Run → Spring Boot  
# Communication Architecture Brief

*For use by the React frontend team*  
*February 2026*

---

## 1. Architecture Overview

StreamNova is deployed on Google Cloud Run as a Spring Boot service backed by Apache Beam (Dataflow runner). React serves as the frontend. The communication model is **async-first**: the user triggers a load, the agent plans and submits the Dataflow job, and the React UI tracks progress via SSE or polling.

| Layer    | Technology           | Host           | Role                              |
|----------|----------------------|----------------|-----------------------------------|
| Frontend | React 18             | CDN / Firebase Hosting | User interaction, job monitoring |
| API      | Spring Boot 3        | Cloud Run      | Agent orchestration, pipeline trigger |
| Execution| Apache Beam / Dataflow | Google Dataflow | Actual data movement (DB → GCS → BQ) |
| Storage  | GCS + BigQuery       | GCP            | Staging files + final destination |

**Cloud Run constraint:** HTTP requests have a max timeout of 3,600s. All pipeline executions must be **async** — Spring Boot submits to Dataflow and returns a `jobId` immediately. The React UI then tracks progress separately.

---

## 2. Communication Protocol

### 2.1 Protocol Selection

| Protocol      | Used For        | Cloud Run OK?              | React Usage        |
|---------------|-----------------|----------------------------|---------------------|
| HTTPS REST    | All API calls   | Yes — native               | axios / fetch       |
| SSE (EventSource) | Job progress stream | Yes — use min-instances=1 | EventSource API     |
| Polling (fallback) | Job status check | Yes — fully stateless   | setInterval + axios |
| WebSocket     | (not used)      | Needs --session-affinity   | Avoid on Cloud Run  |

### 2.2 Base URL and Headers

All API calls from React target the Cloud Run service URL:

```javascript
// constants/api.js
export const BASE_URL = process.env.REACT_APP_API_URL;
// e.g. https://streamnova-xxxx-uc.a.run.app/streamnova

// Attach to every request
const headers = {
  'Content-Type': 'application/json',
  'Authorization': `Bearer ${await getIdToken()}`,  // Firebase Auth token
  'X-Request-ID': crypto.randomUUID()               // for tracing
};
```

---

## 3. End-to-End Flow

The complete user journey has 5 phases. Each phase maps to one or more API calls.

| # | Phase            | User Action        | What Happens |
|---|------------------|--------------------|--------------|
| 1 | Discover         | App loads          | React fetches available handlers (postgres/oracle/gcs/bq) and pipeline modes (2-stage / 3-stage) |
| 2 | Profile & Recommend | User enters table name, picks mode | React calls /recommend. Spring Boot profiles the table and returns ranked execution candidates |
| 3 | Review & Confirm | User sees candidates, confirms one | React calls /template-details. User reviews, then clicks Run. |
| 4 | Execute          | User clicks Run    | React POSTs /execute. Spring Boot returns 202 + jobId immediately. Dataflow job starts asynchronously. |
| 5 | Monitor          | Loading in progress | React subscribes to SSE /jobs/{jobId}/stream OR polls /jobs/{jobId}/status every 5s. Progress bar, stage updates, final result. |

### 3.1 Sequence Diagram (Text)

```
React                    Cloud Run (Spring Boot)           Dataflow
  |                              |                             |
  |--- GET /pipeline-handlers -->|                             |
  |<-- { sources, targets } -----|                             |
  |--- GET /recommend?mode=... ->|                             |
  |    { table: 'orders' }       | profile table              |
  |                              | generate candidates         |
  |<-- { recommended, list } ----|                             |
  |--- POST /execute ----------->|                             |
  |    { candidate, source... }  | submit Dataflow job ------> |
  |<-- 202 { jobId } ------------|                             |
  |--- SSE /jobs/{id}/stream --->|                             |
  |         (keep-alive)         | poll Dataflow every 10s <-> |
  |<-- data: { pct: 40 } --------|                             |
  |<-- data: { pct: 80 } --------|                             |
  |<-- data: { done: true } -----|                             |
```

---

## 4. API Reference

All endpoints are prefixed with `/streamnova`. Base: `https://<cloud-run-url>/streamnova`

### 4.1 Discovery

**GET /api/agent/pipeline-handlers**  
Returns registered source/target handlers and current pipeline selection.

```javascript
// React
const { data } = await axios.get(`${BASE_URL}/api/agent/pipeline-handlers`);

// Response
{
  "handlers": ["postgres", "oracle", "gcs", "bigquery", "hive"],
  "configuredKeys": { "source": "postgres", "target": "gcs" }
}
```

**GET /api/agent/pipeline-listener/stages-mapping**  
Returns available pipeline modes (2-stage, 3-stage, validate-only).

```javascript
const { data } = await axios.get(`${BASE_URL}/api/agent/pipeline-listener/stages-mapping`);

// Response
{
  "2-stage": { "actions": ["SOURCE_READ", "TARGET_WRITE"] },
  "3-stage": { "actions": ["SOURCE_READ", "INTERMEDIATE_WRITE", "TARGET_WRITE"] }
}
```

### 4.2 Recommend

**GET /api/agent/recommend**  
Profiles the source table and returns ranked execution candidates.

| Query Param           | Type    | Description                    |
|-----------------------|---------|--------------------------------|
| mode                  | string  | COST_OPTIMAL \| FAST_LOAD \| BALANCED |
| maxCostUsd            | number (optional) | Maximum job cost guardrail |
| maxDurationSec        | number (optional) | Maximum job duration guardrail |
| minThroughputMbPerSec | number (optional) | Minimum required throughput |

```javascript
const { data } = await axios.get(`${BASE_URL}/api/agent/recommend`, {
  params: { mode: 'BALANCED', maxCostUsd: 10, maxDurationSec: 1800 }
});

// Response
{
  "executionRunId": "run-a1b2c3",
  "recommended": {
    "machineType": "n2-standard-4",
    "workerCount": 2,
    "shardCount": 8,
    "suggestedPoolSize": 16,
    "estimatedDurationSec": 420,
    "estimatedCostUsd": 3.40,
    "label": "BALANCED"
  },
  "cheapest": { ... },
  "fastest":  { ... },
  "balanced": { ... },
  "durationEstimate": { "low": 350, "mid": 420, "high": 600 }
}
```

### 4.3 Execute

**POST /api/agent/execute**  
Submits the pipeline job to Dataflow. Returns 202 with jobId immediately — always async on Cloud Run.

```javascript
const { data } = await axios.post(`${BASE_URL}/api/agent/execute`, {
  candidate: {
    machine_type: 'n2-standard-4',
    worker_count: 2,
    shard_count: 8,
    suggested_pool_size: 16
  },
  execution_run_id: 'run-a1b2c3',   // from /recommend
  source:        'postgres',
  intermediate:  'gcs',              // omit for 2-stage
  target:        'bigquery',
  source_schema: 'public',
  source_table:  'orders',
  extraction_mode: 'FULL'           // FULL | INCREMENTAL
});

// Response (202 Accepted)
{
  "success": true,
  "jobId":   "2026-02-28_dataflow_xyz",
  "stages":  3,
  "message": "Pipeline completed successfully"
}
```

### 4.4 Job Monitoring

**Option A — SSE Stream (Recommended)**  
Spring Boot pushes updates via Server-Sent Events. React uses the EventSource API. Works on Cloud Run with min-instances=1.

```javascript
// React — useJobMonitor.js
import { useEffect, useState } from 'react';

export function useJobMonitor(jobId) {
  const [progress, setProgress] = useState({ pct: 0, stage: '', done: false });
  useEffect(() => {
    if (!jobId) return;
    const es = new EventSource(`${BASE_URL}/api/agent/jobs/${jobId}/stream`);
    es.onmessage = (e) => {
      const update = JSON.parse(e.data);
      setProgress(update);
      if (update.done || update.failed) es.close();
    };
    es.onerror = () => es.close();
    return () => es.close();
  }, [jobId]);
  return progress;
}

// SSE event payload shape from Spring Boot
// { pct: 65, stage: 'INTERMEDIATE_WRITE', rowsRead: 120000, throughputMbSec: 28.4, elapsedSec: 180, done: false }
// Final: { pct: 100, stage: 'COMPLETE', totalRows: 250000, durationSec: 420, actualCostUsd: 3.21, done: true }
```

**Option B — Polling (Fallback)**  
If SSE is not configured, React polls the status endpoint every 5 seconds.

```javascript
const pollStatus = async (jobId) => {
  const interval = setInterval(async () => {
    const { data } = await axios.get(`${BASE_URL}/api/agent/jobs/${jobId}/status`);
    setProgress(data);
    if (data.done || data.failed) clearInterval(interval);
  }, 5000);
  return () => clearInterval(interval);
};

// Status response
// { jobId, state: "RUNNING", pct: 65, stage: "INTERMEDIATE_WRITE", rowsRead: 120000, done: false, failed: false }
```

### 4.5 Outcome Reporting (Learning Loop)

After a job completes, React should POST the actual outcome so future recommendations improve.

```javascript
await axios.post(`${BASE_URL}/api/agent/metrics/execution-outcome`, {
  runId:           'run-a1b2c3',
  success:         true,
  actualDurationSec: 420,
  actualCostUsd:   3.21
});
```

---

## 5. React State Machine

| State      | API Call              | React Component / Action                    |
|------------|------------------------|----------------------------------------------|
| IDLE       | GET /pipeline-handlers | Show source/target selector dropdowns        |
| CONFIGURING| — (user input)         | User fills table name, schema, mode, guardrails |
| PROFILING  | GET /recommend         | Spinner + 'Analysing table…'                 |
| REVIEWING  | GET /template-details  | Show candidate cards                        |
| SUBMITTING | POST /execute          | Spinner + 'Submitting to Dataflow…'          |
| RUNNING    | SSE /jobs/{id}/stream  | Progress bar, stage label, rows/sec          |
| SUCCEEDED  | POST /execution-outcome| Success banner, rows loaded, cost, duration  |
| FAILED     | —                      | Error message, retry with next-best candidate |

### 5.1 Suggested React Hook

```javascript
// hooks/useStreamNovaAgent.js
import { useState, useCallback } from 'react';
import axios from 'axios';

export function useStreamNovaAgent() {
  const [state, setState]     = useState('IDLE');
  const [candidates, setCandidates] = useState([]);
  const [jobId, setJobId]     = useState(null);
  const [runId, setRunId]     = useState(null);
  const [progress, setProgress] = useState({});
  const [error, setError]     = useState(null);

  const recommend = useCallback(async (params) => {
    setState('PROFILING');
    const { data } = await axios.get('/api/agent/recommend', { params });
    setCandidates([data.recommended, data.cheapest, data.fastest]);
    setRunId(data.executionRunId);
    setState('REVIEWING');
  }, []);

  const execute = useCallback(async (candidate, opts) => {
    setState('SUBMITTING');
    const { data } = await axios.post('/api/agent/execute', {
      candidate, execution_run_id: runId, ...opts
    });
    setJobId(data.jobId);
    setState('RUNNING');
    // SSE monitor starts via useJobMonitor(jobId)
  }, [runId]);

  return { state, candidates, jobId, progress, error, recommend, execute };
}
```

---

## 6. Cloud Run Configuration

| Setting                    | Value | Reason |
|----------------------------|-------|--------|
| --min-instances            | 1     | Prevents SSE connections being dropped on cold start |
| --timeout                  | 3600  | Max allowed; needed for SSE streams and large jobs |
| --concurrency              | 80    | Each SSE connection holds a slot; tune for concurrent users |
| --allow-unauthenticated    | false | Use Firebase Auth + IAM; frontend attaches Bearer token |
| CORS headers               | React origin | Spring Boot needs Access-Control-Allow-Origin for your React domain |

### 6.1 Spring Boot SSE Endpoint (To Build)

This endpoint does not yet exist in StreamNova and must be added:

```java
// Spring Boot — JobMonitorController.java
@GetMapping(value = "/api/agent/jobs/{jobId}/stream",
            produces = MediaType.TEXT_EVENT_STREAM_VALUE)
public SseEmitter streamJobStatus(@PathVariable String jobId) {
    SseEmitter emitter = new SseEmitter(3_600_000L); // 1 hour
    scheduledExecutor.scheduleAtFixedRate(() -> {
        DataflowJobStatus status = dataflowService.getStatus(jobId);
        try {
            emitter.send(SseEmitter.event().data(status));
            if (status.isDone() || status.isFailed()) emitter.complete();
        } catch (Exception e) { emitter.completeWithError(e); }
    }, 0, 10, TimeUnit.SECONDS);
    return emitter;
}
```

---

## 7. Error Handling in React

| HTTP Status | Scenario                    | React Action |
|-------------|-----------------------------|--------------|
| 400         | Bad request (invalid table, missing params) | Show inline validation error |
| 401         | Auth token expired          | Auto-refresh Firebase token and retry once |
| 404         | Handler / config not found  | Show 'source not configured' message |
| 429         | Too many requests (agent busy) | Show 'agent busy' with retry countdown |
| 500         | Pipeline execution error    | Show response.message, offer retry with next-best candidate |

```javascript
axios.interceptors.response.use(
  res => res,
  async (err) => {
    if (err.response?.status === 401) {
      const token = await auth.currentUser.getIdToken(true);
      err.config.headers.Authorization = `Bearer ${token}`;
      return axios(err.config);  // retry once
    }
    const msg = err.response?.data?.message || 'Unexpected error';
    notificationStore.error(msg);
    return Promise.reject(err);
  }
);
```

---

## 8. React Quick-Start Checklist

Steps for the React team to integrate with StreamNova on Cloud Run:

1. Set `REACT_APP_API_URL` to Cloud Run base URL in `.env.production`
2. Set `REACT_APP_API_URL` to `http://localhost:8080/streamnova` in `.env.development`
3. Install axios: `npm install axios`
4. Add Firebase Auth: `npm install firebase` (for Bearer token)
5. Configure CORS in Spring Boot to allow React origin
6. Implement `useStreamNovaAgent` hook (see Section 5.1)
7. Implement `useJobMonitor` hook using EventSource for SSE (see Section 4.4)
8. Add polling fallback for environments where SSE is blocked
9. Report execution outcome after each successful job (see Section 4.5)
10. Set Cloud Run `--min-instances=1` so SSE connections survive

**Note:** The `/api/agent/jobs/{jobId}/stream` SSE endpoint and `/api/agent/jobs/{jobId}/status` polling endpoint do not yet exist in StreamNova and must be added to Spring Boot before React monitoring can work end-to-end.
