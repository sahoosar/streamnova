# StreamNova – API URIs (how to invoke the application)

**Base URL:** `http://localhost:8080/streamnova`  
(context-path: `/streamnova`; change host/port for your environment)

---

## 1. Handlers and pipeline selection

| Purpose | Method | URI | Notes |
|--------|--------|-----|--------|
| List handlers | GET | `/streamnova/api/agent/pipeline-handlers` | Available source handlers, configured keys, current pipeline selection. Use for source/intermediate/target choices. |

**Example:**  
`GET http://localhost:8080/streamnova/api/agent/pipeline-handlers`

---

## 2. Listener config (stages, actions, datasource)

| Purpose | Method | URI | Notes |
|--------|--------|-----|--------|
| Stages mapping | GET | `/streamnova/api/agent/pipeline-listener/stages-mapping` | All modes (2-stage, 3-stage, validate-only). |
| Stages mapping (one stage) | GET | `/streamnova/api/agent/pipeline-listener/stages-mapping?stage=2` | Only 2-stage. |
| | GET | `/streamnova/api/agent/pipeline-listener/stages-mapping?stage=3` | Only 3-stage. |
| Actions mapping | GET | `/streamnova/api/agent/pipeline-listener/actions-mapping?source=postgres&target=gcs` | 2-stage: source + target. |
| | GET | `/streamnova/api/agent/pipeline-listener/actions-mapping?source=postgres&intermediate=gcs&target=bigquery` | 3-stage. |
| Datasource details | GET | `/streamnova/api/agent/pipeline-listener/datasource-details?source=postgres&target=gcs` | Connection details per stage. |
| Create datasource | POST | `/streamnova/api/agent/pipeline-listener/create-datasource` | Body: `{ "source": "postgres", "target": "gcs" }` (add `"intermediate": "gcs"` for 3-stage). |
| Template details (GET) | GET | `/streamnova/api/agent/pipeline-listener/template-details?source=postgres&target=gcs` | All calculated template values (password masked). Optional: `&intermediate=...` |
| Template details (POST) | POST | `/streamnova/api/agent/pipeline-listener/template-details` | Body: `{ "source", "target", "intermediate?", "candidate?", "source_schema?", "source_table?", ... }`. |

**Examples:**  
- `GET http://localhost:8080/streamnova/api/agent/pipeline-listener/stages-mapping`  
- `GET http://localhost:8080/streamnova/api/agent/pipeline-listener/actions-mapping?source=postgres&target=gcs`  
- `GET http://localhost:8080/streamnova/api/agent/pipeline-listener/datasource-details?source=postgres&target=gcs`  
- `POST http://localhost:8080/streamnova/api/agent/pipeline-listener/create-datasource`  
  `Content-Type: application/json`  
  `{ "source": "postgres", "target": "gcs" }`  
- `GET http://localhost:8080/streamnova/api/agent/pipeline-listener/template-details?source=postgres&target=gcs`  
- `POST http://localhost:8080/streamnova/api/agent/pipeline-listener/template-details`  
  `Content-Type: application/json`  
  `{ "source": "postgres", "target": "gcs", "candidate": { "worker_count": 2, "shard_count": 4 }, "source_table": "public.my_table" }`

---

## 3. Execute pipeline

| Purpose | Method | URI | Notes |
|--------|--------|-----|--------|
| Execute pipeline | POST | `/streamnova/api/agent/execute` | Body: candidate + optional source, intermediate, target, schema/table/extraction params. |

**Body (JSON):**  
- `candidate`: `{ "machine_type", "worker_count", "shard_count", "virtual_cpus?", "suggested_pool_size?", "label?" }` (required)  
- `execution_run_id?`: run ID from recommend (optional)  
- `source?`: e.g. `"postgres"`  
- `intermediate?`: e.g. `"gcs"` (3-stage)  
- `target?`: e.g. `"gcs"`  
- `source_schema?`, `source_table?`, `source_query?`, `extraction_mode?`, `incremental_column?`, `watermark_from?`, `watermark_to?` (optional, for template)

**Example:**  
`POST http://localhost:8080/streamnova/api/agent/execute`  
`Content-Type: application/json`  
```json
{
  "candidate": {
    "machine_type": "n2-standard-4",
    "worker_count": 2,
    "shard_count": 4,
    "suggested_pool_size": 8
  },
  "source": "postgres",
  "target": "gcs",
  "source_table": "public.market_summary"
}
```

---

## 4. Pool statistics

| Purpose | Method | URI | Notes |
|--------|--------|-----|--------|
| Pool statistics | GET | `/streamnova/api/pipeline/pool-statistics` | HikariCP pool metrics. |

**Example:**  
`GET http://localhost:8080/streamnova/api/pipeline/pool-statistics`

---

## 5. Agent: candidates, recommend, profiler

| Purpose | Method | URI | Notes |
|--------|--------|-----|--------|
| Generate candidates | GET | `/streamnova/api/agent/candidates/generate` | Profile + generate execution candidates. |
| Generate from profile | GET | `/streamnova/api/agent/candidates/generate-from-profile?runId=...` | From stored profile. |
| Recommend | GET | `/streamnova/api/agent/recommend?mode=COST_OPTIMAL|FAST_LOAD|BALANCED&maxCostUsd=&maxDurationSec=&minThroughputMbPerSec=` | Recommended candidate. |
| Profiler profile | GET | `/streamnova/api/agent/profiler/profile` | Run profiler. |
| Profiler recent | GET | `/streamnova/api/agent/profiler/recent` | Recent profile runs. |

**Examples:**  
- `GET http://localhost:8080/streamnova/api/agent/candidates/generate`  
- `GET http://localhost:8080/streamnova/api/agent/recommend?mode=COST_OPTIMAL`  
- `GET http://localhost:8080/streamnova/api/agent/profiler/profile`

---

## Monitoring (Prometheus / metrics)

When `streamnova.aspect.metrics-enabled` is `true` (default), the transaction-tracking aspect publishes:

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `streamnova.tx.duration` | Timer | `layer` (CONTROLLER/SERVICE), `status` (success/failure) | Invocation duration. |
| `streamnova.tx.count` | Counter | `layer`, `status` | Invocation count. |

These are exposed via Actuator Prometheus (`/streamnova/actuator/prometheus`). Log lines `[TX-START]` / `[TX-END]` / `[TX-FAIL]` include `txId` for correlation; set `streamnova.aspect.structured-logging: true` for key=value lines (e.g. for ELK/Splunk). See `streamnova.aspect.*` in `application.yml`.

**Multithreaded logging:** MDC (and thus `txId`) is thread-local. For work run on other threads (e.g. `ExecutorService`, `CompletableFuture`, `@Async`), use `MdcPropagation.runWithTxId(Runnable)` / `MdcPropagation.callWithTxId(Callable)` when submitting tasks, or `MdcPropagation.wrapExecutor(executor)` so all submitted tasks inherit the request's `txId`.

---

## Typical flow (2-stage: postgres → gcs)

1. **Handlers**  
   `GET /streamnova/api/agent/pipeline-handlers`

2. **Stages / actions**  
   `GET /streamnova/api/agent/pipeline-listener/stages-mapping`  
   `GET /streamnova/api/agent/pipeline-listener/actions-mapping?source=postgres&target=gcs`

3. **Template values (optional)**  
   `GET /streamnova/api/agent/pipeline-listener/template-details?source=postgres&target=gcs`

4. **Create datasource**  
   `POST /streamnova/api/agent/pipeline-listener/create-datasource`  
   Body: `{ "source": "postgres", "target": "gcs" }`

5. **Datasource details (optional)**  
   `GET /streamnova/api/agent/pipeline-listener/datasource-details?source=postgres&target=gcs`

6. **Execute**  
   `POST /streamnova/api/agent/execute`  
   Body: `{ "candidate": { ... }, "source": "postgres", "target": "gcs" }`  
   (candidate can come from `GET /streamnova/api/agent/recommend?mode=...`)

7. **Pool stats (optional)**  
   `GET /streamnova/api/pipeline/pool-statistics`
