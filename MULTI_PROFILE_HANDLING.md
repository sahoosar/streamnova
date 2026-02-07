# Handling Multiple Spring Profiles

When you activate **more than one profile** (e.g. `postgres,oracle` or `postgres,postgres-light`), Spring loads **all** matching `application-<profile>.properties` in order. Later profiles override earlier ones for the **same property key**. Here is how StreamNova handles common cases.

---

## 1. Two databases: Postgres + Oracle

**Profiles:** `postgres,oracle`  
**Config:** `streamnova.statistics.supported-source-types=postgres,oracle`

- Spring loads `application-postgres.properties` then `application-oracle.properties`.
- Both set `streamnova.pipeline.config-file`; the **last** profile wins (oracle → `oracle_pipeline_config.yml`).
- **PipelineConfigService** loads that primary file, then **merges** the other DB’s config from the classpath, so you end up with **both** sources: `postgres` and `oracle`.
- **Default source:** When no `source` is passed (e.g. in API), the app uses `pipeline.config.defaultSource` from the **primary** YAML, or the override below. To choose which DB is default when both are active, set in `application.properties` (or a profile):

  ```properties
  streamnova.pipeline.default-source=postgres
  ```
  or `oracle`. If unset, the primary loaded YAML’s `defaultSource` is used (so with `postgres,oracle` and oracle’s file as primary, default is oracle unless you override).

**Summary:** Use `postgres,oracle` and `supported-source-types=postgres,oracle`. Optionally set `streamnova.pipeline.default-source=postgres` (or `oracle`) so “default” is explicit. Both datasources are created at startup; APIs can use `?source=postgres` or `?source=oracle`.

---

## 2. Postgres + scenario (e.g. light / heavy)

**Profiles:** `postgres,postgres-light` or `postgres,postgres-heavy`

- `application-postgres.properties` sets `streamnova.pipeline.config-file` (postgres YAML).
- `application-postgres-light.properties` (or `-heavy`) only sets `streamnova.pipeline.scenario=light` (or `heavy`); it does **not** set `config-file`.
- So the config file stays the postgres one, and the **scenario** is applied on top. No conflict.

**Summary:** Safe to use. Config file comes from `postgres`; scenario comes from the second profile.

---

## 3. Postgres + environment (SIT / UAT / prod)

**Profiles:** `postgres,postgres-sit` or `postgres,postgres-uat` or `postgres,postgres-prod`

- `application-postgres.properties` sets the config file.
- `application-postgres-sit.properties` (etc.) overrides connection/table (e.g. jdbc URL, user, password, table) for that environment.
- Use **one** environment profile at a time (e.g. don’t use `postgres-sit,postgres-uat` together).

**Summary:** Use a single env profile with `postgres` to point that instance to SIT, UAT, or prod.

---

## 4. Order of profiles

Spring loads profiles in the order you list them; for a given key, the **last** value wins.

| Goal                         | Example active profiles     | Notes                                                                 |
|-----------------------------|-----------------------------|-----------------------------------------------------------------------|
| Only Postgres               | `postgres`                  | Default in `application.properties`.                                  |
| Postgres + Oracle           | `postgres,oracle`           | Set `supported-source-types=postgres,oracle`; optional `default-source`. |
| Postgres with “light” load  | `postgres,postgres-light`   | Scenario applied; config file from postgres.                           |
| Postgres SIT                | `postgres,postgres-sit`     | SIT overrides (URL, user, password, table).                           |

---

## 5. Override default source when multiple sources exist

When both `postgres` and `oracle` (or other sources) are in config, the default source for APIs that don’t pass `source=` is:

1. **`streamnova.pipeline.default-source`** in properties (if set), else  
2. **`pipeline.config.defaultSource`** in the primary loaded YAML, else  
3. First source key in the map.

Set explicitly when using multiple DBs:

```properties
# In application.properties or an active profile
streamnova.pipeline.default-source=postgres
```

This keeps behavior predictable when more than one profile (and thus more than one source) is active.
