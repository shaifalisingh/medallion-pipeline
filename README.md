# Medallion Pipeline

Bronze → silver → gold, built on real dbt, real Airflow, and a real local
Delta Lake -- not stand-ins for those tools. This is Project 2: it picks up
exactly where [connector-starter-kit](https://github.com/shaifalisingh/connector-starter-kit)
(Project 1) leaves off. That kit's job ends at landing raw JSONL in a
`bronze/` layer; this project's job starts there.

## Architecture

```
bronze_source/*.jsonl          (raw, from Project 1's connector -- snapshotted here)
      |
      v   dbt-duckdb, reading JSON directly off disk (no load step)
staging.*                      thin views: rename/cast only, no business logic
      |
      v
silver.*                       typed, deduped, conformed -- what analysts query
      |
      v
gold.*                         BI-ready aggregates, one grain per mart
      |
      v   scripts/write_gold_to_delta.py
data/delta/*                   real Delta Lake tables (_delta_log, versioned, Parquet)
```

Orchestrated by an Airflow DAG (`dags/medallion_pipeline_dag.py`):

```
check_bronze_data  ->  dbt_run  ->  dbt_test  ->  write_gold_to_delta
```

Each arrow is a hard gate, not a suggestion. If bronze data is missing, the
first task raises and nothing downstream runs. If a dbt test fails, the
Delta write never happens -- bad data doesn't reach the layer other systems
would read from. That's the actual point of putting a data-quality gate
*before* the write step instead of after it.

## Why this stack, run this way

- **dbt-duckdb, not a warehouse.** The SQL (staging/silver/gold logic) is
  the artifact that matters and is warehouse-portable. DuckDB just means
  this runs on a laptop with no infra bill. Swap the `dbt/profiles.yml`
  target for Snowflake/BigQuery/Redshift later -- the models don't change.
- **A real local Delta Lake, not a claim of one.** dbt-duckdb ships a
  `delta` plugin, but it only implements the *read* side (loading an
  existing Delta table as a source) -- there's no write/store path in it.
  Rather than fake that, `scripts/write_gold_to_delta.py` queries the
  finished gold table out of DuckDB as Arrow and hands it to the real
  `deltalake` Python library to write. Verified independently in this
  build by reading the table back with `DeltaTable(path)` in a separate
  process -- real `_delta_log/*.json` transaction files, real versioned
  Parquet parts, not a stub.
- **A real Airflow DAG, not a script that logs "Task 1 done."** Runs on
  Apache Airflow 3.x with a local SQLite metadata DB. Proven with
  `airflow dags test medallion_pipeline <date>`, which executes the actual
  DAG object -- same task graph, same operators, same failure semantics
  a scheduled run would have. Swapping in a real scheduler + LocalExecutor
  (or CeleryExecutor for multiple workers) is an infra change, not a DAG
  rewrite.

## What's proven, live, in this build

```
check_bronze_data  -> found 5 bronze files, proceeded
dbt_run             -> 6 models built (2 staging views, 2 silver tables, 2 gold tables)
dbt_test            -> 12/12 passed (uniqueness + not-null on natural keys)
write_gold_to_delta -> 123 rows written across 2 real Delta tables
Dag run             -> success
```

Two structurally unrelated domains modeled end-to-end (mirrors Project 1's
"prove genericity across sources" pattern, applied here to "prove the
medallion pattern generalizes across domains"):

| Domain | Silver grain | Gold grain | Gold rows |
|---|---|---|---|
| NYC 311 requests | 1 row / request (deduped on `request_id`) | 1 row / (day, agency, complaint_type) | 94 |
| GitHub commits | 1 row / commit (deduped on `commit_sha`) | 1 row / (day, author) | 29 |

`sec_edgar_10k` bronze data is wired up as a dbt source but has no
staging/silver/gold models yet -- see Known limitations.

## Running it yourself

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt

# dbt
.venv/bin/dbt run --project-dir dbt --profiles-dir dbt
.venv/bin/dbt test --project-dir dbt --profiles-dir dbt

# Delta write
.venv/bin/python scripts/write_gold_to_delta.py

# Airflow -- point AIRFLOW_HOME at a local dir, migrate the metadata DB once,
# and set dags_folder in the generated airflow.cfg to this repo's dags/
export AIRFLOW_HOME="$(pwd)/.airflow"
.venv/bin/airflow db migrate
# then edit .airflow/airflow.cfg: dags_folder = <this repo>/dags
.venv/bin/airflow dags test medallion_pipeline 2026-07-28
```

Inspect the Delta tables independently of dbt/DuckDB:

```python
from deltalake import DeltaTable
dt = DeltaTable("data/delta/gold_311_daily_agency_summary")
print(dt.version(), dt.to_pyarrow_table().num_rows)
```

## Known limitations (being upfront, not hiding them)

- **`sec_edgar_10k` bronze data has no models yet.** It's a valid dbt
  source (see `dbt/models/staging/sources.yml`) but nothing reads it.
  Straightforward to add -- staging view, silver dedup on `_id`, a gold
  aggregate -- just not built out in this pass.
- **Airflow runs via `dags test`, not a live scheduler.** This executes
  the real DAG and real operators end-to-end, but a persistent
  scheduler/webserver/triggerer stack (what you'd actually deploy) wasn't
  stood up here. `airflow standalone` would get you that; didn't run it
  continuously for a portfolio piece that doesn't need to always be up.
- **Delta tables are local-disk only.** `write_deltalake` takes
  `storage_options` for S3/ADLS/GCS -- swapping the local path for an S3
  URI plus credentials is the whole change needed for cloud storage; not
  wired up here because it needs real cloud credentials to prove out (same
  reasoning as Project 1's un-built S3Writer).
- **No incremental/merge logic in the gold write.** Every run is a full
  `mode="overwrite"` of each Delta table. Fine for daily batch aggregates
  at this scale; a real incremental `MERGE` (Delta's `merge()` API) would
  be the next step for larger, append-heavy gold tables.
- **Bronze data here is a snapshot, not a live read.** `bronze_source/` is
  a copy of Project 1's actual live output (see its README for how it was
  produced), not a live pointer at Project 1's `data/bronze/`. Decoupled
  on purpose so this repo is independently cloneable and runnable without
  Project 1 present -- in production both would point at the same S3
  bronze bucket.
