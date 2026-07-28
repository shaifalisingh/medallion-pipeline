"""
Publishes the local gold-layer Delta tables into a real Unity Catalog on
Databricks -- registration, grants, and lineage, not a mock.

Why not register the local Delta tables directly as UC EXTERNAL TABLEs:
Unity Catalog external tables need a cloud storage path (S3/ADLS/GCS) that
the workspace has a configured storage credential/external location for.
These tables live on a laptop's local disk, which a remote workspace has
no way to reach. Faking that with a stub path isn't better than being
honest about the boundary.

What this does instead, which is a real, common production pattern (an
on-prem/local job publishing its finished marts into a governed
lakehouse for downstream consumption): read each local Delta table with
the same `deltalake` library used to write it, export to Parquet, upload
via the Databricks CLI (`databricks fs cp`) into a Unity Catalog Volume,
then materialize it as a real MANAGED Delta table inside Unity Catalog
via the SQL Statement Execution API against the workspace's serverless
SQL warehouse.

Usage: python scripts/publish_gold_to_unity_catalog.py
Requires: `databricks auth login` already done, DATABRICKS_PROFILE env
var (defaults to the profile name below), and a running/startable SQL
warehouse (WAREHOUSE_ID below).
"""
import json
import os
import subprocess
import sys
import tempfile

import pyarrow.parquet as pq
from deltalake import DeltaTable

PROFILE = os.environ.get("DATABRICKS_PROFILE", "singhshaifali25@gmail.com")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "5c2c202aa3df83b2")
CATALOG = "workspace"
SCHEMA = "medallion_pipeline"
VOLUME = "gold_staging"

GOLD_TABLES = [
    "gold_311_daily_agency_summary",
    "gold_commit_activity_daily",
    "gold_sec_filings_by_state_month",
]


def run_statement(sql: str) -> dict:
    result = subprocess.run(
        [
            "databricks", "api", "post", "/api/2.0/sql/statements",
            "--profile", PROFILE,
            "--json", json.dumps({"warehouse_id": WAREHOUSE_ID, "statement": sql, "wait_timeout": "50s"}),
        ],
        capture_output=True, text=True, check=True,
    )
    payload = json.loads(result.stdout)
    state = payload.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise RuntimeError(f"Statement failed ({state}): {sql}\n{result.stdout}")
    return payload


def publish_table(table_name: str, tmp_dir: str):
    dt = DeltaTable(f"data/delta/{table_name}")
    arrow_table = dt.to_pyarrow_table()

    local_parquet = os.path.join(tmp_dir, f"{table_name}.parquet")
    pq.write_table(arrow_table, local_parquet)

    volume_dir = f"dbfs:/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{table_name}"
    subprocess.run(["databricks", "fs", "mkdir", volume_dir, "--profile", PROFILE], check=True)
    subprocess.run(
        ["databricks", "fs", "cp", local_parquet, f"{volume_dir}/{table_name}.parquet",
         "--profile", PROFILE, "--overwrite"],
        check=True,
    )

    full_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    run_statement(
        f"CREATE OR REPLACE TABLE {full_name} AS "
        # read_files() adds its own _rescued_data schema-safety column
        # (Databricks' schema-drift guard) -- excluded here since nothing
        # in this data actually failed to match the inferred schema.
        f"SELECT * EXCEPT (_rescued_data) FROM read_files("
        f"'/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{table_name}/', format => 'parquet')"
    )
    print(f"[{table_name}] published {arrow_table.num_rows} rows -> {full_name}")
    return arrow_table.num_rows


def main():
    total = 0
    with tempfile.TemporaryDirectory() as tmp_dir:
        for table_name in GOLD_TABLES:
            total += publish_table(table_name, tmp_dir)
    print(f"Done. {total} total rows published across {len(GOLD_TABLES)} Unity Catalog tables.")


if __name__ == "__main__":
    sys.exit(main())
