"""
Materializes gold-layer dbt tables as real Delta Lake tables on local disk.

Why this is a separate step instead of a dbt materialization: dbt-duckdb
ships a `delta` plugin, but it only implements the read side (loading an
existing Delta table as a source) -- there's no built-in write/store path
for it. Rather than fake that with a stub, this script does the one honest
thing: query the finished gold table out of DuckDB, hand it to DuckDB as
Arrow, and let the real `deltalake` library write it. Swapping the local
`data/delta/` path for an S3 URI later is a one-line change (deltalake
takes `storage_options` for that) -- everything upstream stays the same.

Usage: python scripts/write_gold_to_delta.py
"""
import sys
import duckdb
from deltalake import write_deltalake, DeltaTable

WAREHOUSE_PATH = "data/warehouse.duckdb"
DELTA_ROOT = "data/delta"

GOLD_TABLES = [
    "gold.gold_311_daily_agency_summary",
    "gold.gold_commit_activity_daily",
    "gold.gold_sec_filings_by_state_month",
]


def write_table(con, qualified_name: str) -> int:
    table_name = qualified_name.split(".")[-1]
    arrow_table = con.sql(f"select * from {qualified_name}").to_arrow_table()
    delta_path = f"{DELTA_ROOT}/{table_name}"

    write_deltalake(delta_path, arrow_table, mode="overwrite")

    dt = DeltaTable(delta_path)
    row_count = arrow_table.num_rows
    print(f"[{table_name}] wrote {row_count} rows -> {delta_path} (delta version {dt.version()})")
    return row_count


def main():
    con = duckdb.connect(WAREHOUSE_PATH, read_only=True)
    total = 0
    for qualified_name in GOLD_TABLES:
        total += write_table(con, qualified_name)
    print(f"Done. {total} total rows across {len(GOLD_TABLES)} Delta tables.")


if __name__ == "__main__":
    sys.exit(main())
