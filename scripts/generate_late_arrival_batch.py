"""
Generates a single late-arriving record: a NYC 311 complaint whose
created_date falls on 2026-07-26 -- a day gold has already aggregated --
but that shows up in a bronze batch ingested days later (2026-08-01).
This is the realistic case (a complaint gets backfilled, corrected, or
just processed slowly upstream) that a naive incremental filter
("only rows newer than what we've already processed") would silently
never pick up.

Targets an existing (day, agency, complaint_type) grain
(2026-07-26 / NYPD / Noise - Street/Sidewalk, baseline count 271) so the
before/after is a single, easy-to-verify number: 271 -> 272.

Usage: python scripts/generate_late_arrival_batch.py
"""
import glob
import json
import os

SRC = glob.glob("bronze_source/nyc_311/ingest_date=2026-07-28/*.jsonl")[0]
OUT = "bronze_source/nyc_311/ingest_date=2026-08-01/nyc_311_late_arrival.jsonl"


def main():
    with open(SRC) as f:
        template = json.loads(f.readline())

    late_record = dict(template)
    late_record["unique_key"] = "99999001"
    late_record["created_date"] = "2026-07-26T23:50:00.000"
    late_record["agency"] = "NYPD"
    late_record["agency_name"] = "New York City Police Department"
    late_record["complaint_type"] = "Noise - Street/Sidewalk"
    late_record["status"] = "Closed"
    late_record["closed_date"] = "2026-08-01T09:00:00.000"

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        f.write(json.dumps(late_record) + "\n")
    print(f"Wrote 1 late-arriving record (created_date=2026-07-26, ingested 2026-08-01) -> {OUT}")


if __name__ == "__main__":
    main()
