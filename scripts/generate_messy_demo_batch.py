"""
Generates a deliberately messy batch of bronze records to prove the
data-quality gate actually does something, instead of just passing 16
green tests against data that was already clean.

Takes real records from the existing bronze files and mutates copies of
them with defects that show up constantly in real feeds:
  - a required field dropped entirely (upstream API just omits it)
  - a required field present but null
  - a renamed/drifted field name (unique_key -> unique_id), simulating
    an upstream schema change that silently breaks a hardcoded field ref

This batch is intentionally bad enough to push the rejection rate over
the 2% gate threshold (see dbt/tests/) so `dbt test` fails for real and
the pipeline halts before the Delta write -- not a hypothetical.

Usage: python scripts/generate_messy_demo_batch.py
"""
import glob
import json
import random

random.seed(7)

NYC_311_SRC = glob.glob("bronze_source/nyc_311/*/*.jsonl")[0]
GITHUB_SRC = glob.glob("bronze_source/github_commits/*/*.jsonl")[0]

NYC_311_OUT = "bronze_source/nyc_311/ingest_date=2026-07-29/nyc_311_174144.jsonl"
GITHUB_OUT = "bronze_source/github_commits/ingest_date=2026-07-29/github_commits_174121.jsonl"


def load_records(path):
    with open(path) as f:
        return [json.loads(line) for line in f]


def make_messy_311_batch(records, bad_per_defect=15, clean=15):
    # 3 defect types x bad_per_defect, plus a clean control group in the
    # same batch -- proves the gate doesn't just reject everything, only
    # the actually-broken rows.
    n = bad_per_defect * 3 + clean
    sample = random.sample(records, n)
    out = []
    for i, r in enumerate(sample):
        r = dict(r)
        r["unique_key"] = f"9{900000 + i}"  # fresh keys, not real duplicates
        defect = i // bad_per_defect if i < bad_per_defect * 3 else 3
        if defect == 0:
            del r["agency"]  # upstream just omitted the field
        elif defect == 1:
            r["created_date"] = None  # present but null
        elif defect == 2:
            r["unique_id"] = r.pop("unique_key")  # schema drift: renamed key
        # defect == 3: left clean, control group
        out.append(r)
    return out


def make_messy_commit_batch(records, n=6):
    sample = random.sample(records, n)
    out = []
    for i, r in enumerate(sample):
        r = dict(r)
        r["sha"] = f"deadbeef{i:032x}"[:40]  # fresh sha, not a real duplicate
        defect = i % 3
        if defect == 0:
            r["commit"]["author"]["date"] = None  # authored_at will be null
        elif defect == 1:
            del r["commit"]["author"]["date"]  # field dropped entirely
        # defect == 2: left clean
        out.append(r)
    return out


def write_jsonl(records, path):
    import os

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def main():
    nyc_311 = make_messy_311_batch(load_records(NYC_311_SRC))
    commits = make_messy_commit_batch(load_records(GITHUB_SRC))
    write_jsonl(nyc_311, NYC_311_OUT)
    write_jsonl(commits, GITHUB_OUT)
    print(f"Wrote {len(nyc_311)} messy nyc_311 records -> {NYC_311_OUT}")
    print(f"Wrote {len(commits)} messy commit records -> {GITHUB_OUT}")


if __name__ == "__main__":
    main()
