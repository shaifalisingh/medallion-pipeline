-- Gold: grain is one row per (day, author). Second, structurally unrelated
-- domain -- proves the medallion pattern (and the Delta write step) is
-- generic across sources, not hardcoded to the 311 pipeline.
select
    authored_date,
    author_name,
    author_email,
    count(*) as commit_count
from {{ ref('slv_commits') }}
group by 1, 2, 3
