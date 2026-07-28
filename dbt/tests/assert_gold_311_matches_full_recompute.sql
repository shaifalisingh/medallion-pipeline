-- Generic detector for "gold is stale because incremental logic missed
-- something" -- doesn't need to know which day was late. Recomputes the
-- same aggregate fresh from all currently-valid silver rows and diffs it
-- against what's actually sitting in the incremental gold table. Any
-- mismatch (including a grain gold is missing entirely) fails the test.
with recomputed as (
    select
        date_trunc('day', created_at) as request_date,
        agency,
        complaint_type,
        count(*) as request_count
    from {{ ref('slv_311_requests') }}
    group by 1, 2, 3
)
select
    r.request_date,
    r.agency,
    r.complaint_type,
    r.request_count as expected_count,
    g.request_count as actual_count
from recomputed r
left join {{ ref('gold_311_daily_agency_summary') }} g
    on r.request_date = g.request_date
   and r.agency = g.agency
   and r.complaint_type = g.complaint_type
where g.request_count is null
   or g.request_count != r.request_count
