with recomputed as (
    -- must mirror gold's coalesce(biz_state, ...) exactly, or this test
    -- flags a false mismatch between "null" and "UNSPECIFIED" for the
    -- same conceptual group.
    select
        date_trunc('month', file_date)      as file_month,
        coalesce(biz_state, 'UNSPECIFIED')   as biz_state,
        form_type,
        count(*) as filing_count
    from {{ ref('slv_sec_filings') }}
    group by 1, 2, 3
)
select
    r.file_month,
    r.biz_state,
    r.form_type,
    r.filing_count as expected_count,
    g.filing_count as actual_count
from recomputed r
left join {{ ref('gold_sec_filings_by_state_month') }} g
    -- biz_state can be null for a filing (no biz_states in the source
    -- record) -- plain `=` treats null-vs-null as no match, which
    -- silently hid a real row here. IS NOT DISTINCT FROM is null-safe.
    on r.file_month is not distinct from g.file_month
   and r.biz_state  is not distinct from g.biz_state
   and r.form_type  is not distinct from g.form_type
where g.filing_count is null
   or g.filing_count != r.filing_count
