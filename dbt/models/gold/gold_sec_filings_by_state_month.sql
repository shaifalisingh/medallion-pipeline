{{
    config(
        materialized='incremental',
        unique_key=['file_month', 'biz_state', 'form_type'],
        incremental_strategy='delete+insert',
    )
}}

-- Gold: grain is one row per (month, business-registered state, form type).
-- Third, structurally unrelated domain -- proves the medallion pattern
-- (quarantine, incremental + lookback, Delta export) generalizes across
-- all three of Project 1's live sources, not just two of them.
--
-- coalesce(biz_state, ...) is load-bearing, not cosmetic: dbt-duckdb's
-- delete+insert strategy matches the unique_key with plain `=`, which is
-- not null-safe (null = null is null, not true). A nullable grain column
-- left as-is would mean the delete step never matches an existing
-- biz_state-is-null row on a later incremental run, so insert silently
-- adds a duplicate instead of replacing it. Coalescing to a sentinel here
-- keeps the unique_key always non-null and the delete+insert safe.
select
    date_trunc('month', file_date)       as file_month,
    coalesce(biz_state, 'UNSPECIFIED')    as biz_state,
    form_type,
    count(*)                              as filing_count,
    count(distinct cik)                   as unique_filers
from {{ ref('slv_sec_filings') }}
{% if is_incremental() %}
where file_date >= (
    select coalesce(max(file_month), timestamp '1900-01-01') from {{ this }}
) - interval '{{ var("late_arrival_lookback_days") }} days'
{% endif %}
group by 1, 2, 3
