{{
    config(
        materialized='incremental',
        unique_key=['authored_date', 'author_name', 'author_email'],
        incremental_strategy='delete+insert',
    )
}}

-- Gold: grain is one row per (day, author). Second, structurally unrelated
-- domain -- proves the medallion pattern (incremental + lookback window,
-- not just the Delta write step) is generic across sources, not
-- hardcoded to the 311 pipeline.
select
    authored_date,
    author_name,
    author_email,
    count(*) as commit_count
from {{ ref('slv_commits') }}
{% if is_incremental() %}
where authored_date >= (
    select coalesce(max(authored_date), timestamp '1900-01-01') from {{ this }}
) - interval '{{ var("late_arrival_lookback_days") }} days'
{% endif %}
group by 1, 2, 3
