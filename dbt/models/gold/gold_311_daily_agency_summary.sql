{{
    config(
        materialized='incremental',
        unique_key=['request_date', 'agency', 'complaint_type'],
        incremental_strategy='delete+insert',
    )
}}

-- Gold: BI-ready aggregate. Grain is one row per (day, agency, complaint_type).
-- This table is what gets materialized to Delta Lake -- the mart, not the
-- raw or intermediate layers.
--
-- Incremental with a lookback window, not a naive "only rows newer than
-- what we've already processed" filter. A late-arriving record for a day
-- we already aggregated (a complaint that got backfilled, a correction)
-- would be silently invisible forever under the naive version, since it's
-- never newer than the watermark. Reprocessing the last N days on every
-- run means delete+insert replaces that day's row with the corrected
-- count instead of leaving a stale one sitting there.
-- See dbt/tests/assert_gold_311_matches_full_recompute.sql, which is what
-- would actually catch it if this window were too short (or missing).
select
    date_trunc('day', created_at)                                as request_date,
    agency,
    agency_name,
    complaint_type,
    count(*)                                                      as request_count,
    avg(resolution_hours)                                         as avg_resolution_hours,
    count(*) filter (where status = 'Closed')                     as closed_count,
    count(*) filter (where status != 'Closed' or status is null)  as open_count
from {{ ref('slv_311_requests') }}
{% if is_incremental() %}
where created_at >= (
    select coalesce(max(request_date), timestamp '1900-01-01') from {{ this }}
) - interval '{{ var("late_arrival_lookback_days") }} days'
{% endif %}
group by 1, 2, 3, 4
