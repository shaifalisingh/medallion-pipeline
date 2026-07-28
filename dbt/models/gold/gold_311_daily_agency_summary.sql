-- Gold: BI-ready aggregate. Grain is one row per (day, agency, complaint_type).
-- This table is what gets materialized to Delta Lake -- the mart, not the
-- raw or intermediate layers.
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
group by 1, 2, 3, 4
