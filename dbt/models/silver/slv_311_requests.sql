-- Silver: typed, deduped, and conformed. This is the layer a client's
-- analysts would actually query -- bronze is never queried directly.
with dedup as (
    select
        *,
        row_number() over (partition by request_id order by created_at desc) as rn
    from {{ ref('stg_nyc_311') }}
    where request_id is not null
)
select
    request_id,
    created_at,
    closed_at,
    date_diff('hour', created_at, closed_at)  as resolution_hours,
    agency,
    agency_name,
    complaint_type,
    descriptor,
    coalesce(borough, 'UNSPECIFIED')          as borough,
    incident_zip,
    status,
    latitude,
    longitude
from dedup
where rn = 1
