-- Thin pass over the raw bronze JSON: rename/cast only, no business logic.
-- Kept as a view so it costs nothing to re-run and always reflects the
-- current bronze files on disk.
select
    try_cast(unique_key as bigint)     as request_id,
    try_cast(created_date as timestamp) as created_at,
    try_cast(closed_date as timestamp)  as closed_at,
    agency,
    agency_name,
    complaint_type,
    descriptor,
    borough,
    incident_zip,
    status,
    try_cast(latitude as double)  as latitude,
    try_cast(longitude as double) as longitude
from {{ source('bronze', 'nyc_311') }}
