-- Quarantine, not silent drop. Real feeds have nulls and schema drift --
-- a request missing its ID or agency doesn't get discarded, it gets
-- captured here with a reason so it can be triaged/reprocessed, and it
-- counts toward the rejection-rate gate in dbt/tests/.
select
    request_id,
    created_at,
    agency,
    complaint_type,
    case
        when request_id is null then 'missing_request_id'
        when agency is null      then 'missing_agency'
        when created_at is null  then 'missing_created_at'
    end as rejection_reason
from {{ ref('stg_nyc_311') }}
where request_id is null
   or agency is null
   or created_at is null
