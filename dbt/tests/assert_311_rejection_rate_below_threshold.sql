-- The actual data-quality gate: a handful of bad rows get quarantined and
-- the pipeline keeps going (that's rejected_311_requests' job). But if the
-- rejection rate spikes past 2%, something upstream is actually broken
-- (schema change, feed corruption) and the run should fail loudly instead
-- of quietly shipping a gold layer built on a badly-degraded batch.
--
-- A singular dbt test fails if this query returns any row.
{% set threshold = 0.02 %}
with counts as (
    select
        (select count(*) from {{ ref('rejected_311_requests') }}) as rejected_count,
        (select count(*) from {{ ref('stg_nyc_311') }})           as total_count
)
select
    rejected_count,
    total_count,
    rejected_count::double / greatest(total_count, 1) as rejection_rate
from counts
where rejected_count::double / greatest(total_count, 1) > {{ threshold }}
