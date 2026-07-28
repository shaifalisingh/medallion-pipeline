{% set threshold = 0.02 %}
with counts as (
    select
        (select count(*) from {{ ref('rejected_commits') }})   as rejected_count,
        (select count(*) from {{ ref('stg_github_commits') }}) as total_count
)
select
    rejected_count,
    total_count,
    rejected_count::double / greatest(total_count, 1) as rejection_rate
from counts
where rejected_count::double / greatest(total_count, 1) > {{ threshold }}
