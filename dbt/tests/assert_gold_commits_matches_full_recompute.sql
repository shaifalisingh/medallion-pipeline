with recomputed as (
    select
        authored_date,
        author_name,
        author_email,
        count(*) as commit_count
    from {{ ref('slv_commits') }}
    group by 1, 2, 3
)
select
    r.authored_date,
    r.author_name,
    r.author_email,
    r.commit_count as expected_count,
    g.commit_count as actual_count
from recomputed r
left join {{ ref('gold_commit_activity_daily') }} g
    on r.authored_date = g.authored_date
   and r.author_name = g.author_name
   and r.author_email = g.author_email
where g.commit_count is null
   or g.commit_count != r.commit_count
