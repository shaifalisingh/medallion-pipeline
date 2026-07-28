with dedup as (
    select
        *,
        row_number() over (partition by commit_sha order by authored_at desc) as rn
    from {{ ref('stg_github_commits') }}
    where commit_sha is not null
      and authored_at is not null
      -- rows failing these checks go to rejected_commits instead of
      -- silently vanishing -- see that model for the reject reasons.
)
select
    commit_sha,
    message,
    split_part(message, chr(10), 1)   as message_headline,
    author_name,
    author_email,
    github_login,
    authored_at,
    date_trunc('day', authored_at)    as authored_date
from dedup
where rn = 1
