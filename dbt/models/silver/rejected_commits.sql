select
    commit_sha,
    author_name,
    author_email,
    case
        when commit_sha is null  then 'missing_commit_sha'
        when authored_at is null then 'missing_authored_at'
    end as rejection_reason
from {{ ref('stg_github_commits') }}
where commit_sha is null
   or authored_at is null
