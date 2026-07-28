select
    sha                                     as commit_sha,
    commit.message                         as message,
    commit.author.name                     as author_name,
    commit.author.email                    as author_email,
    commit.author.date::timestamp          as authored_at,
    author.login                           as github_login
from {{ source('bronze', 'github_commits') }}
