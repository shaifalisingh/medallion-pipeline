-- SEC's full-text search API can return multiple matched documents for
-- the same filing (the main 10-K plus an exhibit that also matched the
-- search terms) -- a real duplicate-key case found in the actual data,
-- not manufactured: 200 rows, 200 distinct filing_id, but only 199
-- distinct accession_number. Deduped to one row per accession_number,
-- keeping the lowest `sequence` (the primary filed document, not an
-- exhibit).
with dedup as (
    select
        *,
        row_number() over (partition by accession_number order by sequence asc) as rn
    from {{ ref('stg_sec_edgar_filings') }}
    where accession_number is not null
      and cik is not null
      and file_date is not null
      -- rows failing these checks go to rejected_sec_filings instead of
      -- silently vanishing -- see that model for the reject reasons.
)
select
    accession_number,
    form_type,
    file_date,
    period_ending,
    display_name,
    cik,
    biz_state,
    sic_code
from dedup
where rn = 1
