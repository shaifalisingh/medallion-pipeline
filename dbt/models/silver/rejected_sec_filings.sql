select
    filing_id,
    accession_number,
    cik,
    case
        when accession_number is null then 'missing_accession_number'
        when cik is null              then 'missing_cik'
        when file_date is null        then 'missing_file_date'
    end as rejection_reason
from {{ ref('stg_sec_edgar_filings') }}
where accession_number is null
   or cik is null
   or file_date is null
