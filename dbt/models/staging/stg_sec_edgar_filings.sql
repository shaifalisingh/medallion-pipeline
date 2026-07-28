select
    _id                        as filing_id,
    _source.adsh               as accession_number,
    _source.form               as form_type,
    _source.file_date          as file_date,
    _source.period_ending      as period_ending,
    _source.sequence           as sequence,
    _source.display_names[1]   as display_name,
    _source.ciks[1]            as cik,
    _source.biz_states[1]      as biz_state,
    _source.sics[1]            as sic_code,
    _source.file_type          as file_type
from {{ source('bronze', 'sec_edgar_10k') }}
