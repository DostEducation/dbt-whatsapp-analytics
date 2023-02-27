with
    contacts as (select * from {{ ref('int_contacts') }}),

    AWS_metrics as (select * from {{ ref('int_AWS_metrics') }})

select
    *,
from
    contacts
    left join AWS_metrics using (google_sheet_id)
