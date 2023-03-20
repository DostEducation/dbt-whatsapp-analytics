with
    contacts as (select * from {{ ref('int_contacts') }}),

    AWS_metrics as (select * from {{ ref('int_AWS_metrics') }}),

    latest_flow as (select * from {{ ref('int_latest_flow_for_user') }})

select
    *,
from
    contacts
    left join AWS_metrics using (google_sheet_id)
    left join latest_flow using (contact_phone)
-- where contact_inserted_at >= '2023-03-15'
