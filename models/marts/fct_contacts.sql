with
    contacts as (select * from {{ ref('int_contacts') }}),

    AWS_metrics as (select * from {{ ref('int_AWS_metrics') }}),

    received_message_metrics as (select * from {{ ref('int_inbound_message_metrics') }}),

    latest_flow as (select * from {{ ref('int_latest_flow_for_user') }}),

    latest_flow_before_ten_days as (select * {{ ref('int_message_not_received') }})

select
    *
from
    contacts
    left join AWS_metrics using (google_sheet_id)
    left join latest_flow using (contact_phone)
    left join received_message_metrics using (contact_phone)
    left join latest_flow_before_ten_days using (contact_phone)
