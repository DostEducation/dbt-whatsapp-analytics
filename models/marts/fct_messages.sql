with
    messages as (select * from {{ ref('int_messages') }}),

    contact_field as (select * from {{ ref('int_contact_fields') }}),

    joining_contact_message as (
        select messages.*except(flow_config_json),programme, sign_up_status,parent_type,city from messages
        left join contact_field using (contact_phone)
    )
select
    *
from joining_contact_message