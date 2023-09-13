with
    messages as (select * from {{ ref('int_messages') }}),

    contacts as (select * from {{ ref('int_contacts') }}),

    joining_contacts_messages as (
        select messages.*except(flow_config_json),programme, sign_up_status,parent_type,city, open_ended_experience,semi_guided_experience,guided_experience from messages
        left join contacts using (contact_phone)
    )
select
    *
from joining_contacts_messages