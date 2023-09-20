with
    messages as (select * from {{ ref("int_messages") }}),
    contacts as (select * from {{ ref("int_contacts") }}),

    join_tables as (
        select
            messages.*,
            programme,
            sign_up_status,
            parent_type,
            city,
            open_ended_experience,
            semi_guided_experience,
            guided_experience,
            experience_type,
            contact_optin_method
        from messages
        left join contacts using (contact_phone)
    )
select *
from join_tables
{# where contact_phone = '919675467828' and message_direction = 'outbound' and start_node_in_flow = 'Yes' #}