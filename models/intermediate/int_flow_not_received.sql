with
    messages as (select * from {{ ref('stg_messages') }}),
    contacts as (select * from {{ ref('fct_contacts') }}),
    add_row_number as (
        select
            *,
            row_number() over (partition by contact_phone order by message_inserted_at desc) as row_number,
        from messages
        {# where message_inserted_at >= current_datetime() - interval 7 day #}
    ),

    get_latest_message_for_contact as (
        select *
        from add_row_number
        where row_number = 1
    ),

    get_latest_flow_uuid as (
        select
            contact_phone,
            flow_uuid as latest_flow_uuid,
            flow_name as latest_flow_name,
            message_direction as latest_message_direction,
            message_status as latest_message_status,
            message_inserted_at as latest_message_inserted_at
        from
            get_latest_message_for_contact
        where message_inserted_at <= current_datetime() - interval 10 day
    )


select 
    get_latest_flow_uuid.*,
    contacts.user_type_from_google_sheets 
from get_latest_flow_uuid
left join contacts using (contact_phone)