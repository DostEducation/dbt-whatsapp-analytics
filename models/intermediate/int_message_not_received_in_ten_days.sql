with
    messages as (select * from {{ ref('fct_messages') }}),
    add_row_number as (
        select
            *,
            row_number() over (partition by contact_phone order by message_inserted_at desc) as row_number,
        from messages
    ),

    get_latest_message_for_contact as (
        select
            contact_phone,
            flow_name as last_flow_name_ten_days,
            message_inserted_at as last_message_sent_ten_days
        from
            add_row_number
        where row_number = 1
            and message_inserted_at <= CURRENT_DATETIME() - INTERVAL 10 DAY
            and message_direction = 'outbound'
    )


select 
    get_latest_message_for_contact.*
from get_latest_message_for_contact
{# where contact_phone = '919992731586' #}