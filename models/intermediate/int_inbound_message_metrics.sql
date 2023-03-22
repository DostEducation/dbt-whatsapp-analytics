with
    messages as (select * from {{ ref("stg_messages") }}),

    group_contacts_by_incoming_message_type as (
        select contact_phone, message_type, count(message_id) count_of_incoming_messages
        from messages
        where
            message_status = 'received'
            and message_direction = 'inbound'
        group by 1, 2
    ),

    pivot_by_message_type as (
        select *
        from
            group_contacts_by_incoming_message_type pivot (
                sum(count_of_incoming_messages) for message_type in (
                    'quick_reply' as quick_reply_received,
                    'list' as list_received,
                    'audio' as audio_received,
                    'text' as text_received
                )
            ) as pivot_results
    )

select *
from pivot_by_message_type
