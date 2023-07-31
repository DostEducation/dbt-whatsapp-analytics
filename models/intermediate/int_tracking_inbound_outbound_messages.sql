with messages as (select * from {{ref('int_messages')}}),
messages_sent_by_user as (
    select
        message_id,
        contact_name,
        contact_phone,
        message_direction,
        message_body,
        message_type,
        message_inserted_at,
        row_number() over (partition by contact_phone order by message_inserted_at desc) as row_number
    from messages
    where REGEXP_CONTAINS(message_body, r'^Hello.[A-Za-z]') or REGEXP_CONTAINS(message_body, r'^Hidost') or
        REGEXP_CONTAINS(message_body, r'^Hey.[A-Za-z]') or REGEXP_CONTAINS(message_body, r'^Hi.[A-Za-z]') or REGEXP_CONTAINS(message_body, r'^heydost')
        or REGEXP_CONTAINS(message_body, r'^hidost') or REGEXP_CONTAINS(message_body, r'^heydost') or REGEXP_CONTAINS(message_body, r'^Hellodost') or REGEXP_CONTAINS(message_body, r'^hellodost')
),
filtering_out_row_number_for_user as (
    select
        *except(row_number, message_inserted_at),
        cast(message_inserted_at as date) as message_inserted_at_user
    from messages_sent_by_user
    where row_number = 1
),
messages_sent_by_dost as (
    select
         message_id,
        contact_name,
        contact_phone,
        message_direction,
        message_body,
        message_type,
        message_inserted_at,
        row_number() over (partition by contact_phone order by message_inserted_at desc) as row_number
    from messages
    where node_label = 'bf01n01'
),
filtering_out_row_number_for_dost as (
    select
        *except(row_number,message_inserted_at),
        cast(message_inserted_at as date) as message_inserted_at_dost
    from messages_sent_by_dost
    where row_number = 1
)
select
    filtering_out_row_number_for_user.* except (message_id),
    filtering_out_row_number_for_dost.message_body as dost_message_body,
    filtering_out_row_number_for_dost.contact_phone as dost_contact_phone,
    filtering_out_row_number_for_dost.message_inserted_at_dost as sent_by_dost
from filtering_out_row_number_for_user
left join filtering_out_row_number_for_dost on filtering_out_row_number_for_dost.message_inserted_at_dost = filtering_out_row_number_for_user.message_inserted_at_user
    and filtering_out_row_number_for_user.contact_phone = filtering_out_row_number_for_dost.contact_phone
