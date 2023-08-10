with
    messages as (select * from {{ ref('stg_messages') }}),
    add_row_number as (
        select
            flow_name,
            message_inserted_at,
            row_number() over (partition by flow_name order by message_inserted_at desc) as row_number,
        from messages
    ),
    get_latest_flow_uuid as (
        select
            flow_name as latest_flow_name,
            message_inserted_at as latest_message_inserted_at
        from
            add_row_number
        where message_inserted_at <= current_datetime() - interval 10 day
            and row_number = 1
    )


select 
    *
from get_latest_flow_uuid