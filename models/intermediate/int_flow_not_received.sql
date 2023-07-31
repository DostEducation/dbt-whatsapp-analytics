with
    messages as (select * from {{ ref('stg_messages') }}),
    contacts as (select * from {{ ref('fct_contacts') }}),
    add_row_number as (
        select
            flow_name,
            message_inserted_at,
            row_number() over (partition by flow_name order by message_inserted_at desc) as row_number,
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
            flow_name as latest_flow_name,
            message_inserted_at as latest_message_inserted_at
        from
            get_latest_message_for_contact
        where message_inserted_at <= current_datetime() - interval 10 day
    )


select 
    *
from get_latest_flow_uuid