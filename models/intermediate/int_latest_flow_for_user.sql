with
    messages as (select * from {{ ref('stg_messages') }}),

    add_row_number as (
        select
            *,
            row_number() over (partition by contact_phone order by message_inserted_at desc) as row_number,
        from messages
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
    )


select * from get_latest_flow_uuid