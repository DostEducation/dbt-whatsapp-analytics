with
    flow_contexts as (select * from {{ source("glific", "flow_contexts") }}),

    add_row_number as (
        select
            *,
            row_number() over (
                partition by flow_uuid, contact_phone order by updated_at desc
            ) as row_number
        from flow_contexts
    ),

    filter_latest_row as (
        select * from add_row_number
        where
            true
            and row_number = 1
    ),

    rename_columns as (
        select
            id as flow_context_id, 
            bq_uuid, 
            bq_inserted_at, 
            node_uuid, 
            flow_uuid, 
            flow_id, 
            contact_id, 
            contact_phone, 
            results, 
            recent_inbound, 
            recent_outbound, 
            status, 
            parent_id, 
            flow_broadcast_id, 
            is_background_flow, 
            is_killed, 
            is_await_result, 
            wakeup_at, 
            -- completed_at, 
            -- inserted_at, 
            -- updated_at, 
            profile_id, 
            message_broadcast_id, 
            reason
        from filter_latest_row
    )

select * from rename_columns