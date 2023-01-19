with
    messages as (select * from {{ source("glific", "messages") }}),

    get_latest_message_record as (
        select *
        from
            (
                select
                    row_number() over (
                        partition by id order by bq_inserted_at desc
                    ) as row_number,
                    messages.*
                from messages
            )
        where row_number < 2
    ),

    message_count_by_direction_type_status as (
        select
            flow,
            type,
            status,
            count(id)
        from get_latest_message_record
        group by 1, 2, 3
        order by 1, 2, 3
    ),

    select_and_rename_columns as (
        select
            id as message_id,
            bq_uuid,
            uuid,
            body,
            type,
            flow,
            status,
            errors,
            sender_phone,
            receiver_phone,
            contact_phone,
            contact_name,
            user_phone,
            user_name,
            media_url,
            sent_at,
            inserted_at,
            tags_label,
            flow_label,
            flow_name,
            flow_uuid,
            flow_id,
            longitude,
            latitude,
            updated_at,
            gcs_url,
            is_hsm,
            template_uuid,
            interactive_template_id,
            context_message_id,
            group_message_id,
            flow_broadcast_id,
            media_id,
            bq_inserted_at,
            bsp_status,
            profile_id,
            message_broadcast_id,
            group_id,
            group_name,
        from get_latest_message_record
    )

select * from select_and_rename_columns