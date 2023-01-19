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
    )

select * from message_count_by_direction_type_status