with
    messages as (select * from {{ ref("stg_messages") }}),

    message_count_by_direction_type_status as (
        select
            message_type,
            message_status,
            count(distinct contact_phone) as count_of_users,
        from messages
        where message_status <> 'enqueued'
        group by 1, 2
        order by 1, 2
    ),

    pivot_by_status as (
        select *
        from
            message_count_by_direction_type_status pivot (
                sum(count_of_users) for message_status in ('sent', 'received')
            ) as pivot_results
    ),

    percent_users_responding as (
        select
            *,
            if(received < sent, safe_divide(received, sent), 1) as percent_users_responding
        from pivot_by_status
    )

select *
from percent_users_responding
