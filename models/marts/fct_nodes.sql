with

    nodes_on_glific as (select * from {{ ref('int_nodes') }}),

    messages as (select * from {{ ref('int_messages') }}),

    aggregate_users_by_node as (
        select
            flow_uuid,
            node_label,
            message_direction,
            count(distinct contact_phone) as users
        from messages
        group by 1, 2, 3
    ),

    pivot_by_message_direction as (
        select *
        from aggregate_users_by_node
        pivot (sum(users) for message_direction in ('outbound' as users_outbound, 'inbound' as users_inbound))
    ),

    add_metrics_to_nodes as (
        select
            *
        from nodes_on_glific
            left join pivot_by_message_direction using (flow_uuid, node_label)
    )

select *
from add_metrics_to_nodes