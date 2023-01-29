with

    flow_counts as (select * from {{ ref("stg_flow_counts") }}),

    aggregate_users_by_node_and_count_type as (
        select
            source_node_uuid,
            flow_uuid,
            flow_count_type,
            sum(flow_count) as user_count
        from flow_counts
        group by 1, 2, 3
    ),

    pivot_by_flow_count_type as (
        select * from aggregate_users_by_node_and_count_type
        PIVOT(SUM(user_count) FOR flow_count_type IN ('node', 'exit'))
    )

select * from aggregate_users_by_node_and_count_type
