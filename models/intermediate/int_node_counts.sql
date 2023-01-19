with

    flow_counts as (select * from {{ ref("stg_flow_counts") }}),

    users_at_node as (
        select
            nodes.node_uuid,
            nodes.flow_uuid,
            flow_counts.type as node_type,
            sum(flow_counts.count) as count
        from `918800625442.gk_flows_and_nodes` as nodes
        left join
            flow_counts
            on flow_counts.source_uuid = nodes.node_uuid
            and flow_counts.flow_uuid = nodes.flow_uuid
        where 1 = 1
        -- and nodes.flow_name = 'Exp1-Flow1.a'
        group by 1, 2, 3
    ),
    exit_counts as (
        select
            nodes_and_exits.node_uuid,
            nodes_and_exits.flow_uuid,
            nodes_and_exits.node_sequence,
            flow_counts.type as exit_count_type,
            sum(flow_counts.count) as exit_count
        from `918800625442.gk_flows_and_nodes_and_exits` nodes_and_exits
        left join
            flow_counts
            on flow_counts.source_uuid = nodes_and_exits.exit_uuid
            and flow_counts.flow_uuid = nodes_and_exits.flow_uuid
        where 1 = 1
        -- and nodes_and_exits.flow_name = 'Exp1-Flow1.a'
        group by 1, 2, 3, 4
        order by node_sequence
    )

-- select * from exit_counts where exit_count is not null
select
    nodes.*,
    users_at_node.node_type,
    users_at_node.count as users_at_node_count,
    -- 0 as exit_count_type, 0 as exit_count
    exit_counts.exit_count_type,
    exit_counts.exit_count
from `918800625442.gk_flows_and_nodes` as nodes
left join
    users_at_node
    on users_at_node.node_uuid = nodes.node_uuid
    and users_at_node.flow_uuid = nodes.flow_uuid
left join
    exit_counts
    on exit_counts.node_uuid = nodes.node_uuid
    and exit_counts.flow_uuid = nodes.flow_uuid
    and exit_counts.exit_count is not null
-- and nodes.flow_name = 'Exp1-Flow1.a'
-- and entry_count is not null
where 1 = 1
