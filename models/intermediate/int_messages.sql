with
    messages as (select * from {{ ref('stg_messages') }}),

    node_configs as (select * from {{ ref('stg_node_configs') }}),

    add_node_info_to_messages as (
        select
            messages.*,
            node_configs.* except (node_label, flow_name, flow_uuid)
        from messages
            left join node_configs using (node_label)
    )

select * from add_node_info_to_messages