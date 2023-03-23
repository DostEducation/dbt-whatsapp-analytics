with
    flows as (select * from {{ ref("stg_flows") }}),

    node_configs as (select * from {{ ref('stg_node_configs') }}),

    extract_nodes_array as (
        select
            flow_uuid,
            flow_name,
            json_query_array(flow_config_json, "$.nodes") as nodes,
        from flows
    ),

    unnest_nodes as (
        select
            json_value(node, "$.uuid") as node_uuid,
            extract_nodes_array.* except (nodes),
            node_sequence,
            if(
                array_length(json_query_array(node, "$.actions")) = 0,
                json_value(node, "$.router.type"),
                json_value(node, "$.actions[0].type")
            ) as node_type_n1,
            json_value(node, "$.actions[0].text") as message_text_n1,
            json_value(node, "$.actions[0].groups[0].name") as group_name_n1,
            json_value(node, "$.actions[0].value") as field_value_n1,
            lower(json_value(node, "$.actions[0].labels[0].name")) as node_label,
            json_query(node, "$.actions[0].labels") as node_label_array,
            json_query(node, "$") as node_config
        from extract_nodes_array, unnest(extract_nodes_array.nodes) node
        with
        offset as node_sequence
    ),

    add_node_config_info as (
        select
            unnest_nodes.*,
            node_configs.* except (node_label, flow_name, flow_uuid),
        from unnest_nodes
            left join node_configs using (node_label)
    )

select *
from add_node_config_info

