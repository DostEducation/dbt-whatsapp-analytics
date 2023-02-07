with
    flows as (select * from {{ ref("stg_flows") }}),

    extract_nodes_array as (
        select flow_uuid, json_query_array(flow_config_json, "$.nodes") as nodes,
        from flows
    ),

    unnest_nodes as (
        select
            json_value(node, "$.uuid") as node_uuid,
            extract_nodes_array.flow_uuid,
            node_sequence,
            if(
                array_length(json_query_array(node, "$.actions")) = 0,
                json_value(node, "$.router.type"),
                json_value(node, "$.actions[0].type")
            ) as node_type_n1,
            json_value(node, "$.actions[0].text") as message_text_n1,
            json_value(node, "$.actions[0].groups[0].name") as group_name_n1,
            json_value(node, "$.actions[0].value") as field_value_n1,
            json_query(node, "$.actions[0].labels") as node_labels,
            json_query(node, "$") as node_config
        from extract_nodes_array, unnest(extract_nodes_array.nodes) node
        with
        offset as node_sequence
    )

select *
from unnest_nodes
where flow_uuid = '75640ab7-992c-40b8-9113-df8c4bdf2a65'

