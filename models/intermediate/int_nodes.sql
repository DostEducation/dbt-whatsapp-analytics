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
            lower(json_value(node, "$.actions[0].labels[0].name")) as node_label,
            json_query(node, "$.actions[0].labels") as node_label_array,
            json_query(node, "$") as node_config
        from extract_nodes_array, unnest(extract_nodes_array.nodes) node
        with
        offset as node_sequence
    )

select *
from unnest_nodes
where
    flow_uuid in (
        'bf2e5555-689f-4708-b9b5-cc6bab8ecf70', -- activation flow
        '094ed199-1b6c-42a6-80bb-f46617fbb937', -- aws registration 1.0
        '9c797785-3062-4295-a824-c3237ecbc98a', -- aws registration 1.1
        'a4900527-f7bc-4dd7-afea-32803280cde1' -- aws registration 1.2
    )

