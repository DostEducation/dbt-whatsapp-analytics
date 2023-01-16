with
    nodes as (select * from {{ ref("int_nodes") }}),

    extract_exits_array as (
        select node_uuid, json_query_array(node_config, "$.exits") as exits from nodes
    ),

    unnest_exits as (
        select
            extract_exits_array.node_uuid,
            exit_sequence,
            json_extract_scalar(json_query(exit, "$.uuid"), "$") as exit_uuid,
            json_extract_scalar(
                json_query(exit, "$.destination_uuid"), "$"
            ) as destination_uuid,
        from extract_exits_array, unnest(extract_exits_array.exits) exit
        with
        offset as exit_sequence

    ),

    get_destination_type as (
        select
            unnest_exits.*,
            destination_nodes.node_type_n1 as destination_node_type
        from unnest_exits
        left join nodes as destination_nodes on destination_nodes.node_uuid =  unnest_exits.destination_uuid
    )

select *
from get_destination_type
