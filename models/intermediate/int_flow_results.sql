with
    flow_results as (select * from {{ ref('stg_flow_results') }}),

    nodes as (select * from {{ ref('int_nodes') }}),

    unnest_result_jsons as (
        select
            * except(result_array),
        from
            flow_results,
            unnest(result_array) as result_json
    ),

    extract_result_details as (
        select
            * except(result_json),
            json_value(result_json, '$.result_key') as result_key,
            json_value(result_json, '$.category') as result_category,
            json_value(result_json, '$.input') as result_input,
            json_value(result_json, '$.inserted_at') as result_inserted_at,
            json_value(result_json, '$.intent') as result_intent,
            json_query(result_json, '$.interactive') as result_interactive,
        from
            unnest_result_jsons
    ),

    add_node_details as (
        select
            extract_result_details.*,
            nodes.* except (flow_uuid)
        from
            extract_result_details
            left join nodes on extract_result_details.result_key =  nodes.node_label
    )

select * from add_node_details
