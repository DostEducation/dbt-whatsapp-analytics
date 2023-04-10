with
    flow_results as (select * from {{ ref('stg_flow_results') }}),

    nodes as (select * from {{ ref('int_nodes') }}),

    configured_responses as (select * from {{ ref('stg_configured_responses') }}),

    outputs as (select * from {{ ref('stg_outputs') }}),

    contacts as (select * from {{ ref('fct_contacts') }}),

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
    ),

    add_desired_response as (
        select
            add_node_details.*,
            configured_responses.configured_response_id,
            configured_responses.desired_response,
        from
            add_node_details
            left join configured_responses using (node_label, result_category)
    ),

    add_output_label as (
        select
            add_desired_response.*,
            outputs.output_name
        from
            add_desired_response
            left join outputs using (output_id)
    ),

    add_user_type as (
        select
            add_output_label.*,
            contacts.user_type
        from
            add_output_label
            left join contacts using (contact_phone)
    )

select * from add_user_type
